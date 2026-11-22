"""Remote access, extraction scheduling and the on-disk cache behind the filesystem.

Cache layout: <cache>/<SYMBOL>/<YYYY-MM-DD>.parquet (and .csv rendered on demand), <cache>/_symbols/<day>.txt, <cache>/_logs/.
A request for a (symbol, day) that is not cached is queued; all requests for one day that arrive within the batch window are served
by a single pass over that day's pcap, because the pass costs the same for one symbol or a thousand.
"""
import logging
import os
import re
import shlex
import shutil
import subprocess
import threading
import time
from concurrent.futures import Future
from pathlib import Path

import pyarrow.csv as pacsv
import pyarrow.parquet as pq

from .config import Config
from .schema import SCHEMA

log = logging.getLogger("datafeedfs")
SYMBOL_PATTERN = re.compile(r"^[A-Z0-9.]{1,8}$")


class ExtractionError(Exception):
    pass


class Store:
    def __init__(self, config: Config) -> None:
        self.config = config
        self.cache = config.cache_dir
        for name in ("_symbols", "_logs", "_work"):
            (self.cache / name).mkdir(parents=True, exist_ok=True)
        self._condition = threading.Condition()
        self._pending: dict[str, dict[str, Future]] = {}   # day -> symbol -> future, waiting for the next pass of that day
        self._first_request: dict[str, float] = {}
        self._inflight: dict[tuple[str, str], Future] = {}
        self._running_days: set[str] = set()
        self._failures: dict[str, str] = {}
        self._days: list[str] = []
        self._days_loaded = 0.0
        self._symbols: list[str] = []
        self._symbol_set: frozenset[str] = frozenset()
        self._lookup_lock = threading.Lock()
        self._pass_counter = 0
        threading.Thread(target=self._scheduler, name="scheduler", daemon=True).start()

    # ----- remote access -------------------------------------------------------------------------------------------------------
    def _remote_command(self, command: str) -> list[str]:
        if not self.config.remote.ssh_host:
            return ["sh", "-c", command]
        return ["ssh", *self.config.remote.ssh_options, self.config.remote.ssh_host, command]

    def days(self) -> list[str]:
        """Trading days that have a pcap on the host, ascending. Cached for days_ttl_seconds."""
        with self._lookup_lock:
            if self._days and time.monotonic() - self._days_loaded < self.config.days_ttl_seconds:
                return self._days
            result = subprocess.run(self._remote_command(f"ls -1 {self.config.day_glob()} 2>/dev/null"), capture_output=True, text=True, timeout=120)
            regex = self.config.day_regex()
            found = sorted({f"{m['yyyy']}-{m['mm']}-{m['dd']}" for line in result.stdout.splitlines() if (m := regex.match(line.strip()))})
            if found or not self._days:
                self._days, self._days_loaded = found, time.monotonic()
            return self._days

    def symbols(self) -> list[str]:
        """Symbols of the newest day. Computed once per day by running the parser in list mode."""
        days = self.days()
        if not days:
            return []
        with self._lookup_lock:
            if self._symbols:
                return self._symbols
        names = self._symbols_for_day(days[-1])
        with self._lookup_lock:
            self._symbols, self._symbol_set = names, frozenset(names)
        return names

    def has_symbol(self, symbol: str) -> bool:
        self.symbols()
        return symbol in self._symbol_set

    def _symbols_for_day(self, day: str) -> list[str]:
        cached = self.cache / "_symbols" / f"{day}.txt"
        if not cached.exists():
            output = self._run_pipeline(day, "list", {}, capture=True)
            names = sorted({line.strip() for line in output.decode().splitlines() if SYMBOL_PATTERN.match(line.strip())})
            if not names:
                raise ExtractionError(f"parser listed no symbols for {day}")
            temporary = cached.with_suffix(".tmp")
            temporary.write_text("\n".join(names) + "\n")
            os.replace(temporary, cached)
        return cached.read_text().split()

    def _run_pipeline(self, day: str, mode: str, extra_env: dict[str, str], capture: bool) -> bytes:
        """ssh cat <pcap> | parser. Returns the parser's stdout when capture is set."""
        pcap = self.config.pcap_for(day)
        self._pass_counter += 1
        log_path = self.cache / "_logs" / f"{day}-{mode}-{self._pass_counter}.log"
        env = {**os.environ, **self.config.parser.env, "DFS_MODE": mode, "DFS_DATE": day, "DFS_PCAP_NAME": os.path.basename(pcap), **extra_env}
        with open(log_path, "wb") as log_file:
            source = subprocess.Popen(self._remote_command(f"cat {shlex.quote(pcap)}"), stdout=subprocess.PIPE, stderr=log_file)
            parser = subprocess.Popen(self.config.parser.command, stdin=source.stdout, stdout=subprocess.PIPE if capture else subprocess.DEVNULL, stderr=log_file, env=env)
            source.stdout.close()  # the parser owns the read end; when it exits the source gets SIGPIPE
            output, _ = parser.communicate()
            try:
                source.wait(timeout=15)
            except subprocess.TimeoutExpired:
                source.kill()
        if parser.returncode != 0:
            tail = log_path.read_text(errors="replace")[-600:]
            raise ExtractionError(f"parser exited {parser.returncode} for {day} ({mode}); log {log_path}: {tail}")
        return output or b""

    # ----- cache ---------------------------------------------------------------------------------------------------------------
    def parquet_path(self, symbol: str, day: str) -> Path:
        return self.cache / symbol / f"{day}.parquet"

    def csv_path(self, symbol: str, day: str) -> Path:
        return self.cache / symbol / f"{day}.csv"

    def ensure_csv(self, symbol: str, day: str) -> Path:
        target = self.csv_path(symbol, day)
        if not target.exists():
            table = pq.read_table(self.parquet_path(symbol, day))
            table = table.set_column(table.schema.get_field_index("event"), "event", table.column("event").cast("string"))
            temporary = target.with_suffix(".csv.tmp")
            pacsv.write_csv(table, temporary)
            os.replace(temporary, target)
        return target

    # ----- extraction ----------------------------------------------------------------------------------------------------------
    def request(self, symbol: str, day: str) -> Future:
        """Future that resolves to the cached parquet path once (symbol, day) is extracted."""
        path = self.parquet_path(symbol, day)
        with self._condition:
            future = self._inflight.get((day, symbol))
            if future is not None:
                return future
            future = Future()
            if path.exists():
                future.set_result(path)
                return future
            self._inflight[(day, symbol)] = future
            self._pending.setdefault(day, {})[symbol] = future
            self._first_request.setdefault(day, time.monotonic())
            self._condition.notify_all()
            return future

    def _scheduler(self) -> None:
        while True:
            with self._condition:
                while True:
                    now = time.monotonic()
                    due = [day for day, symbols in self._pending.items() if symbols and day not in self._running_days
                           and now - self._first_request[day] >= self.config.extract.batch_window_seconds]
                    if due and len(self._running_days) < self.config.extract.max_parallel:
                        break
                    self._condition.wait(0.5)
                day = min(due, key=lambda candidate: self._first_request[candidate])
                batch = self._pending.pop(day)
                del self._first_request[day]
                self._running_days.add(day)
            threading.Thread(target=self._run_batch, args=(day, batch), name=f"pass-{day}", daemon=True).start()

    def _run_batch(self, day: str, batch: dict[str, Future]) -> None:
        started = time.monotonic()
        log.info("pass over %s for %d symbols: %s", day, len(batch), ",".join(sorted(batch)[:8]))
        work = self.cache / "_work" / f"{day}-{os.getpid()}-{self._pass_counter}"
        error: Exception | None = None
        try:
            work.mkdir(parents=True)
            symbols_file = work / "symbols.txt"
            symbols_file.write_text("\n".join(sorted(batch)) + "\n")
            self._run_pipeline(day, "extract", {"DFS_SYMBOLS_FILE": str(symbols_file), "DFS_OUT_DIR": str(work / "out")}, capture=False)
            for symbol, future in batch.items():
                target = self.parquet_path(symbol, day)
                target.parent.mkdir(parents=True, exist_ok=True)
                produced = work / "out" / symbol / f"{day}.parquet"
                if produced.exists():
                    os.replace(produced, target)
                else:  # the symbol had no activity that day: serve an empty table with the right columns
                    pq.write_table(SCHEMA.empty_table(), target, compression="zstd")
        except Exception as exception:  # noqa: BLE001 - reported to every waiting reader
            error = exception
            log.error("pass over %s failed: %s", day, exception)
        finally:
            shutil.rmtree(work, ignore_errors=True)
        with self._condition:
            for symbol, future in batch.items():
                self._inflight.pop((day, symbol), None)
                if error is None:
                    self._failures.pop(f"{symbol} {day}", None)
                    future.set_result(self.parquet_path(symbol, day))
                else:
                    self._failures[f"{symbol} {day}"] = str(error)[:300]
                    future.set_exception(ExtractionError(str(error)))
            self._running_days.discard(day)
            self._condition.notify_all()
        log.info("pass over %s finished in %.0fs (%s)", day, time.monotonic() - started, "ok" if error is None else "failed")

    def status(self) -> dict:
        with self._condition:
            return {
                "running_days": sorted(self._running_days),
                "pending": {day: sorted(symbols) for day, symbols in self._pending.items() if symbols},
                "failed": dict(self._failures),
                "cached_days": len(self.days()) if self._days else None,
            }
