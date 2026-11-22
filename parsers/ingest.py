"""Read itch_tob records from stdin and write one parquet part per symbol for one trading day.

Usage: itch_tob ... --out - | ingest.py YYYY-MM-DD symbols.txt parts_dir
Writes parts_dir/<SYMBOL>/<YYYY-MM-DD>.parquet (row groups of up to FLUSH_ROWS rows). Symbols with no rows get no file.
"""
import resource
import sys
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

RECORD_DTYPE = np.dtype([
    ("itch_ns", "<u8"), ("seq", "<u8"),
    ("bid_px", "<u4"), ("bid_sz", "<u4"), ("ask_px", "<u4"), ("ask_sz", "<u4"),
    ("trade_px", "<u4"), ("trade_sz", "<u4"),
    ("sym", "<u2"), ("event", "u1"),
])
assert RECORD_DTYPE.itemsize == 43
PRICE_SCALE = 10_000
CHUNK_ROWS = 4_000_000
FLUSH_ROWS = 50_000          # per-symbol rows buffered before a row group is written
MAX_BUFFERED_ROWS = 15_000_000
EVENT_CODES = np.zeros(256, dtype=np.int8)
EVENT_CODES[ord("T")], EVENT_CODES[ord("X")] = 1, 2
EVENT_DICTIONARY = pa.array(["Q", "T", "X"])
SCHEMA = pa.schema([
    ("timestamp", pa.timestamp("ns", tz="UTC")),
    ("event", pa.dictionary(pa.int8(), pa.string())),
    ("bid", pa.float64()), ("bid_size", pa.int32()), ("ask", pa.float64()), ("ask_size", pa.int32()),
    ("trade_price", pa.float64()), ("trade_size", pa.int32()),
    ("seq", pa.uint64()),
])


def price(values: np.ndarray) -> np.ndarray:
    result = values.astype(np.float64) / PRICE_SCALE
    result[values == 0] = np.nan
    return result


def to_table(records: np.ndarray, midnight_utc_ns: int) -> pa.Table:
    return pa.table({
        "timestamp": pa.array(records["itch_ns"].astype(np.int64) + midnight_utc_ns, type=pa.timestamp("ns", tz="UTC")),
        "event": pa.DictionaryArray.from_arrays(pa.array(EVENT_CODES[records["event"]]), EVENT_DICTIONARY),
        "bid": price(records["bid_px"]), "bid_size": records["bid_sz"].astype(np.int32),
        "ask": price(records["ask_px"]), "ask_size": records["ask_sz"].astype(np.int32),
        "trade_price": price(records["trade_px"]), "trade_size": records["trade_sz"].astype(np.int32),
        "seq": records["seq"],
    }, schema=SCHEMA)


def raise_open_file_limit() -> None:
    soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
    target = hard if hard != resource.RLIM_INFINITY else 65536
    resource.setrlimit(resource.RLIMIT_NOFILE, (target, hard))


def main() -> None:
    raise_open_file_limit()
    day, symbols_path, parts_dir = sys.argv[1], sys.argv[2], Path(sys.argv[3])
    symbols = [line.strip() for line in open(symbols_path) if line.strip() and len(line.strip()) <= 8]
    midnight = datetime.fromisoformat(day).replace(tzinfo=ZoneInfo("America/New_York"))
    midnight_utc_ns = int(midnight.timestamp()) * 10**9
    stem = day
    buffers: dict[int, list[np.ndarray]] = {}
    buffered = {}
    writers: dict[int, pq.ParquetWriter] = {}
    total_rows = 0

    def flush(symbol_index: int) -> None:
        chunks = buffers.pop(symbol_index, None)
        if not chunks:
            return
        records = np.concatenate(chunks)
        buffered.pop(symbol_index)
        if symbol_index not in writers:
            directory = parts_dir / symbols[symbol_index]
            directory.mkdir(parents=True, exist_ok=True)
            writers[symbol_index] = pq.ParquetWriter(directory / f"{stem}.parquet", SCHEMA, compression="zstd", compression_level=5)
        writers[symbol_index].write_table(to_table(records, midnight_utc_ns))

    pending = b""
    stream = sys.stdin.buffer
    while True:
        data = stream.read(CHUNK_ROWS * RECORD_DTYPE.itemsize)
        if not data:
            break
        data = pending + data
        usable = len(data) // RECORD_DTYPE.itemsize * RECORD_DTYPE.itemsize
        pending = data[usable:]
        records = np.frombuffer(data[:usable], dtype=RECORD_DTYPE)
        total_rows += len(records)
        order = np.argsort(records["sym"], kind="stable")
        sorted_records = records[order]
        sym_sorted = sorted_records["sym"]
        starts = np.flatnonzero(np.diff(sym_sorted, prepend=np.uint16(sym_sorted[0]) ^ np.uint16(1))) if len(sym_sorted) else []
        ends = list(starts[1:]) + [len(sorted_records)]
        for start, end in zip(starts, ends):
            symbol_index = int(sym_sorted[start])
            buffers.setdefault(symbol_index, []).append(sorted_records[start:end].copy())
            buffered[symbol_index] = buffered.get(symbol_index, 0) + (end - start)
            if buffered[symbol_index] >= FLUSH_ROWS:
                flush(symbol_index)
        if sum(buffered.values()) > MAX_BUFFERED_ROWS:
            for symbol_index in list(buffers):
                flush(symbol_index)
    for symbol_index in list(buffers):
        flush(symbol_index)
    for writer in writers.values():
        writer.close()
    print(f"ingest {day}: {total_rows} rows, {len(writers)} symbols with data", file=sys.stderr)


if __name__ == "__main__":
    main()
