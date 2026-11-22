"""FUSE operations. Layout:
    /                                symbols of the newest day (plus .status)
    /<SYMBOL>/                       one entry pair per trading day on the ssh host
    /<SYMBOL>/<YYYY-MM-DD>.parquet   tick-to-tick top of book and trades for that symbol and day
    /<SYMBOL>/<YYYY-MM-DD>.csv       the same as CSV
    /.status                         JSON: running and pending extractions, failures
Opening a file that is not cached yet triggers the extraction (see Store.request).
"""
import errno
import itertools
import json
import os
import stat
import threading
import time

from fuse import FuseOSError, Operations

from .store import SYMBOL_PATTERN, ExtractionError, Store

STATUS_PATH = "/.status"


class DataFeedFS(Operations):
    def __init__(self, store: Store) -> None:
        self.store = store
        self._virtual: dict[int, bytes] = {}
        self._handles = itertools.count(1 << 30)
        self._lock = threading.Lock()
        self._started = time.time()

    # ----- path handling -------------------------------------------------------------------------------------------------------
    def _split(self, path: str):
        """Returns ("root",), ("status",), ("symbol", symbol) or ("file", symbol, day, extension); raises ENOENT for anything else."""
        if path == "/":
            return ("root",)
        if path == STATUS_PATH:
            return ("status",)
        parts = path.strip("/").split("/")
        if not SYMBOL_PATTERN.match(parts[0]) or len(parts) > 2:
            raise FuseOSError(errno.ENOENT)
        try:
            known = self.store.has_symbol(parts[0])
        except Exception:  # noqa: BLE001 - remote unreachable
            raise FuseOSError(errno.EIO)
        if not known:
            raise FuseOSError(errno.ENOENT)
        if len(parts) == 1:
            return ("symbol", parts[0])
        day, _, extension = parts[1].rpartition(".")
        if extension not in ("parquet", "csv") or day not in self.store.days():
            raise FuseOSError(errno.ENOENT)
        return ("file", parts[0], day, extension)

    def _attributes(self, mode: int, size: int = 0, mtime: float | None = None) -> dict:
        timestamp = mtime or self._started
        return {"st_mode": mode, "st_nlink": 2 if stat.S_ISDIR(mode) else 1, "st_size": size, "st_uid": os.getuid(), "st_gid": os.getgid(),
                "st_atime": timestamp, "st_mtime": timestamp, "st_ctime": timestamp}

    def getattr(self, path, fh=None):
        if fh is not None and fh not in self._virtual and fh < (1 << 30):  # fstat on an open file: answer from the file itself
            info = os.fstat(fh)
            return self._attributes(stat.S_IFREG | 0o444, info.st_size, info.st_mtime)
        kind = self._split(path)
        if kind[0] in ("root", "symbol"):
            return self._attributes(stat.S_IFDIR | 0o555)
        if kind[0] == "status":
            return self._attributes(stat.S_IFREG | 0o444)
        _, symbol, day, extension = kind
        cached = self.store.parquet_path(symbol, day) if extension == "parquet" else self.store.csv_path(symbol, day)
        try:
            info = cached.stat()
            return self._attributes(stat.S_IFREG | 0o444, info.st_size, info.st_mtime)
        except FileNotFoundError:
            return self._attributes(stat.S_IFREG | 0o444)  # size is unknown until extracted; the mount uses direct_io so readers do not rely on it

    def readdir(self, path, fh):
        kind = self._split(path)
        if kind[0] == "root":
            try:
                return [".", "..", ".status", *self.store.symbols()]
            except Exception as exception:  # noqa: BLE001
                raise FuseOSError(errno.EIO) from exception
        if kind[0] == "symbol":
            return [".", "..", *(f"{day}.{extension}" for day in self.store.days() for extension in ("parquet", "csv"))]
        raise FuseOSError(errno.ENOTDIR)

    # ----- reading -------------------------------------------------------------------------------------------------------------
    def open(self, path, flags):
        if flags & (os.O_WRONLY | os.O_RDWR):
            raise FuseOSError(errno.EROFS)
        kind = self._split(path)
        if kind[0] == "status":
            with self._lock:
                handle = next(self._handles)
                self._virtual[handle] = (json.dumps(self.store.status(), indent=2) + "\n").encode()
            return handle
        if kind[0] != "file":
            raise FuseOSError(errno.EISDIR)
        _, symbol, day, extension = kind
        future = self.store.request(symbol, day)
        if self.store.config.extract.on_miss == "async" and not future.done():
            raise FuseOSError(errno.EAGAIN)  # extraction continues in the background; retry later or watch /.status
        try:
            parquet = future.result()
            served = parquet if extension == "parquet" else self.store.ensure_csv(symbol, day)
            return os.open(served, os.O_RDONLY)
        except ExtractionError:
            raise FuseOSError(errno.EIO)

    def read(self, path, size, offset, fh):
        if fh in self._virtual:
            return self._virtual[fh][offset:offset + size]
        return os.pread(fh, size, offset)

    def release(self, path, fh):
        if fh in self._virtual:
            with self._lock:
                self._virtual.pop(fh, None)
        else:
            os.close(fh)
        return 0
