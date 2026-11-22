"""Command line: datafeedfs mount CONFIG MOUNTPOINT | prefetch CONFIG DAY SYMBOL... | symbols CONFIG [DAY] | days CONFIG"""
import argparse
import logging
import sys
from pathlib import Path

from . import config as config_module
from .store import Store


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="datafeedfs")
    commands = parser.add_subparsers(dest="command", required=True)
    mount = commands.add_parser("mount", help="mount the filesystem (runs in the foreground until unmounted)")
    mount.add_argument("config")
    mount.add_argument("mountpoint")
    mount.add_argument("--allow-other", action="store_true")
    prefetch = commands.add_parser("prefetch", help="extract symbols for a day into the cache without mounting")
    prefetch.add_argument("config")
    prefetch.add_argument("day")
    prefetch.add_argument("symbols", nargs="+")
    symbols = commands.add_parser("symbols", help="print the symbols of a day (default: newest)")
    symbols.add_argument("config")
    symbols.add_argument("day", nargs="?")
    days = commands.add_parser("days", help="print the trading days available on the ssh host")
    days.add_argument("config")
    arguments = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    store = Store(config_module.load(arguments.config))

    if arguments.command == "days":
        print("\n".join(store.days()))
    elif arguments.command == "symbols":
        print("\n".join(store._symbols_for_day(arguments.day) if arguments.day else store.symbols()))
    elif arguments.command == "prefetch":
        futures = [store.request(symbol, arguments.day) for symbol in arguments.symbols]
        for future in futures:
            print(future.result())
    else:
        from fuse import FUSE
        from .fs import DataFeedFS
        Path(arguments.mountpoint).mkdir(parents=True, exist_ok=True)
        options = {"allow_other": True} if arguments.allow_other else {}
        FUSE(DataFeedFS(store), arguments.mountpoint, foreground=True, nothreads=False, direct_io=True, ro=True, fsname="datafeedfs", attr_timeout=0, **options)  # attr_timeout=0: sizes are never served stale from the kernel cache
    return 0


if __name__ == "__main__":
    sys.exit(main())
