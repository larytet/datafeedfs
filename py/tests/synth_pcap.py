"""Builds a tiny pcap of Nasdaq ITCH 5.0 messages in MoldUDP64 packets with a known order-book story (one message per packet)."""
import lzma
import struct
from pathlib import Path

PORT = 30001
OPEN_NS = 9 * 3600 * 10**9 + 30 * 60 * 10**9  # 09:30:00 exchange time


def _header(kind: bytes, locate: int, ts: int) -> bytes:
    return kind + struct.pack(">HH", locate, 0) + ts.to_bytes(6, "big")


def directory(locate: int, symbol: str, ts: int) -> bytes:
    return _header(b"R", locate, ts) + symbol.encode().ljust(8) + b"\x00" * 20


def add(locate: int, symbol: str, ts: int, ref: int, side: str, shares: int, price: int) -> bytes:
    return _header(b"A", locate, ts) + struct.pack(">Q", ref) + side.encode() + struct.pack(">I", shares) + symbol.encode().ljust(8) + struct.pack(">I", price)


def execute(locate: int, ts: int, ref: int, shares: int) -> bytes:
    return _header(b"E", locate, ts) + struct.pack(">QIQ", ref, shares, 1)


def delete(locate: int, ts: int, ref: int) -> bytes:
    return _header(b"D", locate, ts) + struct.pack(">Q", ref)


def replace(locate: int, ts: int, old_ref: int, new_ref: int, shares: int, price: int) -> bytes:
    return _header(b"U", locate, ts) + struct.pack(">QQII", old_ref, new_ref, shares, price)


def trade(locate: int, symbol: str, ts: int, shares: int, price: int) -> bytes:
    return _header(b"P", locate, ts) + struct.pack(">QcI", 0, b"B", shares) + symbol.encode().ljust(8) + struct.pack(">IQ", price, 7)


def story() -> list[bytes]:
    t = lambda step: OPEN_NS + step * 10**6  # noqa: E731 - one millisecond per step
    return [
        directory(1, "AAA", 0), directory(2, "BBB", 0), directory(3, "CCC", 0),
        add(1, "AAA", t(1), 1, "B", 100, 100000),
        add(1, "AAA", t(2), 2, "S", 200, 100500),
        add(1, "AAA", t(3), 3, "B", 50, 100000),
        execute(1, t(4), 1, 40),
        delete(1, t(5), 3),
        replace(1, t(6), 2, 4, 150, 100400),
        add(2, "BBB", t(7), 5, "B", 10, 200000),
        trade(1, "AAA", t(8), 25, 100200),
    ]


def packet(sequence: int, message: bytes, second: int) -> bytes:
    mold = b"TESTSESS01" + struct.pack(">QH", sequence, 1) + struct.pack(">H", len(message)) + message
    udp = struct.pack(">HHHH", 40000, PORT, 8 + len(mold), 0) + mold
    ip = struct.pack(">BBHHHBBH4s4s", 0x45, 0, 20 + len(udp), 0, 0x4000, 64, 17, 0, bytes([10, 0, 0, 1]), bytes([233, 54, 12, 111])) + udp
    frame = b"\x01\x00\x5e\x36\x0c\x6f" + b"\x00\x11\x22\x33\x44\x55" + b"\x08\x00" + ip
    return struct.pack("<IIII", second, 0, len(frame), len(frame)) + frame


def build(path: Path, duplicate_sequence: int | None = 7) -> None:
    """Writes the pcap (xz-compressed when the name ends in .xz). The packet carrying message duplicate_sequence is sent twice."""
    body = struct.pack("<IHHiIII", 0xA1B2C3D4, 2, 4, 0, 0, 65535, 1)
    for index, message in enumerate(story()):
        sequence = index + 1
        body += packet(sequence, message, 1000 + index)
        if sequence == duplicate_sequence:
            body += packet(sequence, message, 1000 + index)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(lzma.compress(body) if path.suffix == ".xz" else body)


# What AAA should look like: (event, bid, bid_size, ask, ask_size, trade_price, trade_size); None = empty side / no trade
EXPECTED_AAA = [
    ("Q", 10.0, 100, None, 0, None, 0),
    ("Q", 10.0, 100, 10.05, 200, None, 0),
    ("Q", 10.0, 150, 10.05, 200, None, 0),
    ("T", 10.0, 150, 10.05, 200, 10.0, 40),     # the trade is reported against the book as it was
    ("Q", 10.0, 110, 10.05, 200, None, 0),
    ("Q", 10.0, 60, 10.05, 200, None, 0),
    ("Q", 10.0, 60, 10.04, 150, None, 0),
    ("T", 10.0, 60, 10.04, 150, 10.02, 25),
]
EXPECTED_BBB = [("Q", 20.0, 10, None, 0, None, 0)]
