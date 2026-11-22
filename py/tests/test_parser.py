import os
import subprocess
import sys
from pathlib import Path

import pyarrow.parquet as pq

import synth_pcap
from datafeedfs.schema import SCHEMA


def run(parser_script: Path, pcap: Path, **env) -> subprocess.CompletedProcess:
    full_env = {**os.environ, "DFS_PCAP_NAME": pcap.name, "DFS_PYTHON": sys.executable, **env}
    return subprocess.run([str(parser_script)], stdin=open(pcap, "rb"), capture_output=True, text=True, env=full_env, timeout=60)


def rows(table) -> list[tuple]:
    return [(row["event"], row["bid"], row["bid_size"], row["ask"], row["ask_size"], row["trade_price"], row["trade_size"]) for row in table.to_pylist()]


def normalise(expected: list[tuple]) -> list[tuple]:
    return [tuple(None if value is None or value == 0 and index in (1, 3, 5) else value for index, value in enumerate(row)) for row in expected]


def test_list_mode(parser_script, pcap_root):
    result = run(parser_script, pcap_root / "2026/11/20/feed.pcap.xz", DFS_MODE="list")
    assert result.returncode == 0, result.stderr
    assert result.stdout.split() == ["AAA", "BBB", "CCC"]


def test_extract_mode_rebuilds_book_and_ignores_duplicates(parser_script, pcap_root, tmp_path):
    symbols = tmp_path / "symbols.txt"
    symbols.write_text("AAA\nBBB\nCCC\n")
    out = tmp_path / "out"
    result = run(parser_script, pcap_root / "2026/11/20/feed.pcap.xz", DFS_MODE="extract", DFS_DATE="2026-11-20", DFS_SYMBOLS_FILE=str(symbols), DFS_OUT_DIR=str(out))
    assert result.returncode == 0, result.stderr
    table = pq.read_table(out / "AAA" / "2026-11-20.parquet")
    assert table.schema.equals(SCHEMA)
    got = [tuple(None if isinstance(v, float) and v != v else v for v in row) for row in rows(table)]
    assert got == [tuple(None if v is None else v for v in row) for row in synth_pcap.EXPECTED_AAA]
    assert [row[0] for row in got] == [event for event, *_ in synth_pcap.EXPECTED_AAA]
    assert pq.read_table(out / "BBB" / "2026-11-20.parquet").num_rows == 1
    assert not (out / "CCC").exists()  # no activity, no file
    # exchange time 09:30:00.001 New York is 14:30:00.001 UTC (EST in November)
    assert table.column("timestamp")[0].as_py().isoformat() == "2026-11-20T14:30:00.001000+00:00"
