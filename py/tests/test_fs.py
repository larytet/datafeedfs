import csv
import errno
import os
import subprocess
import sys
import threading
import time
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from conftest import PY_ROOT, require_fuse

CONFIG = """
[remote]
ssh_host = ""
pcap_path = "{pcaps}/{{yyyy}}/{{mm}}/{{dd}}/feed.pcap.xz"

[parser]
command = "{script}"
env = {{ DFS_PYTHON = "{python}" }}

[cache]
dir = "{cache}"

[extract]
batch_window_seconds = 1.0
on_miss = "{on_miss}"
"""


@pytest.fixture()
def mounted(parser_script, pcap_root, tmp_path, request):
    require_fuse()
    on_miss = getattr(request, "param", "block")
    config = tmp_path / "config.toml"
    config.write_text(CONFIG.format(pcaps=pcap_root, script=parser_script, python=sys.executable, cache=tmp_path / "cache", on_miss=on_miss))
    mountpoint = tmp_path / "mnt"
    process = subprocess.Popen([sys.executable, "-m", "datafeedfs", "mount", str(config), str(mountpoint)], cwd=PY_ROOT, stderr=subprocess.PIPE)
    deadline = time.time() + 20
    while not os.path.ismount(mountpoint):
        if process.poll() is not None or time.time() > deadline:
            process.kill()
            pytest.skip(f"mount failed: {process.stderr.read().decode()[-300:]}")
        time.sleep(0.2)
    yield mountpoint, tmp_path / "cache"
    subprocess.run(["fusermount", "-u", str(mountpoint)], check=False)
    process.wait(timeout=10)


def test_tree_and_data(mounted):
    mountpoint, _ = mounted
    assert sorted(os.listdir(mountpoint)) == [".status", "AAA", "BBB", "CCC"]
    assert sorted(os.listdir(mountpoint / "AAA")) == ["2026-11-20.csv", "2026-11-20.parquet"]
    with pytest.raises(FileNotFoundError):
        os.stat(mountpoint / "ZZZ")
    with pytest.raises(FileNotFoundError):
        os.stat(mountpoint / "AAA" / "2026-11-21.parquet")

    table = pq.read_table(mountpoint / "AAA" / "2026-11-20.parquet")
    assert table.num_rows == 8 and table.column("event").to_pylist()[3] == "T"

    with open(mountpoint / "AAA" / "2026-11-20.csv", newline="") as handle:
        rows = list(csv.DictReader(handle))
    assert len(rows) == 8 and rows[3]["event"] == "T" and float(rows[3]["trade_price"]) == 10.0 and int(rows[3]["trade_size"]) == 40

    assert pq.read_table(mountpoint / "CCC" / "2026-11-20.parquet").num_rows == 0  # no activity: empty table, not an error
    assert os.stat(mountpoint / "AAA" / "2026-11-20.parquet").st_size > 0           # size is real once cached


def test_concurrent_readers_share_one_pass(mounted):
    mountpoint, cache = mounted
    results = {}

    def read(symbol: str) -> None:
        results[symbol] = pq.read_table(mountpoint / symbol / "2026-11-20.parquet").num_rows

    threads = [threading.Thread(target=read, args=(symbol,)) for symbol in ("AAA", "BBB")]
    [thread.start() for thread in threads]
    [thread.join(timeout=60) for thread in threads]
    assert results == {"AAA": 8, "BBB": 1}
    assert len(list((cache / "_logs").glob("*-extract-*.log"))) == 1


def test_status_file(mounted):
    mountpoint, _ = mounted
    assert '"running_days"' in (mountpoint / ".status").read_text()


@pytest.mark.parametrize("mounted", ["async"], indirect=True)
def test_async_mode_returns_eagain_then_data(mounted):
    mountpoint, _ = mounted
    target = mountpoint / "AAA" / "2026-11-20.parquet"
    with pytest.raises(OSError) as raised:
        open(target, "rb")
    assert raised.value.errno == errno.EAGAIN
    deadline = time.time() + 30
    while time.time() < deadline:
        try:
            assert pq.read_table(target).num_rows == 8
            return
        except OSError as error:
            assert error.errno == errno.EAGAIN
            time.sleep(0.5)
    pytest.fail("extraction did not finish")
