import os
import shutil
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent))
ROOT = Path(__file__).parent.parent


@pytest.fixture(scope="session")
def parser_script() -> Path:
    built = (ROOT / "parsers" / "itch_tob").exists()
    if shutil.which("xz") is None or (not built and shutil.which("gcc") is None):
        pytest.skip("xz and either a built parsers/itch_tob or gcc are required")
    return ROOT / "parsers" / "parse_nasdaq_itch.sh"


@pytest.fixture()
def pcap_root(tmp_path: Path) -> Path:
    import synth_pcap
    synth_pcap.build(tmp_path / "pcaps" / "2026" / "11" / "20" / "feed.pcap.xz")
    return tmp_path / "pcaps"


def require_fuse() -> None:
    if not os.path.exists("/dev/fuse") or shutil.which("fusermount") is None and shutil.which("fusermount3") is None:
        pytest.skip("FUSE is not available")
