import importlib.util
from pathlib import Path

from datafeedfs.schema import SCHEMA


def test_ingest_schema_matches_package_schema():
    spec = importlib.util.spec_from_file_location("ingest", Path(__file__).parent.parent / "parsers" / "ingest.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    assert module.SCHEMA.equals(SCHEMA)
