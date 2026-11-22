"""Schema of the per-symbol parquet files. Must match parsers/ingest.py (tests/test_schema.py checks it)."""
import pyarrow as pa

SCHEMA = pa.schema([
    ("timestamp", pa.timestamp("ns", tz="UTC")),
    ("event", pa.dictionary(pa.int8(), pa.string())),
    ("bid", pa.float64()), ("bid_size", pa.int32()), ("ask", pa.float64()), ("ask_size", pa.int32()),
    ("trade_price", pa.float64()), ("trade_size", pa.int32()),
    ("seq", pa.uint64()),
])
