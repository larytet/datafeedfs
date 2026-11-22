#!/bin/bash
# datafeedfs parser for a Nasdaq TotalView-ITCH 5.0 pcap (MoldUDP64). Contract with datafeedfs:
#   stdin   the pcap file exactly as stored on the ssh host (xz / zst / gz / plain, chosen by DFS_PCAP_NAME)
#   env     DFS_MODE         "list" or "extract"
#           DFS_DATE         trading day, YYYY-MM-DD
#           DFS_PCAP_NAME    file name of the pcap on the host, used to pick the decompressor
#           DFS_SYMBOLS_FILE (extract) file with the symbols to keep, one per line
#           DFS_OUT_DIR      (extract) directory to write <SYMBOL>/<DFS_DATE>.parquet into
#           DFS_PORT         optional UDP destination port of the feed (default: any port in the pcap)
#           DFS_PYTHON       optional python with pyarrow and numpy (default python3), runs ../py/ingest.py
#           DFS_INGEST       optional command that replaces the python ingest, e.g. "datafeedfs-go ingest" (the Go version sets it)
#   list    print the symbols of the day, one per line, and exit
#   extract write one parquet per symbol with the tick-to-tick top of book and trades
here=$(cd "$(dirname "$0")" && pwd)
[ -x "$here/itch_tob" ] || gcc -O2 -o "$here/itch_tob" "$here/itch_tob.c" || { echo "cannot build itch_tob" >&2; exit 1; }
case "$DFS_PCAP_NAME" in
    *.xz) decompress=(xz -dc -T0) ;;   # -T0: threaded decompression of multi-block files (xz >= 5.4); older xz ignores it
    *.zst) decompress=(zstd -dc) ;;
    *.gz) decompress=(gzip -dc) ;;
    *) decompress=(cat) ;;
esac
if [ "$DFS_MODE" = list ]; then
    "${decompress[@]}" | "$here/itch_tob" --list-symbols 2>/dev/null
    exit "${PIPESTATUS[1]}"   # the decompressor dies of SIGPIPE once the directory is read; that is expected
fi
set -o pipefail
if [ -n "$DFS_INGEST" ]; then read -ra ingest <<< "$DFS_INGEST"; else ingest=("${DFS_PYTHON:-python3}" "$here/../py/ingest.py"); fi
"${decompress[@]}" \
    | "$here/itch_tob" --symbols-file "$DFS_SYMBOLS_FILE" --out - --port "${DFS_PORT:-0}" --table-bits "${DFS_TABLE_BITS:-22}" \
    | "${ingest[@]}" "$DFS_DATE" "$DFS_SYMBOLS_FILE" "$DFS_OUT_DIR"
