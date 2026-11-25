#!/bin/bash
# datafeedfs parser for a Nasdaq TotalView-ITCH 5.0 pcap (MoldUDP64). Contract with datafeedfs:
#   stdin   the pcap file exactly as stored on the ssh host (xz / zst / gz / plain, chosen by DFS_PCAP_NAME)
#   env     DFS_MODE         "list" or "extract"
#           DFS_DATE         trading day, YYYY-MM-DD
#           DFS_PCAP_NAME    file name of the pcap on the host, used to pick the decompressor
#           DFS_SYMBOLS_FILE (extract) file with the symbols to keep, one per line
#           DFS_OUT_DIR      (extract) directory to write <SYMBOL>/<DFS_DATE>.parquet into
#           DFS_KIND         optional: what the parquet files hold (default tick)
#                              tick       tick-to-tick best bid and ask, and trades
#                              orderbook  the top DFS_DEPTH levels per side (default 5), a row whenever any of them changes, and trades
#                              stream     every order message as it came (add, execute, cancel, delete, replace, trade, cross), no book
#           DFS_DEPTH        optional: levels per side of the orderbook kind (default 5)
#           DFS_PORT         optional UDP destination port of the feed (default: any port in the pcap)
#           DFS_PROTOCOL     optional: itch (default) or pitch (Cboe US equities); parse_cboe_pitch.sh sets it
#           DFS_STOP_AT      optional: stop decoding at this many seconds since midnight (exchange time), for short tests; 14520 = 04:02, the first 2 minutes of the feed
#           DFS_INGEST       command that writes the parquet files from itch_tob records (default: datafeedfs-go ingest; datafeedfs sets it)
#   list    print the symbols of the day, one per line, and exit
#   extract write one parquet per symbol of the chosen DFS_KIND
here=$(cd "$(dirname "$0")" && pwd)
{ [ -x "$here/itch_tob" ] && [ ! "$here/itch_tob.c" -nt "$here/itch_tob" ]; } || gcc -O2 -o "$here/itch_tob" "$here/itch_tob.c" || { echo "cannot build itch_tob" >&2; exit 1; }
case "$DFS_PCAP_NAME" in
    *.xz) decompress=(xz -dc -T0) ;;   # -T0: threaded decompression of multi-block files (xz >= 5.4); older xz ignores it
    *.zst) decompress=(zstd -dc) ;;
    *.gz) decompress=(gzip -dc) ;;
    *) decompress=(cat) ;;
esac
protocol=(--protocol "${DFS_PROTOCOL:-itch}")
if [ "$DFS_MODE" = list ]; then
    # PITCH has no symbol directory, so its list pass reads add orders to the end of the pcap, or up to DFS_LIST_STOP_AT seconds since midnight
    "${decompress[@]}" | "$here/itch_tob" --list-symbols "${protocol[@]}" --port "${DFS_PORT:-0}" ${DFS_LIST_STOP_AT:+--stop-at "$DFS_LIST_STOP_AT"} 2>/dev/null
    exit "${PIPESTATUS[1]}"   # the decompressor dies of SIGPIPE once the directory is read; that is expected
fi
case "${DFS_KIND:-tick}" in
    tick) decoder_args=(); ingest_args=() ;;
    orderbook) decoder_args=(--depth "${DFS_DEPTH:-5}"); ingest_args=(--kind orderbook --depth "${DFS_DEPTH:-5}") ;;
    stream) decoder_args=(--events); ingest_args=(--kind stream) ;;
    *) echo "unknown DFS_KIND $DFS_KIND" >&2; exit 1 ;;
esac
read -ra ingest <<< "${DFS_INGEST:-datafeedfs-go ingest}"
"${decompress[@]}" \
    | "$here/itch_tob" --symbols-file "$DFS_SYMBOLS_FILE" --out - "${protocol[@]}" --port "${DFS_PORT:-0}" --table-bits "${DFS_TABLE_BITS:-22}" "${decoder_args[@]}" ${DFS_STOP_AT:+--stop-at "$DFS_STOP_AT"} \
    | "${ingest[@]}" "${ingest_args[@]}" "$DFS_DATE" "$DFS_SYMBOLS_FILE" "$DFS_OUT_DIR"
status=("${PIPESTATUS[@]}")
# with DFS_STOP_AT the decoder stops early and the decompressor dies of SIGPIPE: that is expected, any other stage must succeed
[ -n "$DFS_STOP_AT" ] && status[0]=0
for code in "${status[@]}"; do [ "$code" -eq 0 ] || exit "$code"; done
