#!/bin/bash
# datafeedfs parser for a Cboe US equities PITCH pcap (EDGA, EDGX, BYX, BZX). Same contract as parse_nasdaq_itch.sh, which does the work
# with itch_tob --protocol pitch. Set DFS_PORT to one unit's UDP port when the pcap holds gap-request or spin traffic as well.
#   DFS_LIST_STOP_AT  optional: PITCH has no symbol directory, so listing the symbols reads add orders; stop at this many seconds since midnight
#                     (for example 36000 = 10:00) instead of reading the whole pcap. Symbols that first appear later are then not served.
here=$(cd "$(dirname "$0")" && pwd)
export DFS_PROTOCOL=pitch
exec "$here/parse_nasdaq_itch.sh"
