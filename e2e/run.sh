#!/bin/bash
# End-to-end test, see testplan.md. Builds the images, starts an sshd container that serves the test capture and the datafeedfs container
# that reads it over ssh, then checks samples of the tick, orderbook and stream files.
#   e2e/run.sh                  run the test
#   e2e/run.sh --update-golden  rewrite e2e/golden from the current output (review the diff before committing)
#   e2e/run.sh --keep           leave the containers running afterwards
set -euo pipefail
cd "$(dirname "$0")/.."
ROOT=$PWD
update=0 keep=0
for argument in "$@"; do
    case "$argument" in
        --update-golden) update=1 ;;
        --keep) keep=1 ;;
        *) echo "usage: e2e/run.sh [--update-golden] [--keep]" >&2; exit 2 ;;
    esac
done

NETWORK=dfs-e2e SSHD=dfs-e2e-sshd APP=dfs-e2e-app DAY=2026-11-20
PCAP=testdata/pcaps/2026/11/20/nasdaq_tv.pcap.xz
WORK=$(mktemp -d)
failures=0

cleanup() {
    if [ "$keep" -eq 0 ]; then
        docker rm -fv "$APP" "$SSHD" >/dev/null 2>&1 || true
        docker network rm "$NETWORK" >/dev/null 2>&1 || true
        rm -rf "$WORK"
    else
        echo "containers left running: $APP $SSHD (docker rm -fv $APP $SSHD; docker network rm $NETWORK)"
    fi
}
trap cleanup EXIT

pass() { echo "ok   $*"; }
fail() { echo "FAIL $*" >&2; failures=$((failures + 1)); }
step() { echo; echo "== $*"; }
app() { docker exec "$APP" "$@"; }
mounted() { app cat "/mnt/datafeed/$1"; }

step "0. prerequisites"
command -v docker >/dev/null || { echo "docker is required" >&2; exit 2; }
if [ ! -s "$PCAP" ] || head -c 40 "$PCAP" | grep -q "git-lfs.github.com"; then
    echo "$PCAP is missing or is a Git LFS pointer: run 'git lfs pull'" >&2
    exit 2
fi
pass "capture $PCAP ($(du -h "$PCAP" | cut -f1))"
docker rm -fv "$APP" "$SSHD" >/dev/null 2>&1 || true
docker network rm "$NETWORK" >/dev/null 2>&1 || true

step "1. build the images"
docker build -q -t datafeedfs-e2e . >/dev/null && pass "datafeedfs image (datafeedfs-e2e)"
docker build -q -t datafeedfs-e2e-sshd e2e/sshd >/dev/null && pass "sshd image (datafeedfs-e2e-sshd)"

step "2. the pcap host: an sshd container with the capture"
docker network create "$NETWORK" >/dev/null
ssh-keygen -q -t ed25519 -N "" -C datafeedfs-e2e -f "$WORK/id_e2e"
docker run -d --name "$SSHD" --network "$NETWORK" -v "$ROOT/testdata/pcaps:/data/pcaps:ro" datafeedfs-e2e-sshd >/dev/null
docker cp "$WORK/id_e2e.pub" "$SSHD:/home/e2e/.ssh/authorized_keys"
docker exec "$SSHD" sh -c 'chown e2e: /home/e2e/.ssh/authorized_keys && chmod 600 /home/e2e/.ssh/authorized_keys'
echo "$SSHD $(docker exec "$SSHD" cut -d' ' -f1,2 /etc/ssh/ssh_host_ed25519_key.pub)" > "$WORK/known_hosts"
pass "sshd container $SSHD, throwaway key, host key pinned in known_hosts"

step "3. docker run datafeedfs, reading the capture over ssh"
cp e2e/config.e2e.toml "$WORK/config.toml"
docker run -d --name "$APP" --network "$NETWORK" --device /dev/fuse --cap-add SYS_ADMIN --security-opt apparmor:unconfined \
    -v "$WORK/config.toml:/config/config.toml:ro" -v "$WORK/id_e2e:/root/.ssh/id_e2e:ro" -v "$WORK/known_hosts:/root/.ssh/known_hosts:ro" datafeedfs-e2e >/dev/null
for _ in $(seq 1 40); do
    app mountpoint -q /mnt/datafeed 2>/dev/null && break
    sleep 0.5
done
app mountpoint -q /mnt/datafeed && pass "filesystem mounted at /mnt/datafeed" || { fail "not mounted"; docker logs "$APP" >&2; exit 1; }

step "4. layout"
[ "$(mounted .status | head -c1)" = "{" ] && pass ".status is JSON" || fail ".status"
[ "$(app ls -A /mnt/datafeed | tr '\n' ' ')" = ".status orderbook stream tick " ] && pass "root: .status orderbook stream tick" || fail "root listing: $(app ls -A /mnt/datafeed | tr '\n' ' ')"
symbols=$(app ls /mnt/datafeed/tick | tr '\n' ' ')
[ "$symbols" = "AAPL AMD META MSFT NVDA QQQ SPY TSLA " ] && pass "symbols: $symbols" || fail "symbols: $symbols"
[ "$(app ls /mnt/datafeed/tick | md5sum)" = "$(app ls /mnt/datafeed/orderbook | md5sum)" ] && [ "$(app ls /mnt/datafeed/tick | md5sum)" = "$(app ls /mnt/datafeed/stream | md5sum)" ] && pass "the three kinds list the same symbols" || fail "symbol lists differ"
[ "$(app ls /mnt/datafeed/tick/AAPL | tr '\n' ' ')" = "$DAY.csv $DAY.parquet " ] && pass "tick/AAPL: $DAY.csv $DAY.parquet" || fail "tick/AAPL listing"
[ "$(app datafeedfs-go days /config/config.toml)" = "$DAY" ] && pass "days: $DAY (found over ssh)" || fail "days"

step "5. extract (the first read of each kind runs a pass over the capture via ssh)"
for kind in tick orderbook stream; do
    started=$SECONDS
    for symbol in AAPL SPY; do mounted "$kind/$symbol/$DAY.csv" > "$WORK/$kind.$symbol.csv" || fail "read $kind/$symbol"; done
    pass "$kind: $(wc -l < "$WORK/$kind.AAPL.csv") rows for AAPL in $((SECONDS - started))s"
    [ "$(app head -c4 "/mnt/datafeed/$kind/AAPL/$DAY.parquet")" = "PAR1" ] && pass "$kind parquet has the PAR1 magic" || fail "$kind parquet magic"
done

step "6. headers"
[ "$(head -1 "$WORK/tick.AAPL.csv")" = "timestamp,event,bid,bid_size,ask,ask_size,trade_price,trade_size,seq" ] && pass "tick header" || fail "tick header: $(head -1 "$WORK/tick.AAPL.csv")"
expected="timestamp,event"
for level in 1 2 3 4 5; do expected="$expected,bid_$level,bid_size_$level,ask_$level,ask_size_$level"; done
expected="$expected,trade_price,trade_size,seq"
[ "$(head -1 "$WORK/orderbook.AAPL.csv")" = "$expected" ] && pass "orderbook header (depth 5 from the config)" || fail "orderbook header: $(head -1 "$WORK/orderbook.AAPL.csv")"
[ "$(head -1 "$WORK/stream.AAPL.csv")" = "timestamp,event,seq,ref,new_ref,side,price,shares" ] && pass "stream header" || fail "stream header: $(head -1 "$WORK/stream.AAPL.csv")"

step "7. golden samples (first rows and rows 20001-20005 of AAPL)"
for kind in tick orderbook stream; do
    sample="$WORK/$kind.sample"
    sed -n '1,6p;20001,20005p' "$WORK/$kind.AAPL.csv" > "$sample"
    golden="e2e/golden/$kind.AAPL.sample.csv"
    if [ "$update" -eq 1 ]; then
        cp "$sample" "$golden"; pass "$kind golden rewritten"
    elif diff -u "$golden" "$sample" >"$WORK/$kind.diff"; then
        pass "$kind sample matches $golden"
    else
        fail "$kind sample differs from $golden"; cat "$WORK/$kind.diff" >&2
    fi
done

step "8. invariants over the whole files (AAPL and SPY)"
for symbol in AAPL SPY; do
    tick="$WORK/tick.$symbol.csv" book="$WORK/orderbook.$symbol.csv" stream="$WORK/stream.$symbol.csv"
    # tick: time and sequence never go back, a two sided quote is never crossed
    result=$(awk -F, 'NR>1 { if ($1 < t || $9 < s) bad++; t=$1; s=$9; if ($3 != "nan" && $5 != "nan" && $3 >= $5) crossed++ } END { print bad+0, crossed+0, NR-1 }' "$tick")
    [ "${result% *}" = "0 0" ] && pass "$symbol tick: ordered, never crossed (${result##* } rows)" || fail "$symbol tick ordering/crossing: $result"
    # orderbook: bids descend, asks ascend, levels are filled from the top without holes
    result=$(awk -F, 'NR>1 { for (k = 1; k < 5; k++) { b = 3 + 4*(k-1); a = 5 + 4*(k-1);
            if ($(b+4) != "nan" && ($(b) == "nan" || $(b+4) >= $(b))) bad++;
            if ($(a+4) != "nan" && ($(a) == "nan" || $(a+4) <= $(a))) bad++ }
            if ($3 != "nan" && $5 != "nan" && $3 >= $5) crossed++ } END { print bad+0, crossed+0, NR-1 }' "$book")
    [ "${result% *}" = "0 0" ] && pass "$symbol orderbook: bids descend, asks ascend, no holes, never crossed (${result##* } rows)" || fail "$symbol orderbook levels: $result"
    # orderbook vs tick: every tick row has an orderbook row with the same event and seq and the same best bid and ask; the orderbook has more rows
    result=$(awk -F, 'FNR==NR { if (FNR>1) key[$2 " " $NF] = $3 " " $4 " " $5 " " $6; next }
            { n++; k = $2 " " $9; v = $3 " " $4 " " $5 " " $6; if (!(k in key)) missing++; else if (key[k] != v) differ++ }
            END { print missing+0, differ+0, n }' "$book" <(tail -n +2 "$tick"))
    [ "${result% *}" = "0 0" ] && pass "$symbol tick rows are all in the orderbook with the same touch (${result##* } rows)" || fail "$symbol tick vs orderbook: $result"
    ticks=$(($(wc -l < "$tick") - 1)) books=$(($(wc -l < "$book") - 1))
    [ "$books" -gt "$ticks" ] && pass "$symbol orderbook has more rows than tick ($books > $ticks): changes below the touch" || fail "$symbol orderbook rows $books not above tick rows $ticks"
    # stream: every order is added before it is changed, and every tick trade is an execution, a non-displayed trade or a cross of the stream
    result=$(awk -F, 'NR>1 { if ($2 == "A") live[$4] = 1; else if ($2 == "U") { if (!($4 in live)) bad++; live[$5] = 1 } else if ($2 == "E" || $2 == "C" || $2 == "X" || $2 == "D") { if (!($4 in live)) bad++ } n[$2]++ }
            END { print bad+0, n["A"]+0, n["E"]+0, n["C"]+0, n["X"]+0, n["D"]+0, n["U"]+0, n["P"]+0, n["Q"]+0 }' "$stream")
    [ "${result%% *}" = "0" ] && pass "$symbol stream: no order is changed before it is added (A E C X D U P Q = ${result#* })" || fail "$symbol stream lifecycle: $result"
    result=$(awk -F, 'FNR==NR { if (FNR>1 && $2 ~ /^[ECPQ]$/) seen[$3 " " $7 " " $8] = 1; next }
            { n++; if (!(($9 " " $7 " " $8) in seen)) missing++ } END { print missing+0, n+0 }' "$stream" <(awk -F, 'NR>1 && ($2 == "T" || $2 == "X")' "$tick"))
    [ "${result% *}" = "0" ] && pass "$symbol every tick trade or cross is an execution, trade or cross of the stream (${result#* } rows)" || fail "$symbol tick trades vs stream: $result"
done

step "9. a second read is served from the cache"
before=$(docker logs "$APP" 2>&1 | grep -c "pass over" || true)
mounted "tick/AAPL/$DAY.csv" >/dev/null
after=$(docker logs "$APP" 2>&1 | grep -c "pass over" || true)
[ "$before" = "$after" ] && pass "no new pass over the capture" || fail "a cached file started a new pass"

echo
if [ "$failures" -eq 0 ]; then echo "e2e: all checks passed"; else echo "e2e: $failures check(s) FAILED" >&2; docker logs "$APP" 2>&1 | tail -20 >&2; exit 1; fi
