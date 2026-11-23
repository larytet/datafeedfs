# End-to-end test plan

Goal: prove that the **container image** mounts the filesystem, reads a compressed pcap **over ssh**, and serves correct
`tick/`, `orderbook/` and `stream/` files, without any real machine or real market data.

```
 e2e/run.sh
   |  docker build                      datafeedfs-e2e        (the image under test, the default Dockerfile target)
   |  docker build e2e/sshd             datafeedfs-e2e-sshd   (the "pcap host": sshd + the capture, user e2e, key only)
   |
   |   docker network dfs-e2e
   |   +-------------------------+   ssh e2e@dfs-e2e-sshd   +--------------------------+
   |   | dfs-e2e-app             | -----------------------> | dfs-e2e-sshd             |
   |   | datafeedfs mount        |   cat nasdaq_tv.pcap.xz  | /data/pcaps/2026/11/20/  |
   |   | /mnt/datafeed (FUSE)    |                          |   nasdaq_tv.pcap.xz (LFS)|
   |   +-------------------------+                          +--------------------------+
   |        ^ docker exec: ls / cat / awk on /mnt/datafeed
```

## The capture

`testdata/pcaps/2026/11/20/nasdaq_tv.pcap.xz` is about 5 MB, stored in **Git LFS** (`.gitattributes`). It is synthetic and deterministic, written by
`tools/genpcap`: 8 symbols (AAPL MSFT NVDA SPY QQQ TSLA AMD META), 390 000 ITCH 5.0 messages in about 120 000 MoldUDP64 packets on UDP port 26477, with adds (`A`, `F`),
partial cancels, deletes, replaces, executions (`E`, `C`), non-displayed trades and crosses. The generated book never crosses. Prices are not realistic (about $2 to $8).

Regenerate it (the golden files must then be regenerated too):

```
go run ./tools/genpcap -messages 390000 -out /tmp/day.pcap
xz -9 -T0 -c /tmp/day.pcap > testdata/pcaps/2026/11/20/nasdaq_tv.pcap.xz
```

## Prerequisites

- Docker (with `/dev/fuse` and permission for `--cap-add SYS_ADMIN`), `bash`, `ssh-keygen`, `awk` on the machine that runs the test.
- `git lfs pull` after a clone, otherwise the capture is a pointer file (the script says so and stops).
- No ssh access to anything: the test creates a throwaway key, pins the sshd container's host key and removes everything afterwards.

## Run

```
e2e/run.sh                    # about 2 minutes; exit status 0 when every check passed
e2e/run.sh --keep             # leave dfs-e2e-app and dfs-e2e-sshd running to poke around: docker exec dfs-e2e-app ls /mnt/datafeed/orderbook/AAPL
e2e/run.sh --update-golden    # rewrite e2e/golden from the current output; review `git diff e2e/golden` before committing
```

## Steps and expected results

| # | Step | Expected |
|---|------|----------|
| 0 | the capture exists and is not an LFS pointer | `ok capture ... (4.8M)` |
| 1 | `docker build .` and `docker build e2e/sshd` | both images build |
| 2 | start `dfs-e2e-sshd` on network `dfs-e2e`, install the throwaway public key, pin the host key | container up |
| 3 | `docker run -d --device /dev/fuse --cap-add SYS_ADMIN ... datafeedfs-e2e` with `e2e/config.e2e.toml` (`ssh_host = "e2e@dfs-e2e-sshd"`, `[orderbook] depth = 5`) | `/mnt/datafeed` is a mountpoint within 20 s |
| 4 | layout | root lists `.status orderbook stream tick`; each kind lists `AAPL AMD META MSFT NVDA QQQ SPY TSLA`; `tick/AAPL` has `2026-11-20.csv` and `.parquet`; `datafeedfs-go days` prints `2026-11-20` (found over ssh) |
| 5 | read `AAPL` and `SPY` of each kind (the first read of a kind runs one pass: `ssh cat \| xz -dc \| itch_tob \| ingest`) | each kind takes about 7 s; the `.parquet` starts with `PAR1`. About 17 900 tick, 36 200 orderbook and 48 600 stream rows for AAPL |
| 6 | headers | tick `timestamp,event,bid,bid_size,ask,ask_size,trade_price,trade_size,seq`; orderbook `timestamp,event,bid_1,bid_size_1,ask_1,ask_size_1,...,bid_5,bid_size_5,ask_5,ask_size_5,trade_price,trade_size,seq` (25 columns); stream `timestamp,event,seq,ref,new_ref,side,price,shares` |
| 7 | golden samples: rows 1-6 and 20001-20005 of AAPL of each kind against `e2e/golden/<kind>.AAPL.sample.csv` | identical (a change in the decoder output shows up as a diff) |
| 8 | invariants over the whole AAPL and SPY files (below) | all hold |
| 9 | read `tick/AAPL` again | served from the cache: no new `pass over` line in the container log |

Step 8 invariants:

- **tick**: timestamp and seq never decrease; a two sided quote is never crossed (`bid < ask`).
- **orderbook**: bids strictly descend and asks strictly ascend with the level number; a level is never present below an empty one; never crossed.
- **tick vs orderbook**: every tick row has an orderbook row with the same `event` and `seq` and the same best bid, bid size, ask and ask size; the orderbook has more rows (changes below the touch).
- **stream**: every `E C X D U` refers to an order that was added (`A`/`F`, or the new reference of a `U`) before; the counts of `A E C X D U P Q` are printed.
- **stream vs tick**: every tick `T` (trade) and `X` (cross) row is an `E`, `C`, `P` or `Q` event of the stream with the same `seq`, price and shares.

## Samples to look at by hand

After `e2e/run.sh --keep`:

```
docker exec dfs-e2e-app head -4 /mnt/datafeed/tick/AAPL/2026-11-20.csv
docker exec dfs-e2e-app head -4 /mnt/datafeed/orderbook/AAPL/2026-11-20.csv
docker exec dfs-e2e-app head -4 /mnt/datafeed/stream/AAPL/2026-11-20.csv
docker exec dfs-e2e-app cat /mnt/datafeed/.status
docker logs dfs-e2e-app            # one "pass over 2026-11-20 ..." line per kind
```

The first rows of AAPL, which are the golden samples: the sell side fills first (`nan` bid), then the second level appears in the orderbook while the tick file still shows only the touch.

```
tick       Q,nan,0,5.9,300           Q,nan,0,5.81,100                          Q,nan,0,5.81,1300
orderbook  Q,nan,0,5.9,300,...       Q,nan,0,5.81,100,nan,0,5.9,300,...        Q,nan,0,5.81,1300,nan,0,5.9,300,...
stream     A,26,18,0,S,5.9,300       A,28,20,0,S,5.81,100                      A,30,22,0,S,5.81,1200
```

## Failure triage

| Symptom | Look at |
|---------|---------|
| `is missing or is a Git LFS pointer` | `git lfs pull` |
| not mounted after 20 s | `docker logs dfs-e2e-app`: `/dev/fuse` not available, or `SYS_ADMIN` / AppArmor denied |
| `days` empty or `Input/output error` on a read | ssh from the app to the sshd container: `docker exec dfs-e2e-app ssh -o BatchMode=yes -i /root/.ssh/id_e2e -o UserKnownHostsFile=/root/.ssh/known_hosts e2e@dfs-e2e-sshd ls /data/pcaps` ; then the per-pass log `docker exec dfs-e2e-app ls /cache/_logs` |
| a golden sample differs | an intended change of the decoder or the schema: review the diff, then `e2e/run.sh --update-golden`; an unintended one is a regression |
| an invariant fails | the printed numbers are `bad crossed rows` (or `missing differ rows`); reproduce on the kept container with the awk line in `e2e/run.sh` step 8 |

## Not covered

Real pcaps (size, port, A/B feed duplicates, sequence gaps), `on_miss = "async"`, `whole_day`, the docker-compose bind-propagated mount on a host, and the cases that `go test ./src` already covers with a tiny capture (duplicate packets, replace, non-displayed trade).
