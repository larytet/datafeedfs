# datafeedfs

A user-space filesystem (FUSE) that presents exchange market data pcap files as a tree of symbols and serves
tick-to-tick top of book (best bid / ask) and trade data parsed from those files.

The pcap files are the output of BEAK capture traffic (the exchange feed captured off the wire) and stay on a remote host. The filesystem is configured with an ssh host and the path where the pcap files live,
and with a script that parses and filters the pcap stream down to the requested symbols.

```
/tick/                           symbols (of the newest day)
/tick/SPY/2026-11-20.parquet     tick-to-tick best bid / ask and trades of SPY on that day (one .parquet and one .csv per trading day found on the ssh host)
/orderbook/SPY/2026-11-20.parquet  the top N price levels per side (N = `[orderbook] depth`, default 5), a row whenever one changes, and trades
/stream/SPY/2026-11-20.parquet   every order message of the day (add, execute, cancel, delete, replace, trade, cross), no book
/.status                         JSON: running and pending extractions, failures
```

Every file also exists as `.csv`. The three kinds are extracted by separate passes, so you pay for depth or for the raw stream only when you read it.

**tick**: each row is an event in feed order: `timestamp` (exchange time as UTC), `event` (`Q` best bid/ask changed,
`T` trade, `X` cross), `bid`, `bid_size`, `ask`, `ask_size` (the book at that moment, for a trade the book as it was before it),
`trade_price`, `trade_size`, `seq` (MoldUDP64 sequence number). An empty side is `NaN` / 0.

**orderbook**: the same events and `seq`, but with levels instead of the touch only: `timestamp, event, bid_1, bid_size_1, ask_1, ask_size_1, ..., bid_N, bid_size_N, ask_N, ask_size_N, trade_price, trade_size, seq`.
Level 1 is the best price; a level that does not exist is `NaN` / 0. A `Q` row is written whenever any of the top N levels changes, so it has more rows than the tick file
(a change below the touch is a row here and not there); join the two on `seq` (plus `event`: a trade and the book change of the same message share a `seq`).
`depth` is set in the config (1 to 64); a different depth is a separate cache, so changing it never serves files of another depth.

**stream**: the order messages as they came, for the symbol: `timestamp, event, seq, ref, new_ref, side, price, shares`.
`event` is `A` add, `E` execute at the order's price, `C` execute at another price (`price` is the execution price), `X` cancel (`shares` cancelled), `D` delete (`shares` remaining before it),
`U` replace (`ref` the old order, `new_ref` the new one, `price` and `shares` the new ones), `P` non-displayed trade, `Q` cross. `ref` is 0 for `P` and `Q`; `side` is `B` or `S`.
Replaying the stream rebuilds the full book at any depth and lets you model queue position. Orders are tracked from the start of the pcap, so a pcap that starts mid-session misses the orders placed before it.

## Layout

```
src/       the filesystem (Go, go-fuse and parquet-go): a single static binary, with its tests
Dockerfile, docker-compose*.yml, docker-entrypoint.sh, config.docker.toml   the container setup
parsers/   itch_tob.c (C: ITCH order-book decoder) and parse_nasdaq_itch.sh (the parser contract script)
```

The pipeline of a pass is `ssh cat | xz -dc -T0 | itch_tob | datafeedfs-go ingest`: the C decoder rebuilds the order books and the Go `ingest`
subcommand writes the parquet files. `itch_tob` has three output modes, selected by the parser script from `DFS_KIND`: the default (best bid and ask), `--depth N` (top N levels) and `--events` (raw order messages).

## How it works

1. `ls` of the configured pcap path on the ssh host gives the trading days; the parser's `list` mode gives the symbols.
2. Opening `/KIND/SYMBOL/DAY.parquet` that is not cached starts `ssh host cat <pcap> | parser` for that day.
   Requests for the same day and kind that arrive within `batch_window_seconds` are served by one pass, because a pass over a
   day costs the same for one symbol or a thousand (a full Nasdaq TotalView day is tens of GB compressed: expect one to two hours).
3. The parser writes one parquet per symbol; they are cached under the cache directory and served from there afterwards.
   `.csv` files are rendered from the parquet on first read.

While a file is not cached `ls -l` shows size 0; the mount uses `direct_io`, so readers (`cat`, parquet libraries) do not depend on the size.
A cached file shows its real size, so **size 0 means not cached**, and a cheap, approximate inventory needs no extra tooling (`stat` does not start an extraction, only `open` does):

```
find ~/datafeed/orderbook/QQQ -name '*.parquet' -size +0     # the cached days of one symbol
ls -ls ~/datafeed/tick/NVDA | awk '$6 > 0'                    # same with sizes, size column > 0
```

Do not run this over the whole tree: a full listing is millions of entries (symbols x days x 3 kinds). The answer can also be out of date: files evicted by the cache cap (below) show size 0 again.
`on_miss = "async"` makes `open()` fail with `EAGAIN` instead of blocking while extraction runs in the background (watch `/.status`).
Use `datafeedfs prefetch` to extract ahead of time, e.g. many symbols for a day in a single pass.

## Usage

```
go build -o datafeedfs-go ./src
cp config.example.toml config.toml          # set ssh_host, pcap_path (the parser path is relative to the config file)
export PATH=$PWD:$PATH                            # the parser script calls `datafeedfs-go ingest`
datafeedfs-go days config.toml
datafeedfs-go mount config.toml ~/mnt/datafeed &
ls ~/mnt/datafeed/tick/SPY
head -5 ~/mnt/datafeed/tick/SPY/2026-11-20.csv        # the parquet file opens in any parquet reader
fusermount -u ~/mnt/datafeed
datafeedfs-go prefetch config.toml 2026-11-20 AAPL MSFT NVDA     # one pass for all three (tick files)
datafeedfs-go prefetch --kind orderbook config.toml 2026-11-20 AAPL    # or --kind stream
```

Requirements (without Docker): Go, FUSE (`fusermount`), `gcc` (the parser builds `parsers/itch_tob` on first use), `xz` (5.4 or newer decompresses on all cores)
on the machine that runs the parser, and passwordless ssh to the host.

## Deploy with Docker, step by step

The repository root holds the complete Docker setup: `Dockerfile`, `docker-entrypoint.sh`, `docker-compose.yml`, `docker-compose.key.yml`, `docker-compose.agent.yml`
and `config.docker.toml`. Run every command below from the repository root.
The image holds the static Go binary and the compiled parser; nothing else. Everything is Go and C; there is no Python. The host needs Docker with Compose, a Linux kernel with FUSE (`/dev/fuse`),
and passwordless ssh from the host to the machine that stores the pcap files.

1. **Get the code on the host**
   ```
   git clone git@github.com:larytet/datafeedfs.git && cd datafeedfs
   ```
2. **Check ssh to the pcap host works without a password, from the host, and that its key is already trusted.** The container runs ssh with `BatchMode`, so it never
   prompts; a host key that is not in `known_hosts` fails the connection.
   ```
   ssh user@pcap-host hostname          # must print the host name straight away; answer "yes" once if asked to trust the key
   ```
3. **Write the config**
   ```
   cp config.docker.toml config.toml
   $EDITOR config.toml                  # set ssh_host (user@pcap-host) and pcap_path (one pcap per day, with {yyyy} {mm} {dd})
   ```
4. **Create the directory where the filesystem will appear on the host.** Create it yourself: Docker runs as root and cannot create directories on NFS / EFS homes (`chown ... operation not permitted`).
   ```
   mkdir -p ~/datafeed
   ```
5. **Build and start.** Pick how the container logs in to the pcap host:
   ```
   export DFS_HOST_MOUNT=$HOME/datafeed

   # a key file that may log in to the pcap host (works unattended, survives reboots)
   export DFS_SSH_KEY=$HOME/.ssh/id_ed25519
   docker compose -f docker-compose.yml -f docker-compose.key.yml up -d --build

   # or the host's ssh agent (works only while the agent lives, e.g. not after an `ssh -A` session ends)
   docker compose -f docker-compose.yml -f docker-compose.agent.yml up -d --build
   ```
   Use the same `-f` files for every later `docker compose` command.
6. **Check it**
   ```
   mountpoint ~/datafeed                # "is a mountpoint"
   ls ~/datafeed                        # tick  orderbook  stream
   ls ~/datafeed/tick | head            # symbols
   ls ~/datafeed/tick/SPY               # one .parquet and one .csv per trading day
   cat ~/datafeed/.status               # running / pending extractions and failures
   docker compose -f docker-compose.yml -f docker-compose.key.yml logs -f
   ```
7. **Use it.** The first read of a symbol and day starts a pass over that day's pcap and blocks until it is done (one to two hours for a full Nasdaq day); later reads are served from the cache.
   To warm the cache, extract many symbols in one pass without mounting:
   ```
   docker compose -f docker-compose.yml -f docker-compose.key.yml run --rm datafeedfs prefetch 2026-11-20 AAPL MSFT NVDA
   head -5 ~/datafeed/orderbook/AAPL/2026-11-20.csv      # or open the .parquet in any parquet reader
   ```
8. **Update to a new version**
   ```
   git pull && docker compose -f docker-compose.yml -f docker-compose.key.yml up -d --build
   ```
9. **Stop.** `down` unmounts the filesystem from the host; the cache volume is kept.
   ```
   docker compose -f docker-compose.yml -f docker-compose.key.yml down
   ```
10. **Run the tests** (`go vet`, the unit tests and the end-to-end test of the parser on a synthetic pcap)
    ```
    docker build --target test -t datafeedfs-go-test .
    docker run --rm datafeedfs-go-test
    ```

One-off commands run without a mount: `docker compose ... run --rm datafeedfs days | symbols [DAY] | prefetch [--kind tick|orderbook|stream] DAY SYMBOL...`.

How it works in a container: the container needs `--device /dev/fuse`, `SYS_ADMIN` and no AppArmor / SELinux label (`docker-compose.yml` sets these). The mount reaches the host through `rshared`
bind propagation, so the host directory must be on a shared mount (the default on systemd hosts), and the filesystem is mounted with `allow_other`. The cache is the `cache` volume (`/cache`).

Troubleshooting:

| symptom | cause and fix |
|---|---|
| `error while creating mount source path ... chown ... operation not permitted` | create the host directory yourself (step 4) |
| `required variable DFS_SSH_KEY is missing` | export it, also for `build` and `down`, or use the agent override |
| `ls` of a symbol shows files but `cat` fails with `Input/output error` | the pass failed; see `/.status` and `docker compose logs`, and the per-pass logs in the cache volume under `_logs/` |
| `Transport endpoint is not connected` on the host directory | the container died without unmounting: `docker compose ... down`, then `fusermount -uz ~/datafeed` (or `umount -l` as root) |
| ssh errors in the logs | repeat step 2 from the host; the container uses the same key and `known_hosts` |
| `open()` returns `EAGAIN` | `on_miss = "async"` is set: the extraction runs in the background, retry later or watch `/.status` |

## Concurrency and resources

- Reading the pcap is the expensive part, and a pass costs the same for one symbol or for every symbol of the day. Requests for a day inside
  `batch_window_seconds` share one pass. With `whole_day = true` the first miss for a day extracts all of that day's symbols (about 0.5 to 3 GB of parquet),
  so every later read of that day is a cache hit and the pcap is read once per day.
- Reading cached files costs no extra processes: the filesystem is multi-threaded and serves them with `pread`, any number in parallel.
- Opening files that are not cached queues one extraction per (symbol, day). Requests for the **same day** that arrive within `batch_window_seconds` (3 s) share **one** pass, so
  reading many symbols of one day at once starts one parser pipeline, not one per file. A request for that day that arrives **after** its pass has started cannot join it (the pcap stream
  is already past that point) and waits for the next pass of that day. Use `prefetch` with the full list of symbols you need to avoid this.
- Different days run in parallel, up to `max_parallel` passes (default 2). Each pass is a pipeline of four processes: `ssh cat`, `xz -dc`, the C decoder `itch_tob` and `datafeedfs-go ingest`.
  A pass uses roughly 2 to 3 cores; `xz -T0` decompresses the multi-block pcap on all cores (about 10x faster than one core on 16 cores), after which the single-threaded decoder `itch_tob` (about 70 MB/s of decompressed pcap per core) is the limit. To use more cores, raise `max_parallel`
  and read several days at once; 6 parallel days pushed a 16-core machine to a load average near 29, so size `max_parallel` to the cores you have.
- I/O: every pass reads the whole compressed pcap over ssh from the host (tens of GB for Nasdaq), so several passes share the host's disk, network and its single `sshd` encryption core.
  Raising `max_parallel` helps until that, or the CPU of the machine running the container, becomes the limit.

## Cache size cap (LRU eviction)

By default the cache grows without bound. Set `[cache] max_gb` to cap the parquet and csv files (the `_work`, `_logs` and `_symbols` directories are not counted). Eviction is lazy so that reads never churn the disk:

- A read bumps the file's mtime, at most once per `touch_interval_seconds` (default 3600) per file; there is no other bookkeeping on the read path.
- A background pass runs every `scan_interval_seconds` (default 600) and does nothing while the cache is under `max_gb`.
- Over the cap it deletes the least recently used files, 50 at a time with a pause between batches, down to `evict_to` times the cap (default 0.9). The gap is hysteresis: the next passes find nothing to do until the cache has grown by it again.
- The cache can therefore exceed the cap by what is written between passes. Leave headroom on the disk (the cap is not a quota).
- Never evicted: files used in the last hour (`min_age_seconds`, never below the touch interval) and files under 16 KiB. The latter are the empty tables that mark a day as extracted; deleting one would cost a whole pass over the pcap.
- An evicted file is simply a cache miss: the next read extracts it again, which costs a pass over that day's pcap, not just that file.

## Parser contract

`[parser] command` is run once per pass with the pcap, exactly as stored on the host, on stdin, and these environment variables:

| variable | meaning |
|---|---|
| `DFS_MODE` | `list` (print the day's symbols, one per line) or `extract` |
| `DFS_DATE` | trading day `YYYY-MM-DD` |
| `DFS_PCAP_NAME` | file name on the host, to pick the decompressor |
| `DFS_SYMBOLS_FILE` | extract: symbols to keep, one per line |
| `DFS_KIND` | optional: `tick` (default), `orderbook` or `stream`, see above |
| `DFS_DEPTH` | optional: levels per side of the `orderbook` kind (default 5; datafeedfs sets it from `[orderbook] depth`) |
| `DFS_OUT_DIR` | extract: write `<SYMBOL>/<DFS_DATE>.parquet` here (symbols with no activity: no file, served as an empty table) |
| `DFS_PORT` | optional: decode only this UDP destination port (default: all ports in the pcap) |

Plus anything under `[parser] env`, and `DFS_INGEST` (set by datafeedfs to `<this binary> ingest`) and `DFS_STOP_AT` (optional, seconds since midnight exchange time at which decoding stops,
for short tests: `14520` is the first two minutes of the feed). `parsers/parse_nasdaq_itch.sh` is the bundled parser for Nasdaq TotalView-ITCH 5.0 over MoldUDP64:
`parsers/itch_tob.c` (C, about 70 MB/s of decompressed pcap per core) rebuilds the order book of the chosen symbols, drops duplicate packets by
sequence number and streams records to `datafeedfs-go ingest`, which writes the parquet files. Another feed needs only another script with the same contract.

## Tests

```
go vet ./... && go test ./...
```

`src/parser_test.go` builds a small pcap with a known order-book story (adds, partial execution, delete, replace, a non-displayed trade and a
duplicated packet) and checks the parser output exactly; `src/ingest_test.go` checks the parquet and CSV writers. Both need `gcc` and `bash`.
The end-to-end test (`e2e/run.sh`, plan in [testplan.md](testplan.md)) builds the image, serves a 5 MB synthetic pcap (Git LFS, made by `tools/genpcap`) from an sshd container and checks samples of the tick, orderbook and stream files.
