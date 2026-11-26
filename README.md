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

## Feeds (one directory per venue feed)

A host usually holds several pcap files per day, one per venue feed (`edga.pcap.xz`, `edgx.pcap.xz`, `nasdaq_tv.pcap.xz`, `iex_deep.pcap.xz`, ...). Declare each as a `[feeds.NAME]` table and every feed gets its own
directory in the mount and in the cache, with the same tree as above under it:

```
/nasdaq_tv/tick/AAPL/2026-11-24.parquet
/cboe_edga/orderbook/SPY/2026-11-24.csv
/iex_deep/stream/SPY/2026-11-24.parquet
```

```toml
[remote]                          # defaults for every feed
ssh_host = "user@pcap-host"
pcap_path = "/data/pcaps/{yyyy}/{mm}/{dd}/{feed}.pcap.xz"   # {feed} is the table name
[parser]                          # default parser, used by a feed that does not set its own
command = "parsers/parse_nasdaq_itch.sh"

[feeds.nasdaq_tv]                 # nothing to set: /data/pcaps/2026/11/24/nasdaq_tv.pcap.xz, the default parser
[feeds.cboe_edga]                 # a feed can override pcap_path, ssh_host, ssh_options and the parser
pcap_path = "/data/cboe/{yyyy}-{mm}-{dd}/edga.pcap.xz"
[feeds.cboe_edga.parser]
command = "parsers/parse_cboe_pitch.sh"
env = { DFS_PORT = "30101" }      # merged over [parser] env
```

Names are lowercase letters, digits, `_` and `-`. Without any `[feeds.*]` table nothing changes: one anonymous feed, `/tick`, `/orderbook`, `/stream` at the root. The CLI takes `--feed NAME`
(`days`, `symbols`, `prefetch`; `datafeedfs-go feeds CONFIG` lists them), the cache is `<cache>/<feed>/<kind>/...`, the console and `/.status` name the feed of every pass, and telemetry gets a `feed` attribute.
The `cache` size cap, `max_parallel` and the batch window are shared by all feeds. The bundled parser decodes Nasdaq ITCH only: feeds in other protocols (CBOE PITCH, NYSE, IEX DEEP, ...) need a parser script
that follows the contract below; it is told which feed it is running for in `DFS_FEED`, so one script can serve several.

For the four Cboe US equities feeds (EDGA, EDGX, BYX, BZX) use `parsers/parse_cboe_pitch.sh`, which decodes PITCH:

```toml
[feeds.cboe_edga]
parser = { command = "parsers/parse_cboe_pitch.sh", env = { DFS_PORT = "30101", DFS_LIST_STOP_AT = "36000" } }
```

PITCH differs from ITCH in two ways you will notice. It has no symbol directory, so the first listing of a day reads add orders: until the end of the pcap, or until `DFS_LIST_STOP_AT`
(seconds since midnight, exchange time) if set, which is much cheaper but then misses symbols that first appear later. And a capture carries several units (the feed is split by symbol range), each with its own
sequence number, so `seq` in the files is `unit << 32 | unit sequence`; it orders rows within a unit, not across units. Order Executed at Price/Size is event `C`, Modify Order is `U` (the old and new `ref` are the same order),
non-displayed Trade is `P`. Auction summaries (the `X` rows of ITCH) are not decoded. The layouts are those of the PITCH spec as the tests encode them (`src/pitch_test.go`); check a first extraction
against a known day before relying on it.

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

## CSV files are short-lived

A `.csv` is rendered from the parquet on first read and is several times larger. It is deleted once no handle has been open on it for `[cache] csv_idle_seconds` (default 60; negative keeps csv files). The next read renders it again from the parquet, which is cheap (no pass over the pcap). A file that is open is never deleted underneath its reader. This is independent of `max_gb`.

## Cache size cap (LRU eviction)

By default the cache grows without bound. Set `[cache] max_gb` to cap the parquet and csv files (the `_work`, `_logs` and `_symbols` directories are not counted). Eviction is lazy so that reads never churn the disk:

- A read bumps the file's mtime, at most once per `touch_interval_seconds` (default 3600) per file; there is no other bookkeeping on the read path.
- A background pass runs every `scan_interval_seconds` (default 600) and does nothing while the cache is under `max_gb`.
- Over the cap it deletes the least recently used files, 50 at a time with a pause between batches, down to `evict_to` times the cap (default 0.9). The gap is hysteresis: the next passes find nothing to do until the cache has grown by it again.
- The cache can therefore exceed the cap by what is written between passes. Leave headroom on the disk (the cap is not a quota).
- Never evicted: files used in the last hour (`min_age_seconds`, never below the touch interval) and files under 16 KiB. The latter are the empty tables that mark a day as extracted; deleting one would cost a whole pass over the pcap.
- An evicted file is simply a cache miss: the next read extracts it again, which costs a pass over that day's pcap, not just that file.

## Web console

Set `[ui] listen` (for example `127.0.0.1:8088`) and the mount process serves a console in the browser, or run `datafeedfs-go serve CONFIG` to serve it without a mount. Four tabs and the raw file:

- **Status**: passes running and waiting, failures, the cache by kind against the cap and the eviction target, mount details.
- **Configuration**: every setting of `config.toml` as a form. Click a value to edit it, Enter applies it to the form, Esc cancels. Nothing is written until **Save config.toml**; **Review changes** shows a diff.
- **Performance**: network received, cache hit ratio, durations of recent passes, cache writes.
- **Load**: CPU by process group, load average, extraction slots, the busiest processes, memory and disk.
- **config.toml**: the raw file with line numbers. It is the same text as the form: an edit in one shows in the other, and form edits keep your comments.

It follows the browser's light or dark theme and has no external dependencies (one embedded page, no fonts or scripts from other sites).

| `[ui]` setting | meaning |
|---|---|
| `listen` | `host:port`; empty (the default) turns the console off. A non-loopback address requires `token` |
| `allow_edit` | `false` (default): the console is read-only. `true`: it can save `config.toml` |
| `allow_restart` | `true` shows a **Restart now** button that stops the process so its supervisor (the Docker restart policy) starts it with the saved config |
| `token` | bearer token for `/api`; the page asks for it |

A saved config takes effect only after a restart: the page shows a banner while the file on disk differs from what the process started with. Saving rewrites the file the process was started with, so in Docker the config bind mount must not be `:ro`, and the console port must be published (for example `ports: ["127.0.0.1:8088:8088"]` with `listen = "0.0.0.0:8088"` and a `token`).

Security: whoever can save the config can change `parser.command`, which the process runs. That is why editing is off by default, the default bind is loopback, a token is required off loopback, writes need an `X-Requested-With` header and a same-origin `Origin`, and without a token only `localhost` Host names are served (DNS rebinding). Keep `allow_edit` off unless you trust everyone who can reach the port.

The page reads a small JSON API: `GET /api/status`, `/api/metrics?range=5m|15m|1h`, `/api/config`, `POST /api/config/validate`, `PUT /api/config`, `POST /api/restart`. Host figures (CPU, load, memory, network received) are read from `/proc` every 10 seconds and kept for an hour; process CPU is polled every second so short-lived processes (a pass over a small pcap) are still counted in their group. "other" is the rest of the host's CPU, including software outside the container. On other platforms the figures are empty. In a container the CPU, load and memory are the host's and the network and processes are the container's. `?refresh=<seconds>` changes the 3 second poll interval.

## OpenTelemetry

Optional and off by default. With `[telemetry] enabled = true` the process exports metrics and one trace per extraction pass over OTLP/HTTP; with it off the instruments are no-ops. It is runtime config only: there is nothing to build differently.

```toml
[telemetry]
enabled = true
endpoint = "http://otel-collector:4318"   # base URL; empty uses OTEL_EXPORTER_OTLP_ENDPOINT or http://localhost:4318
service_name = "datafeedfs"
metrics_interval_seconds = 15
# metrics = true, traces = true: set either to false to export only the other
```

The standard `OTEL_EXPORTER_OTLP_*` variables (headers, certificates, timeouts) are honoured by the exporters. Metrics (meter `datafeedfs`):

| metric | type | attributes |
|---|---|---|
| `datafeedfs.cache.requests` | counter | `kind`, `result` = hit, miss, joined (a miss that was already being extracted) |
| `datafeedfs.open.wait` | histogram, s | `kind`, `result` = hit, miss |
| `datafeedfs.pass.duration` | histogram, s | `kind`, `status` = ok, failed |
| `datafeedfs.pass.symbols` | histogram | `kind` |
| `datafeedfs.passes` | counter | `kind`, `status` |
| `datafeedfs.cache.evicted.files`, `.bytes` | counters | |
| `datafeedfs.passes.running`, `.pending`, `.failed` | gauges | |
| `datafeedfs.cache.bytes`, `.files` | gauges, as of the last cache scan | `kind`, `format` = parquet, csv (sum over `format` for the total) |
| `datafeedfs.cache.limit` | gauge, bytes (0 = unlimited) | |

Traces: a span `pass` per extraction pass (`kind`, `day`, `symbols.requested`, `status`, the error when it failed) with a child span `pipeline` around `ssh cat | decompress | decoder | ingest`. Individual reads are not traced. The decoder and `xz` are child processes and are not instrumented; host CPU and network belong to the collector's `hostmetrics` receiver.

## Parser contract

`[parser] command` is run once per pass with the pcap, exactly as stored on the host, on stdin, and these environment variables:

| variable | meaning |
|---|---|
| `DFS_FEED` | the `[feeds.NAME]` being read (empty without feeds) |
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
The browser tests of the web console (`e2e/ui`, Playwright) are described below. The end-to-end test (`e2e/run.sh`, plan in [testplan.md](testplan.md)) builds the image, serves a 5 MB synthetic pcap (Git LFS, made by `tools/genpcap`) from an sshd container and checks samples of the tick, orderbook and stream files.

Browser tests of the web console (Playwright, needs Node 20 or newer, Go, gcc, xz and, for the extraction tests, FUSE):

```
cd e2e/ui
npm ci
npx playwright install chromium     # once; or set PW_CHROME=/usr/bin/google-chrome to use an installed Chrome
npm test                            # builds the binary, starts two instances and runs 16 tests in about 40 seconds
```

One instance is the mounted filesystem with editing on: it checks the status page, a real extraction through the mount (a running pass, then cache), the charts, the light and dark theme, editing a field in place, the diff, saving to disk, the restart banner, the raw editor, server-side validation, and a save refused after the file changed on disk. The other is console-only with editing off and a token: it checks the token prompt, read-only fields and raw editor, and that the API refuses writes. Without FUSE the extraction checks are skipped and the instance runs without a mount.
