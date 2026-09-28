# Crypto Market Data Recorder

An asynchronous recorder that listens to live Binance spot and perpetual
futures market data over websockets and stores it on disk as **parquet**.

Every stream is collected concurrently and independently of the others, for
one or more symbols at a time:

| Stream | Market | Binance channel | What it records |
| --- | --- | --- | --- |
| `trade` | spot | `<symbol>@trade` | Every executed trade (tick data) |
| `book_ticker` | spot | `<symbol>@bookTicker` | Best bid/ask price and quantity, on every change |
| `depth` | spot | `<symbol>@depth@100ms` | Order book diff updates (100 ms) + local order book |
| `depth20` | spot | `<symbol>@depth20@100ms` | Binance's own top-20 book, as a reference to validate `depth`'s reconstructed book against |
| `futures_depth` | futures | `<symbol>@depth@100ms` | Same as `depth`, for the perpetual, using futures' `pu`-based sync rules |
| `futures_agg_trade` | futures | `<symbol>@aggTrade` | Aggregated perp trades |
| `futures_mark_price` | futures | `<symbol>@markPrice@1s` | Mark price, index price, funding rate |
| `futures_liquidation` | futures | `<symbol>@forceOrder` | Forced liquidation orders (**sample, not exhaustive** -- see Known limitations) |
| `futures_open_interest` | futures | REST poll, `/fapi/v1/openInterest` | Open interest (no push stream exists) |

`depth` and `futures_depth` also write two side files per symbol: a
per-second top-20 **snapshot** of the reconstructed book (`snapshots/`), and
an **events** log (`events/`) recording connects, disconnects, resyncs,
sequence gaps and (spot-only) cross-checks against `book_ticker`/`depth20`.
A `_GLOBAL` events stream additionally records periodic clock-offset checks against
Binance's server time, and `exchange_info/` stores a daily snapshot of tick
size / lot size / trading status for the tracked symbols. Every parquet file
carries the recorder's git commit hash in its metadata.

## Installation

```bash
make install
```

Or by hand:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Requirements: Python 3.10+ (developed on 3.13), `websockets`, `pandas`,
`pyarrow`, `aiohttp`, `sortedcontainers`.

## Usage

```bash
make run                             # depth, trade, book_ticker, default symbol (XRPUSDT)
make run btcusdt                     # different symbol
make run btcusdt ethusdt bnbusdt     # several symbols at once
make run btcusdt STREAMS="trade depth"   # only selected streams
make run btcusdt BUFFER=1000         # more frequent parquet writes
make trades ethusdt                  # a single stream
make futures btcusdt                 # all futures streams for one symbol
make run btcusdt DATA_DIR=/mnt/data  # write under a custom directory
make test                            # run the test suite
```

Symbols can be written bare after the target, as above, or passed as a
variable — `make run SYMBOL="btcusdt ethusdt"`. Both forms end up as
`--symbol btcusdt ethusdt`.

Stop with `Ctrl+C` or by sending `SIGTERM` (e.g. `systemctl stop`, `kill`,
Docker's default stop signal) — both are handled the same way: every task is
cancelled and whatever is still sitting in memory (buffers, event logs,
pending snapshots) is flushed to disk before the process exits.

A run looks like this:

```
============================================================
Crypto Market Data Recorder
Symbols: BTCUSDT, ETHUSDT
Streams: depth, trade, book_ticker
Buffer size: 5000
Connections: 6
Press Ctrl+C to stop
============================================================
[BTCUSDT] depth output: /home/you/crypto_market_data/data/raw/BTCUSDT/depth
[BTCUSDT] Connecting to depth stream...
...
[BTCUSDT] Snapshot loaded | lastUpdateId=99352275916 | bids=5000 | asks=5000
[BTCUSDT] Order book synchronized.
Best bid: ('77743.25000000', '0.79595000')
Best ask: ('77743.26000000', '3.35310000')
Spread: 0.00999999999476131
[BTCUSDT] Wrote 5000 trade records.
```

`Connections` is `symbols x streams` — one websocket per pair.

| Variable | Default | Maps to |
| --- | --- | --- |
| `SYMBOL` | `xrpusdt` | `--symbol` |
| `STREAMS` | `depth trade book_ticker` | `--streams` |
| `BUFFER` | `5000` | `--buffer-size` |

Lowercase `symbol=` / `streams=` / `buffer=` work too, since make variables are
otherwise case sensitive and silently ignore the wrong case.

### Targets

Running `make` on its own prints this list.

| Target | What it does |
| --- | --- |
| `make install` | Create `.venv` and install dependencies |
| `make run` | Record all selected streams |
| `make depth` / `make trades` / `make book` | Record a single stream |
| `make stats` | Summary of recorded data (file count / size) |
| `make clean` | Remove `__pycache__` directories |
| `make clean-data` | Delete recorded data (asks for confirmation) |

If `.venv` exists the targets use the python inside it automatically, otherwise
they fall back to the system `python3` — no need to activate the virtualenv
first.

`make stats` reports what has been recorded so far:

```
data/raw/BTCUSDT/book_ticker                     41 files      332K
data/raw/BTCUSDT/trades                          13 files      108K
data/raw/ETHUSDT/book_ticker                     22 files      180K
data/raw/ETHUSDT/trades                           9 files       76K
---
896K	data
```

### Without make

The Makefile is a thin wrapper; the CLI is the actual interface.

```bash
python main.py
python main.py --symbol btcusdt
python main.py --symbol btcusdt ethusdt xrpusdt
python main.py --symbol ethusdt --streams trade depth
python main.py --buffer-size 1000
```

| Argument | Default | Description |
| --- | --- | --- |
| `--symbol` | `xrpusdt` | One or more Binance trading pairs. Case insensitive, duplicates ignored. |
| `--streams` | `depth trade book_ticker` | Streams to record. See the table above for all spot/futures choices. |
| `--buffer-size` | `5000` | Number of records kept in memory before writing one parquet file (also capped at 60s, see Resilience). |
| `--data-dir` | current directory | Directory `data/...` is created under. |

## Output layout

The directories are created at startup, so there is nothing to set up by
hand. Spot streams live under `data/raw`, futures streams under
`data/raw_futures`, keeping the two markets clearly apart:

```
data/
├── raw/
│   ├── XRPUSDT/
│   │   ├── trades/       trades_<ms>_<uuid>.parquet
│   │   ├── book_ticker/  book_ticker_<ms>_<uuid>.parquet
│   │   ├── depth/        depth_<ms>_<uuid>.parquet
│   │   ├── depth20/      depth20_<ms>_<uuid>.parquet
│   │   ├── snapshots/    snapshots_<ms>_<uuid>.parquet   (top-20 of the local book, 1/s)
│   │   └── events/       events_<ms>_<uuid>.parquet      (connects, gaps, resyncs, validation)
│   └── _GLOBAL/
│       ├── events/         (clock offset checks, shutdown/exit reason)
│       └── exchange_info/  (daily tick/lot size snapshot)
└── raw_futures/
    └── BTCUSDT/
        ├── depth/  agg_trades/  mark_price/  liquidations/  open_interest/
        ├── snapshots/
        └── events/
```

`<ms>` in the file name is the epoch millisecond of the flush.

> The path defaults to `Path.cwd()`; use `--data-dir` (or `make ... DATA_DIR=`)
> to point it somewhere else, e.g. a mounted data disk.

### Parquet schemas

**trades**

| Column | Source | Description |
| --- | --- | --- |
| `symbol` | `s` | Trading pair |
| `trade_id` | `t` | Binance trade id |
| `price` | `p` | Price (string, precision preserved) |
| `quantity` | `q` | Quantity |
| `event_time` | `E` | Binance event time (ms) |
| `trade_time` | `T` | Time the trade was executed (ms) |
| `buyer_is_maker` | `m` | `True` means the sell side was the aggressor |
| `local_receive_time` | — | Time the message was received locally (ms) |

**book_ticker**

| Column | Source |
| --- | --- |
| `best_bid_price` / `best_bid_quantity` | `b` / `B` |
| `best_ask_price` / `best_ask_quantity` | `a` / `A` |
| `update_id` | `u` |
| `local_receive_time` | — |

**depth**

| Column | Source | Description |
| --- | --- | --- |
| `symbol` | `s` | Trading pair |
| `event_time` | `E` | Event time (ms) |
| `first_update_id` | `U` | First id of the update range |
| `final_update_id` | `u` | Last id of the update range |
| `bids` / `asks` | `b` / `a` | `[[price, quantity], ...]` diff list |
| `local_receive_time` | — | Local receive time (ms) |

The difference between `local_receive_time` and `event_time` can be used to
measure network latency.

### Reading the data back

Each flush is a separate file, so a session is read by globbing the stream
directory:

```python
import glob

import pandas as pd

files = sorted(glob.glob("data/raw/XRPUSDT/trades/*.parquet"))
df = pd.concat(pd.read_parquet(f) for f in files)

df["price"] = df["price"].astype(float)
df["quantity"] = df["quantity"].astype(float)
df["trade_time"] = pd.to_datetime(df["trade_time"], unit="ms")

ohlcv = df.set_index("trade_time").resample("1s").agg(
    open=("price", "first"),
    high=("price", "max"),
    low=("price", "min"),
    close=("price", "last"),
    volume=("quantity", "sum"),
)
```

Files are named by flush time, so sorting them by name is chronological. In the
`depth` files, `bids` and `asks` are nested arrays of `[price, quantity]`
string pairs and need to be exploded before use.

## Order book synchronization

`DepthCollector` implements Binance's official local order book management
procedure:

1. Connect to the `@depth@100ms` stream and buffer incoming updates in
   `pending_updates`.
2. Fetch a REST snapshot
   (`data-api.binance.vision/api/v3/depth`, limit 5000).
3. Drop stale updates where `final_update_id <= lastUpdateId`.
4. Starting from the first update satisfying
   `first_update_id <= lastUpdateId + 1 <= final_update_id`, apply the buffered
   updates in order → the book becomes `is_synced`.
5. Every subsequent update is applied after a sequence continuity check. If a
   sequence gap is detected the book is invalidated (`invalidate_sync`) and
   resynchronized from step 2 — the websocket stays open, since a gap
   invalidates the book, not the connection. A one second pause before the new
   snapshot keeps repeated gaps from hammering the REST endpoint.

Raw diff updates keep being written to parquet regardless of sync state, so no
data is lost while recording.

Once synchronized, the live book can be inspected with `get_best_bid()`,
`get_best_ask()`, `get_top_bids()` / `get_top_asks()` and `get_spread()` —
backed by a `sortedcontainers.SortedDict` so these are O(log n) / O(k) instead
of scanning every price level.

**Futures uses a different continuity rule.** Binance's futures diff depth
events carry a `pu` field (the previous event's `u`) instead of chaining
`U`/`u` the way spot does, and the first synced event only needs
`U <= lastUpdateId <= u` (no `+1`). `DepthCollector(market="futures")`
switches to this rule automatically; mixing up the two causes the book to
falsely detect a gap and resync on almost every single update, which is
exactly what an earlier version of this recorder did.

If a snapshot turns out to be older than every buffered update — so no update
will ever straddle `lastUpdateId + 1` — the sync would otherwise wait forever.
`is_sync_stalled()` detects this after 30 seconds and triggers a fresh
snapshot fetch instead.

## Data quality: snapshots, events and cross-checks

- **Snapshots** (`snapshots/`): once a second, the top 20 levels of the
  synced local book are captured independently of the raw diff stream — a
  cheap, directly-readable reference for research.
- **Events** (`events/`, per symbol, plus a `_GLOBAL` stream): every connect,
  disconnect, resync, sequence gap and validation mismatch is recorded with a
  timestamp, so a `check_data_quality`-style script has structured data to
  read instead of grepping logs. Trade id gaps (spot `trade` and futures
  `futures_agg_trade`) are also detected and logged the instant they happen,
  since both ids are sequential. A `shutdown` event is logged on exit with
  the reason (`SIGTERM`, `SIGINT` or `task_exited_unexpectedly`), so a gap at
  the end of a file can be told apart from an actual outage.
- **Cross-validation** (spot only): `book_ticker` and `depth20` are separate
  connections, so at any given instant they are virtually never in sync with
  the locally reconstructed `depth` book — comparing them by wall-clock time
  would mostly flag normal timing skew as a "mismatch". Instead, each
  reference carries the order-book update id it is current as of, and it is
  only compared once that id matches `depth`'s `last_update_id` exactly, i.e.
  only when they are genuinely the same version of the book. Any mismatch at
  that point is logged as a `validation` event. `futures_depth` is **not**
  cross-checked against `book_ticker`/`depth20` — those are spot references,
  and would permanently "mismatch" a perpetual's price.
- **Clock offset**: every 5 minutes, the `_GLOBAL` events stream records the
  difference between Binance's server clock (`/api/v3/time`) and the local
  clock, so latency figures computed from `local_receive_time` can be
  corrected for drift. Keep the host's own clock disciplined with
  chrony/ntpd — this only measures what's left over.
- **Exchange metadata**: `exchange_info/` stores tick size, lot size and
  trading status for the tracked symbols once a day, so a filter change
  doesn't silently invalidate downstream analysis.

## Resilience

- Every symbol/stream pair runs in its own connection loop; if one drops the
  others are unaffected and the dropped one reconnects after a jittered
  exponential backoff (3s up to 60s). Each symbol keeps its own order book,
  buffers and output directory.
- Buffers are flushed by size (`--buffer-size`) **or** time — at least once a
  minute — so a quiet symbol/stream doesn't sit on unflushed data for hours.
- A buffer flush swaps the in-memory list out (`take_batch()`) and writes the
  swapped-out batch in a background thread (`asyncio.to_thread`), so it never
  blocks `ws.recv()` (keeping `local_receive_time` accurate) and can never be
  double-flushed if a task is cancelled mid-write.
- `Ctrl+C` **and** `SIGTERM` (what `systemctl stop`/Docker/`kill` send) both
  trigger a shutdown that flushes whatever is left in memory before exiting —
  important for running this as a service, where `SIGTERM` and not
  `KeyboardInterrupt` is what actually happens. Shutdown is ordered: producer
  tasks (streams) are cancelled and flushed first, *then* a `shutdown` event
  is logged, and only then are the event logger tasks cancelled and flushed.
  Doing it the other way around would let the event logger exit before a
  producer's final event (e.g. a gap noticed while it's being torn down) is
  logged, silently dropping it.
- Parquet files are written with `zstd` compression and tagged with the
  recorder's git commit hash in their schema metadata, so any file can be
  traced back to the exact code version that produced it.

## Project layout

```
main.py                              CLI, stream tasks, reconnect loops
collector/
├── base_collector.py                BaseCollector: buffer/flush/parquet, shared by everything below
├── version.py                       git_commit() helper, tagged onto every parquet file
├── trade_collector.py               TradeCollector (spot trades)
├── book_ticker_collector.py         BookTickerCollector
├── depth_collector.py               DepthCollector + local order book (spot & futures)
├── depth20_collector.py             Depth20Collector (Binance's own top-20 book)
├── futures_collectors.py            FuturesAggTrade/MarkPrice/Liquidation/OpenInterestCollector
├── snapshot_writer.py                SnapshotWriter (periodic top-N book capture)
├── event_logger.py                  EventLogger (connect/gap/resync/validation events)
├── exchange_info_recorder.py        ExchangeInfoRecorder (daily tick/lot size snapshot)
└── time_sync.py                     measure_clock_offset() against Binance's server time
tests/                                pytest suite (mainly DepthCollector's sync logic)
Makefile                             shortcuts for the commands above
requirements.txt
README.md
```

Every collector extends `BaseCollector`, which owns `data_path`,
`ensure_data_path()`, `add_to_buffer()`, `take_batch()`, `should_flush_by_time()`
and `flush_buffer()`. A subclass only needs to set `self.ws_url` and add a
`parse_*()` method. To add a new stream: write such a class and register a
runner for it in the `STREAM_RUNNERS` dictionary in `main.py`.

## Tests

```bash
make test
# or: pytest tests/ -v
```

`DepthCollector`'s synchronization logic (normal sync, sequence gaps, stale
snapshots, the spot vs. futures continuity rules, best bid/ask/top-N queries)
is pure Python with no network calls, so it is covered with synthetic update
sequences rather than live connections.

## Known limitations

- Every symbol/stream pair opens its own websocket connection, so the
  connection count is `symbols x streams` (printed at startup). That is fine
  for a handful of symbols; for dozens, combined streams
  (`/stream?streams=...`) would be the way to go.
- Each depth resync fetches a REST snapshot, which costs request weight
  against Binance's IP budget (250 at `limit=5000` for spot, less for
  futures) — worth keeping in mind when recording depth for many symbols at
  once.
- Prices and quantities are stored as strings (to avoid precision loss) and
  should be converted to `float`/`decimal` for analysis.
- `futures_open_interest` is polled over REST every 30 seconds rather than
  pushed, since Binance has no open-interest websocket stream.
- Validation (`book_ticker`/`depth20` cross-checks) only runs for the spot
  `depth` book, and only while `depth` is selected together with those
  streams for the same symbol; it logs mismatches as events but does not
  attempt to correct the local book itself. `futures_depth` has no
  equivalent reference stream to validate against.
- `futures_liquidation` (`<symbol>@forceOrder`) is a **sample, not an
  exhaustive record**: Binance pushes at most one forced liquidation order
  per symbol per second on this stream, so if several liquidations happen
  within the same second only the most recent is seen. Aggregate stats
  computed from it (e.g. "total liquidation volume") will undercount —
  treat it as an indicator of liquidation activity, not a full ledger.
