# Crypto Market Data Recorder

An asynchronous recorder that listens to live Binance spot market data over
websockets and stores it on disk as **parquet**.

Three streams are collected concurrently and independently of each other,
for one or more symbols at a time:

| Stream | Binance channel | What it records |
| --- | --- | --- |
| `trade` | `<symbol>@trade` | Every executed trade (tick data) |
| `book_ticker` | `<symbol>@bookTicker` | Best bid/ask price and quantity, on every change |
| `depth` | `<symbol>@depth@100ms` | Order book diff updates (100 ms) + local order book |

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
`pyarrow`, `aiohttp`.

## Usage

```bash
make run                             # all three streams, default symbol (XRPUSDT)
make run btcusdt                     # different symbol
make run btcusdt ethusdt bnbusdt     # several symbols at once
make run btcusdt STREAMS="trade depth"   # only selected streams
make run btcusdt BUFFER=1000         # more frequent parquet writes
make trades ethusdt                  # a single stream
```

Symbols can be written bare after the target, as above, or passed as a
variable — `make run SYMBOL="btcusdt ethusdt"`. Both forms end up as
`--symbol btcusdt ethusdt`.

Stop with `Ctrl+C`. Records still sitting in the buffer are written to disk on
shutdown, so a partially filled buffer is not lost.

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
| `--streams` | `depth trade book_ticker` | Streams to record. One or several. |
| `--buffer-size` | `5000` | Number of records kept in memory before writing one parquet file. |

## Output layout

The directories (including `data/raw`) are created at startup, so there is
nothing to set up by hand:

```
data/
└── raw/
    ├── XRPUSDT/
    │   ├── trades/       trades_<ms>.parquet
    │   ├── book_ticker/  book_ticker_<ms>.parquet
    │   └── depth/        depth_<ms>.parquet
    └── BTCUSDT/
        └── ...
```

`<ms>` in the file name is the epoch millisecond of the flush.

> The path is based on `Path.cwd()`, so `data/` is created **in the directory
> you run the command from**. Running from the project root is cleanest.

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
`get_best_ask()` and `get_spread()`.

## Resilience

- Every symbol/stream pair runs in its own connection loop; if one drops the
  others are unaffected and the dropped one reconnects after 3 seconds. Each
  symbol keeps its own order book, buffers and output directory.
- The buffer is flushed to disk before reconnecting and on shutdown.
- Parquet writes run in a worker thread (`asyncio.to_thread`), so a flush on
  one stream does not stall `ws.recv()` — and therefore `local_receive_time` —
  on the others. The shutdown flush stays synchronous on purpose, so it cannot
  be interrupted by the cancellation that triggered it.
- `Ctrl+C` cancels all tasks cleanly.

## Project layout

```
main.py                              CLI, stream tasks, reconnect loops
collector/
├── trade_collector.py               TradeCollector
├── book_ticker_collector.py         BookTickerCollector
└── depth_collector.py               DepthCollector + local order book
Makefile                             shortcuts for the commands above
requirements.txt
README.md
```

Every collector shares the same interface: `ws_url`, `data_path`,
`ensure_data_path()`, `parse_*()`, `add_to_buffer()`, `flush_buffer()`.
To add a new stream, write a class with that interface and register it in the
`STREAM_RUNNERS` dictionary in `main.py`.

## Known limitations

- File names are based on a millisecond timestamp; two flushes within the same
  millisecond would overwrite each other. This does not happen in practice at
  the default buffer size, but a counter should be added to the name if the
  buffer is made very small.
- Every symbol/stream pair opens its own websocket connection, so the
  connection count is `symbols x streams` (printed at startup). That is fine
  for a handful of symbols; for dozens, combined streams
  (`/stream?streams=...`) would be the way to go.
- Each depth resync fetches a REST snapshot, which costs 250 request weight at
  `limit=5000` against Binance's 6000/minute IP budget — worth keeping in mind
  when recording depth for many symbols at once.
- Prices and quantities are stored as strings (to avoid precision loss) and
  should be converted to `float`/`decimal` for analysis.
