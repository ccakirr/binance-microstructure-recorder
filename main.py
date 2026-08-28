import argparse
import asyncio
import json

import aiohttp
import websockets

from collector.book_ticker_collector import BookTickerCollector
from collector.depth_collector import DepthCollector
from collector.trade_collector import TradeCollector

STREAM_CHOICES = ("depth", "trade", "book_ticker")


def parse_args():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--symbol",
        "--symbols",
        "-symbol",
        type=str,
        nargs="+",
        default=["xrpusdt"],
        help=(
            "One or more Binance trading pairs, "
            "e.g. --symbol btcusdt ethusdt"
        ),
    )

    parser.add_argument(
        "--streams",
        "-streams",
        type=str,
        nargs="+",
        choices=STREAM_CHOICES,
        default=list(STREAM_CHOICES),
        help="Streams to record, e.g. --streams depth trade",
    )

    parser.add_argument(
        "--buffer-size",
        "-buffer-size",
        type=int,
        default=5000,
        help="Number of records kept in memory before writing a parquet file",
    )

    return parser.parse_args()


def prepare_data_path(collector, symbol: str, name: str):
    """
    Creates the data/raw/<SYMBOL>/<stream> directory the collector writes to,
    at startup instead of waiting for the first parquet write.
    """

    collector.ensure_data_path()

    print(
        f"[{symbol.upper()}] {name} output: {collector.data_path}"
    )


def flush_remaining(collector, symbol: str, name: str):
    pending = len(collector.buffer)

    if pending == 0:
        return

    collector.flush_buffer()

    print(
        f"[{symbol.upper()}] Flushed {pending} pending {name} records."
    )


async def run_stream_collector(collector, parse, symbol: str, name: str):
    """
    Listens to a single websocket stream (trade, bookTicker, ...), parses each
    incoming message into the buffer and flushes it to a parquet file once the
    buffer is full.
    """

    prepare_data_path(collector, symbol, name)

    while True:
        try:
            print(f"[{symbol.upper()}] Connecting to {name} stream...")

            async with websockets.connect(collector.ws_url) as ws:
                print(f"[{symbol.upper()}] {name} stream connected.")

                while True:
                    message = await ws.recv()
                    data = json.loads(message)

                    record = parse(data)

                    if collector.add_to_buffer(record):
                        flushed = len(collector.buffer)

                        # Parquet writing is synchronous CPU + disk work.
                        # Running it in a thread keeps the event loop free,
                        # so a flush here does not delay ws.recv() (and thus
                        # local_receive_time) on every other stream.
                        await asyncio.to_thread(collector.flush_buffer)

                        print(
                            f"[{symbol.upper()}] "
                            f"Wrote {flushed} {name} records."
                        )

        except asyncio.CancelledError:
            flush_remaining(collector, symbol, name)
            raise

        except KeyboardInterrupt:
            flush_remaining(collector, symbol, name)
            raise

        except Exception as exc:
            print(
                f"[{symbol.upper()}] {name} connection error: "
                f"{type(exc).__name__}: {exc}"
            )

        flush_remaining(collector, symbol, name)

        print(
            f"[{symbol.upper()}] Reconnecting {name} stream in 3 seconds..."
        )

        await asyncio.sleep(3)


async def run_trade_collector(symbol: str, buffer_size: int):
    collector = TradeCollector(symbol=symbol, buffer_size=buffer_size)

    await run_stream_collector(
        collector,
        collector.parse_trade,
        symbol,
        "trade",
    )


async def run_book_ticker_collector(symbol: str, buffer_size: int):
    collector = BookTickerCollector(symbol=symbol, buffer_size=buffer_size)

    await run_stream_collector(
        collector,
        collector.parse_book_ticker,
        symbol,
        "book_ticker",
    )


async def run_depth_collector(symbol: str, buffer_size: int):
    collector = DepthCollector(symbol=symbol, buffer_size=buffer_size)

    prepare_data_path(collector, symbol, "depth")

    while True:
        try:
            print(f"[{symbol.upper()}] Connecting to depth stream...")

            async with aiohttp.ClientSession() as session:
                async with websockets.connect(collector.ws_url) as ws:
                    print(f"[{symbol.upper()}] WebSocket connected.")

                    resyncing = False

                    # Resync loop. A sequence gap only invalidates the local
                    # book, not the connection, so it is enough to pull a
                    # fresh snapshot here and keep the same websocket.
                    while True:
                        if resyncing:
                            await asyncio.sleep(1)

                        snapshot = await collector.fetch_snapshot(session)

                        collector.load_snapshot(snapshot)

                        print(
                            f"[{symbol.upper()}] Snapshot loaded | "
                            f"lastUpdateId={collector.last_update_id} | "
                            f"bids={len(collector.local_bids)} | "
                            f"asks={len(collector.local_asks)}"
                        )

                        while not collector.is_synced:
                            message = await ws.recv()
                            data = json.loads(message)

                            depth = collector.parse_depth(data)

                            collector.buffer_pending_update(depth)

                            if collector.add_to_buffer(depth):
                                await asyncio.to_thread(collector.flush_buffer)

                            if collector.sync_pending_updates():
                                print(
                                    f"[{symbol.upper()}] "
                                    f"Order book synchronized."
                                )

                                print(
                                    "Best bid:",
                                    collector.get_best_bid(),
                                )

                                print(
                                    "Best ask:",
                                    collector.get_best_ask(),
                                )

                                print(
                                    "Spread:",
                                    collector.get_spread(),
                                )

                        while True:
                            message = await ws.recv()
                            data = json.loads(message)

                            depth = collector.parse_depth(data)

                            if collector.add_to_buffer(depth):
                                await asyncio.to_thread(collector.flush_buffer)

                            success = collector.process_live_update(depth)

                            if not success:
                                print(
                                    f"[{symbol.upper()}] "
                                    f"Order book sync lost. Resyncing "
                                    f"(keeping the connection)..."
                                )

                                break

                        resyncing = True

        except asyncio.CancelledError:
            flush_remaining(collector, symbol, "depth")
            raise

        except KeyboardInterrupt:
            flush_remaining(collector, symbol, "depth")
            raise

        except Exception as exc:
            print(
                f"[{symbol.upper()}] Connection error: "
                f"{type(exc).__name__}: {exc}"
            )

        flush_remaining(collector, symbol, "depth")

        print(
            f"[{symbol.upper()}] Reconnecting in 3 seconds..."
        )

        await asyncio.sleep(3)


STREAM_RUNNERS = {
    "depth": run_depth_collector,
    "trade": run_trade_collector,
    "book_ticker": run_book_ticker_collector,
}


def unique_symbols(raw_symbols: list) -> list:
    """
    Lowercases the given symbols and drops duplicates, keeping the order
    they were passed in.
    """

    symbols = []

    for symbol in raw_symbols:
        symbol = symbol.lower()

        if symbol not in symbols:
            symbols.append(symbol)

    return symbols


async def main():
    args = parse_args()

    symbols = unique_symbols(args.symbol)

    streams = [
        stream
        for stream in STREAM_CHOICES
        if stream in args.streams
    ]

    print("=" * 60)
    print("Crypto Market Data Recorder")
    print(f"Symbols: {', '.join(s.upper() for s in symbols)}")
    print(f"Streams: {', '.join(streams)}")
    print(f"Buffer size: {args.buffer_size}")
    print(f"Connections: {len(symbols) * len(streams)}")
    print("Press Ctrl+C to stop")
    print("=" * 60)

    tasks = [
        asyncio.create_task(
            STREAM_RUNNERS[stream](symbol, args.buffer_size),
            name=f"{symbol}:{stream}",
        )
        for symbol in symbols
        for stream in streams
    ]

    try:
        await asyncio.gather(*tasks)

    finally:
        for task in tasks:
            if not task.done():
                task.cancel()

        await asyncio.gather(*tasks, return_exceptions=True)


if __name__ == "__main__":
    try:
        asyncio.run(main())

    except KeyboardInterrupt:
        print("\nStopping recorder...")
