import argparse
import asyncio
import json
import logging
import random
import signal
import time
from pathlib import Path

import aiohttp
import websockets

from collector.book_ticker_collector import BookTickerCollector
from collector.depth_collector import DepthCollector
from collector.depth20_collector import Depth20Collector
from collector.event_logger import EventLogger
from collector.exchange_info_recorder import ExchangeInfoRecorder
from collector.futures_collectors import (
    FuturesAggTradeCollector,
    FuturesLiquidationCollector,
    FuturesMarkPriceCollector,
    FuturesOpenInterestCollector,
)
from collector.snapshot_writer import SnapshotWriter
from collector.time_sync import measure_clock_offset
from collector.trade_collector import TradeCollector

SPOT_STREAM_CHOICES = ("depth", "trade", "book_ticker", "depth20")
FUTURES_STREAM_CHOICES = (
    "futures_depth",
    "futures_agg_trade",
    "futures_mark_price",
    "futures_liquidation",
    "futures_open_interest",
)
STREAM_CHOICES = SPOT_STREAM_CHOICES + FUTURES_STREAM_CHOICES
DEFAULT_STREAMS = ("depth", "trade", "book_ticker")

RECEIVE_TIMEOUT_SECONDS = 1.0
FLUSH_INTERVAL_SECONDS = 60
EVENTS_FLUSH_INTERVAL_SECONDS = 10
BASE_BACKOFF_SECONDS = 3
MAX_BACKOFF_SECONDS = 60
SYNC_STALL_TIMEOUT_SECONDS = 30
VALIDATION_TOLERANCE = 1e-8
VALIDATION_STATS_INTERVAL_SECONDS = 60
OPEN_INTEREST_POLL_SECONDS = 30
TIME_SYNC_INTERVAL_SECONDS = 5 * 60
EXCHANGE_INFO_INTERVAL_SECONDS = 24 * 60 * 60

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger("recorder")


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
        default=list(DEFAULT_STREAMS),
        help=(
            "Streams to record. Spot: depth, trade, book_ticker, depth20. "
            "Futures: futures_depth, futures_agg_trade, futures_mark_price, "
            "futures_liquidation, futures_open_interest."
        ),
    )

    parser.add_argument(
        "--buffer-size",
        "-buffer-size",
        type=int,
        default=5000,
        help="Number of records kept in memory before writing a parquet file",
    )

    parser.add_argument(
        "--data-dir",
        "-data-dir",
        type=str,
        default=None,
        help="Directory to write data/... into (defaults to cwd)",
    )

    return parser.parse_args()


def prepare_data_path(collector, symbol: str, name: str):
    """
    Creates the directory a collector writes to at startup instead of
    waiting for the first parquet write.
    """

    collector.ensure_data_path()

    logger.info("[%s] %s output: %s", symbol.upper(), name, collector.data_path)


def schedule_flush(collector, pending_writes: list, symbol: str, name: str):
    """
    Swaps the buffer out and writes it in a background task instead of
    awaiting it inline, so ws.recv() on this stream is never blocked by a
    parquet write (and local_receive_time stays accurate).
    """

    batch = collector.take_batch()
    collector.last_flush_time = time.time()

    if not batch:
        return

    async def _write():
        await asyncio.to_thread(collector.flush_buffer, batch)
        logger.info("[%s] Wrote %d %s records.", symbol.upper(), len(batch), name)

    pending_writes.append(asyncio.create_task(_write()))
    pending_writes[:] = [t for t in pending_writes if not t.done()]


async def flush_remaining(collector, symbol: str, name: str):
    batch = collector.take_batch()

    if not batch:
        return

    await asyncio.to_thread(collector.flush_buffer, batch)

    logger.info("[%s] Flushed %d pending %s records.", symbol.upper(), len(batch), name)


async def drain_pending_writes(pending_writes: list):
    if pending_writes:
        await asyncio.gather(*pending_writes, return_exceptions=True)
        pending_writes.clear()


def backoff_delay(attempt: int) -> float:
    delay = min(MAX_BACKOFF_SECONDS, BASE_BACKOFF_SECONDS * (2 ** attempt))
    return delay * (0.5 + random.random())


def log_event(event_logger, stream: str, event_type: str, detail: str = ""):
    if event_logger is not None:
        event_logger.log(stream, event_type, detail)


async def run_stream_collector(
    collector, parse, symbol: str, name: str, event_logger=None
):
    """
    Listens to a single websocket stream (trade, bookTicker, ...), parses each
    incoming message into the buffer and flushes it to a parquet file once the
    buffer is full or the flush interval elapses.
    """

    prepare_data_path(collector, symbol, name)

    attempt = 0
    pending_writes = []

    try:
        while True:
            try:
                logger.info("[%s] Connecting to %s stream...", symbol.upper(), name)

                async with websockets.connect(collector.ws_url) as ws:
                    logger.info("[%s] %s stream connected.", symbol.upper(), name)
                    log_event(event_logger, name, "connected")
                    attempt = 0

                    while True:
                        try:
                            message = await asyncio.wait_for(
                                ws.recv(), timeout=RECEIVE_TIMEOUT_SECONDS
                            )
                        except asyncio.TimeoutError:
                            if collector.should_flush_by_time(FLUSH_INTERVAL_SECONDS):
                                schedule_flush(collector, pending_writes, symbol, name)
                            continue

                        data = json.loads(message)
                        record = parse(data)

                        if (
                            collector.add_to_buffer(record)
                            or collector.should_flush_by_time(FLUSH_INTERVAL_SECONDS)
                        ):
                            schedule_flush(collector, pending_writes, symbol, name)

            except asyncio.CancelledError:
                raise

            except Exception as exc:
                logger.warning(
                    "[%s] %s connection error: %s: %s",
                    symbol.upper(), name, type(exc).__name__, exc,
                )
                log_event(
                    event_logger, name, "connection_error",
                    f"{type(exc).__name__}: {exc}",
                )

            attempt += 1
            delay = backoff_delay(attempt)
            logger.info(
                "[%s] Reconnecting %s stream in %.1fs...", symbol.upper(), name, delay
            )
            await asyncio.sleep(delay)

    except asyncio.CancelledError:
        await drain_pending_writes(pending_writes)
        await flush_remaining(collector, symbol, name)
        raise

    finally:
        await drain_pending_writes(pending_writes)


async def run_trade_collector(
    symbol: str, buffer_size: int, data_dir: Path, event_logger=None, refs=None
):
    collector = TradeCollector(symbol=symbol, buffer_size=buffer_size, data_dir=data_dir)

    def parse_and_check(data: dict) -> dict:
        record = collector.parse_trade(data)
        gap = collector.check_gap(record["trade_id"])

        if gap:
            log_event(
                event_logger, "trade", "trade_id_gap",
                f"missing {gap} trade(s) before id={record['trade_id']}",
            )

        return record

    await run_stream_collector(collector, parse_and_check, symbol, "trade", event_logger)


async def run_book_ticker_collector(
    symbol: str, buffer_size: int, data_dir: Path, event_logger=None, refs=None
):
    collector = BookTickerCollector(symbol=symbol, buffer_size=buffer_size, data_dir=data_dir)

    if refs is not None:
        refs["book_ticker"] = collector

    await run_stream_collector(
        collector, collector.parse_book_ticker, symbol, "book_ticker", event_logger
    )


async def run_depth20_collector(
    symbol: str, buffer_size: int, data_dir: Path, event_logger=None, refs=None
):
    collector = Depth20Collector(symbol=symbol, buffer_size=buffer_size, data_dir=data_dir)

    if refs is not None:
        refs["depth20"] = collector

    await run_stream_collector(
        collector, collector.parse_depth20, symbol, "depth20", event_logger
    )


async def run_futures_agg_trade_collector(
    symbol: str, buffer_size: int, data_dir: Path, event_logger=None, refs=None
):
    collector = FuturesAggTradeCollector(
        symbol=symbol, buffer_size=buffer_size, data_dir=data_dir
    )

    def parse_and_check(data: dict) -> dict:
        record = collector.parse_agg_trade(data)
        gap = collector.check_gap(record["agg_trade_id"])

        if gap:
            log_event(
                event_logger, "futures_agg_trade", "agg_trade_id_gap",
                f"missing {gap} agg trade(s) before id={record['agg_trade_id']}",
            )

        return record

    await run_stream_collector(
        collector, parse_and_check, symbol, "futures_agg_trade", event_logger
    )


async def run_futures_mark_price_collector(
    symbol: str, buffer_size: int, data_dir: Path, event_logger=None, refs=None
):
    collector = FuturesMarkPriceCollector(
        symbol=symbol, buffer_size=buffer_size, data_dir=data_dir
    )

    await run_stream_collector(
        collector, collector.parse_mark_price, symbol, "futures_mark_price", event_logger
    )


async def run_futures_liquidation_collector(
    symbol: str, buffer_size: int, data_dir: Path, event_logger=None, refs=None
):
    collector = FuturesLiquidationCollector(
        symbol=symbol, buffer_size=buffer_size, data_dir=data_dir
    )

    await run_stream_collector(
        collector, collector.parse_liquidation, symbol, "futures_liquidation", event_logger
    )


async def run_futures_open_interest_collector(
    symbol: str, buffer_size: int, data_dir: Path, event_logger=None, refs=None
):
    """
    Open interest has no push stream, so it is polled over REST on a fixed
    interval instead of following the websocket reconnect loop.
    """

    collector = FuturesOpenInterestCollector(
        symbol=symbol, buffer_size=buffer_size, data_dir=data_dir
    )

    prepare_data_path(collector, symbol, "futures_open_interest")

    attempt = 0

    try:
        async with aiohttp.ClientSession() as session:
            while True:
                try:
                    record = await collector.fetch(session)

                    if (
                        collector.add_to_buffer(record)
                        or collector.should_flush_by_time(FLUSH_INTERVAL_SECONDS)
                    ):
                        await asyncio.to_thread(collector.flush_buffer)

                    attempt = 0
                    await asyncio.sleep(OPEN_INTEREST_POLL_SECONDS)

                except asyncio.CancelledError:
                    raise

                except Exception as exc:
                    attempt += 1
                    logger.warning(
                        "[%s] futures_open_interest poll error: %s: %s",
                        symbol.upper(), type(exc).__name__, exc,
                    )
                    log_event(
                        event_logger, "futures_open_interest", "poll_error",
                        f"{type(exc).__name__}: {exc}",
                    )
                    await asyncio.sleep(backoff_delay(attempt))

    except asyncio.CancelledError:
        await flush_remaining(collector, symbol, "futures_open_interest")
        raise


def new_validation_stats() -> dict:
    return {
        "book_ticker_checks": 0,
        "book_ticker_found": 0,
        "book_ticker_mismatches": 0,
        "depth20_checks": 0,
        "depth20_found": 0,
        "depth20_mismatches": 0,
    }


def validate_local_book(depth_collector, refs, event_logger, stats: dict):
    """
    Cross-checks the locally reconstructed **spot** order book against
    independent spot references (bookTicker's best price, Binance's own
    depth20 snapshot) and logs a validation event on any mismatch beyond
    floating point noise.

    Only called for the spot depth collector: bookTicker/depth20 are spot
    streams, so comparing them against the futures book would compare two
    different instruments and produce a constant, meaningless "mismatch".

    book_ticker/depth20 arrive over separate connections from the depth diff
    stream, so by the time a depth update is processed here, ".latest" on
    either of them has usually already moved past this exact update id --
    comparing only against ".latest" would make genuine matches rare (worst
    on the busiest symbols, where validation matters most), and "0
    mismatches" would then quietly mean "0 checks" rather than "book
    confirmed correct". Instead each reference keeps a short history keyed
    by update id, and this looks up the id that matches the local book's
    current last_update_id in that history, i.e. it finds the genuinely
    same version of the book regardless of which one arrived "first".

    `stats` is mutated with attempt/found/mismatch counters so a caller can
    periodically report how much of this is actually landing a comparison
    (see run_depth_collector's validation_stats event) instead of only
    seeing silence and assuming everything's fine.
    """

    if event_logger is None or refs is None:
        return

    local_bid = depth_collector.get_best_bid()
    local_ask = depth_collector.get_best_ask()
    local_update_id = depth_collector.last_update_id

    book_ticker = refs.get("book_ticker")
    stats["book_ticker_checks"] += 1

    ref = book_ticker.history.get(local_update_id) if book_ticker is not None else None

    if ref is not None:
        stats["book_ticker_found"] += 1
        mismatched = False

        if local_bid is not None:
            diff = float(local_bid[0]) - float(ref["best_bid_price"])
            if abs(diff) > VALIDATION_TOLERANCE:
                mismatched = True
                log_event(
                    event_logger, "validation", "best_bid_mismatch",
                    f"update_id={local_update_id} local={local_bid[0]} "
                    f"book_ticker={ref['best_bid_price']}",
                )

        if local_ask is not None:
            diff = float(local_ask[0]) - float(ref["best_ask_price"])
            if abs(diff) > VALIDATION_TOLERANCE:
                mismatched = True
                log_event(
                    event_logger, "validation", "best_ask_mismatch",
                    f"update_id={local_update_id} local={local_ask[0]} "
                    f"book_ticker={ref['best_ask_price']}",
                )

        if mismatched:
            stats["book_ticker_mismatches"] += 1

    depth20 = refs.get("depth20")
    stats["depth20_checks"] += 1

    ref = depth20.history.get(local_update_id) if depth20 is not None else None

    if ref is not None:
        stats["depth20_found"] += 1
        mismatched = False

        ref_bid = Depth20Collector.best_bid_of(ref)
        ref_ask = Depth20Collector.best_ask_of(ref)

        if local_bid is not None and ref_bid is not None:
            if abs(float(local_bid[0]) - float(ref_bid[0])) > VALIDATION_TOLERANCE:
                mismatched = True
                log_event(
                    event_logger, "validation", "depth20_bid_mismatch",
                    f"update_id={local_update_id} local={local_bid[0]} depth20={ref_bid[0]}",
                )

        if local_ask is not None and ref_ask is not None:
            if abs(float(local_ask[0]) - float(ref_ask[0])) > VALIDATION_TOLERANCE:
                mismatched = True
                log_event(
                    event_logger, "validation", "depth20_ask_mismatch",
                    f"update_id={local_update_id} local={local_ask[0]} depth20={ref_ask[0]}",
                )

        if mismatched:
            stats["depth20_mismatches"] += 1


def maybe_log_validation_stats(stats: dict, last_flush: list, event_logger):
    """
    Reports how much of validate_local_book's checking is actually landing
    a comparison, once a minute. Without this, "0 mismatch" events in the
    log are indistinguishable from "validation never found a matching id
    and effectively never ran".
    """

    now = time.time()

    if now - last_flush[0] < VALIDATION_STATS_INTERVAL_SECONDS:
        return

    last_flush[0] = now

    log_event(
        event_logger, "validation", "validation_stats",
        f"book_ticker: {stats['book_ticker_found']}/{stats['book_ticker_checks']} "
        f"matched, {stats['book_ticker_mismatches']} mismatches | "
        f"depth20: {stats['depth20_found']}/{stats['depth20_checks']} matched, "
        f"{stats['depth20_mismatches']} mismatches",
    )

    stats.update(new_validation_stats())


async def run_depth_collector(
    symbol: str,
    buffer_size: int,
    data_dir: Path,
    event_logger=None,
    refs=None,
    market: str = "spot",
):
    stream_label = "depth" if market == "spot" else "futures_depth"

    collector = DepthCollector(
        symbol=symbol, buffer_size=buffer_size, data_dir=data_dir, market=market
    )

    if refs is not None:
        # Keyed by stream_label (not a shared "depth" key) so the futures
        # book never gets cross-checked against spot's bookTicker/depth20 --
        # they are different instruments and would "mismatch" permanently.
        refs[stream_label] = collector

    snapshot_writer = SnapshotWriter(
        symbol=symbol, data_dir=data_dir, market=market
    )
    prepare_data_path(snapshot_writer, symbol, f"{stream_label}_snapshots")

    prepare_data_path(collector, symbol, stream_label)

    attempt = 0
    pending_writes = []
    validation_stats = new_validation_stats()
    validation_stats_last_flush = [time.time()]

    def maybe_flush_depth():
        if collector.should_flush_by_time(FLUSH_INTERVAL_SECONDS):
            schedule_flush(collector, pending_writes, symbol, stream_label)

    def maybe_flush_snapshot():
        if (
            len(snapshot_writer.buffer) >= snapshot_writer.buffer_size
            or snapshot_writer.should_flush_by_time(FLUSH_INTERVAL_SECONDS)
        ):
            schedule_flush(
                snapshot_writer, pending_writes, symbol, f"{stream_label}_snapshots"
            )

    try:
        while True:
            try:
                logger.info("[%s] Connecting to %s stream...", symbol.upper(), stream_label)

                async with aiohttp.ClientSession() as session:
                    async with websockets.connect(collector.ws_url) as ws:
                        logger.info("[%s] WebSocket connected.", symbol.upper())
                        log_event(event_logger, stream_label, "connected")
                        attempt = 0

                        resyncing = False

                        # Resync loop. A sequence gap only invalidates the local
                        # book, not the connection, so it is enough to pull a
                        # fresh snapshot here and keep the same websocket.
                        while True:
                            if resyncing:
                                await asyncio.sleep(1)

                            snapshot = await collector.fetch_snapshot(session)

                            collector.load_snapshot(snapshot)

                            logger.info(
                                "[%s] Snapshot loaded | lastUpdateId=%s | bids=%d | asks=%d",
                                symbol.upper(),
                                collector.last_update_id,
                                len(collector.local_bids),
                                len(collector.local_asks),
                            )

                            sync_ok = True

                            while not collector.is_synced:
                                try:
                                    message = await asyncio.wait_for(
                                        ws.recv(), timeout=RECEIVE_TIMEOUT_SECONDS
                                    )
                                except asyncio.TimeoutError:
                                    if collector.is_sync_stalled(
                                        SYNC_STALL_TIMEOUT_SECONDS
                                    ):
                                        logger.warning(
                                            "[%s] Snapshot too stale to sync after "
                                            "%.0fs, refetching...",
                                            symbol.upper(), SYNC_STALL_TIMEOUT_SECONDS,
                                        )
                                        log_event(
                                            event_logger, stream_label, "stale_snapshot",
                                            f"timeout={SYNC_STALL_TIMEOUT_SECONDS}s",
                                        )
                                        sync_ok = False
                                        break
                                    continue

                                data = json.loads(message)
                                depth = collector.parse_depth(data)

                                collector.buffer_pending_update(depth)

                                if collector.add_to_buffer(depth):
                                    schedule_flush(
                                        collector, pending_writes, symbol, stream_label
                                    )
                                else:
                                    maybe_flush_depth()

                                if collector.sync_pending_updates():
                                    logger.info(
                                        "[%s] Order book synchronized.", symbol.upper()
                                    )
                                    log_event(event_logger, stream_label, "synced")
                                    logger.info(
                                        "[%s] Best bid: %s | Best ask: %s | Spread: %s",
                                        symbol.upper(),
                                        collector.get_best_bid(),
                                        collector.get_best_ask(),
                                        collector.get_spread(),
                                    )

                            if not sync_ok:
                                resyncing = True
                                continue

                            while True:
                                try:
                                    message = await asyncio.wait_for(
                                        ws.recv(), timeout=RECEIVE_TIMEOUT_SECONDS
                                    )
                                except asyncio.TimeoutError:
                                    maybe_flush_depth()
                                    continue

                                data = json.loads(message)
                                depth = collector.parse_depth(data)

                                if collector.add_to_buffer(depth):
                                    schedule_flush(
                                        collector, pending_writes, symbol, stream_label
                                    )
                                else:
                                    maybe_flush_depth()

                                success = collector.process_live_update(depth)

                                if not success:
                                    logger.warning(
                                        "[%s] Order book sync lost. Resyncing "
                                        "(keeping the connection)...",
                                        symbol.upper(),
                                    )
                                    log_event(event_logger, stream_label, "sequence_gap")
                                    break

                                if snapshot_writer.should_capture():
                                    snapshot_writer.capture(collector)
                                    maybe_flush_snapshot()

                                if market == "spot":
                                    validate_local_book(
                                        collector, refs, event_logger, validation_stats
                                    )
                                    maybe_log_validation_stats(
                                        validation_stats,
                                        validation_stats_last_flush,
                                        event_logger,
                                    )

                            resyncing = True

            except asyncio.CancelledError:
                raise

            except Exception as exc:
                logger.warning(
                    "[%s] Connection error: %s: %s",
                    symbol.upper(), type(exc).__name__, exc,
                )
                log_event(
                    event_logger, stream_label, "connection_error",
                    f"{type(exc).__name__}: {exc}",
                )

            attempt += 1
            delay = backoff_delay(attempt)
            logger.info("[%s] Reconnecting in %.1fs...", symbol.upper(), delay)
            await asyncio.sleep(delay)

    except asyncio.CancelledError:
        await drain_pending_writes(pending_writes)
        await flush_remaining(collector, symbol, stream_label)
        await flush_remaining(snapshot_writer, symbol, f"{stream_label}_snapshots")
        raise

    finally:
        await drain_pending_writes(pending_writes)


async def run_futures_depth_collector(
    symbol: str, buffer_size: int, data_dir: Path, event_logger=None, refs=None
):
    await run_depth_collector(
        symbol, buffer_size, data_dir, event_logger, refs, market="futures"
    )


async def run_event_logger_task(event_logger: EventLogger):
    """
    Periodically flushes an EventLogger. The buffer swap (take_batch) happens
    on the event loop, same as schedule_flush() for the regular streams, so a
    log() call from another task can never land in between the swap and the
    write and get silently dropped.
    """

    try:
        while True:
            await asyncio.sleep(EVENTS_FLUSH_INTERVAL_SECONDS)

            if event_logger.buffer:
                batch = event_logger.take_batch()
                await asyncio.to_thread(event_logger.flush_buffer, batch)

    except asyncio.CancelledError:
        batch = event_logger.take_batch()
        await asyncio.to_thread(event_logger.flush_buffer, batch)
        raise


async def run_time_sync_task(event_logger: EventLogger):
    """
    Periodically compares Binance's server clock to the local clock and
    records the offset, so latency figures computed from local_receive_time
    can be corrected for clock drift later.
    """

    try:
        async with aiohttp.ClientSession() as session:
            while True:
                try:
                    offset_ms = await measure_clock_offset(session)
                    log_event(
                        event_logger, "time_sync", "clock_offset_ms", str(offset_ms)
                    )
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    logger.warning(
                        "Time sync check failed: %s: %s", type(exc).__name__, exc
                    )

                await asyncio.sleep(TIME_SYNC_INTERVAL_SECONDS)

    except asyncio.CancelledError:
        raise


async def run_exchange_info_task(symbols: list, data_dir: Path):
    recorder = ExchangeInfoRecorder(symbols=symbols, data_dir=data_dir)
    prepare_data_path(recorder, "_GLOBAL", "exchange_info")

    try:
        async with aiohttp.ClientSession() as session:
            while True:
                try:
                    await recorder.fetch_and_flush(session)
                    logger.info(
                        "[_GLOBAL] Recorded exchangeInfo for %s", ", ".join(symbols)
                    )
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    logger.warning(
                        "exchangeInfo fetch failed: %s: %s", type(exc).__name__, exc
                    )

                await asyncio.sleep(EXCHANGE_INFO_INTERVAL_SECONDS)

    except asyncio.CancelledError:
        raise


STREAM_RUNNERS = {
    "depth": run_depth_collector,
    "trade": run_trade_collector,
    "book_ticker": run_book_ticker_collector,
    "depth20": run_depth20_collector,
    "futures_depth": run_futures_depth_collector,
    "futures_agg_trade": run_futures_agg_trade_collector,
    "futures_mark_price": run_futures_mark_price_collector,
    "futures_liquidation": run_futures_liquidation_collector,
    "futures_open_interest": run_futures_open_interest_collector,
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
    data_dir = Path(args.data_dir) if args.data_dir else Path.cwd()

    streams = [
        stream
        for stream in STREAM_CHOICES
        if stream in args.streams
    ]

    logger.info("=" * 60)
    logger.info("Crypto Market Data Recorder")
    logger.info("Symbols: %s", ", ".join(s.upper() for s in symbols))
    logger.info("Streams: %s", ", ".join(streams))
    logger.info("Buffer size: %d", args.buffer_size)
    logger.info("Data dir: %s", data_dir)
    logger.info("Connections: %d", len(symbols) * len(streams))
    logger.info("Press Ctrl+C to stop")
    logger.info("=" * 60)

    event_loggers = {
        symbol: EventLogger(symbol=symbol, data_dir=data_dir) for symbol in symbols
    }
    # Same "_GLOBAL" pseudo-symbol as ExchangeInfoRecorder, so all
    # cross-symbol data lands under one consistently named folder.
    global_event_logger = EventLogger(symbol="_GLOBAL", data_dir=data_dir)
    all_event_loggers = [*event_loggers.values(), global_event_logger]

    refs = {symbol: {} for symbol in symbols}

    # Producers: everything that can call log_event(). Shut these down (and
    # let them flush their own data) *before* touching the event logger
    # tasks below, otherwise a producer's final event -- e.g. a gap detected
    # while it's being cancelled -- could be logged after its event logger
    # already flushed and exited, and be lost.
    producer_tasks = [
        asyncio.create_task(
            STREAM_RUNNERS[stream](
                symbol,
                args.buffer_size,
                data_dir,
                event_loggers[symbol],
                refs[symbol],
            ),
            name=f"{symbol}:{stream}",
        )
        for symbol in symbols
        for stream in streams
    ]
    producer_tasks.append(
        asyncio.create_task(run_time_sync_task(global_event_logger), name="global:time_sync")
    )
    producer_tasks.append(
        asyncio.create_task(
            run_exchange_info_task(symbols, data_dir), name="global:exchange_info"
        )
    )

    event_logger_tasks = [
        asyncio.create_task(
            run_event_logger_task(event_loggers[symbol]), name=f"{symbol}:events"
        )
        for symbol in symbols
    ]
    event_logger_tasks.append(
        asyncio.create_task(run_event_logger_task(global_event_logger), name="global:events")
    )

    all_tasks = producer_tasks + event_logger_tasks

    loop = asyncio.get_running_loop()
    stop_event = asyncio.Event()
    stop_reason = "unknown"

    def _request_stop(sig_name: str):
        nonlocal stop_reason
        stop_reason = sig_name
        logger.info("Received %s, shutting down...", sig_name)
        stop_event.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _request_stop, sig.name)
        except NotImplementedError:
            # Signal handlers aren't available on some platforms (e.g. Windows).
            pass

    stopper = asyncio.create_task(stop_event.wait())

    try:
        done, _ = await asyncio.wait(
            [stopper, *all_tasks], return_when=asyncio.FIRST_COMPLETED
        )

        if stopper not in done:
            # A task finished (crashed or returned) on its own, not via a
            # signal -- record which one and why. With Restart=always under
            # systemd this is otherwise very hard to diagnose after the
            # fact, since only the shutdown event survives the restart.
            details = []

            for task in done:
                if task is stopper:
                    continue

                exc = task.exception() if not task.cancelled() else None

                if exc is not None:
                    details.append(
                        f"{task.get_name()}: {type(exc).__name__}: {exc}"
                    )
                else:
                    details.append(f"{task.get_name()}: exited without error")

            stop_reason = "task_exited_unexpectedly (" + "; ".join(details) + ")"

    finally:
        stopper.cancel()

        for task in producer_tasks:
            if not task.done():
                task.cancel()
        await asyncio.gather(*producer_tasks, return_exceptions=True)

        for logger_ in all_event_loggers:
            log_event(logger_, "recorder", "shutdown", stop_reason)

        for task in event_logger_tasks:
            if not task.done():
                task.cancel()
        await asyncio.gather(*event_logger_tasks, stopper, return_exceptions=True)


if __name__ == "__main__":
    asyncio.run(main())
