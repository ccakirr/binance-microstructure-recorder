import tempfile
from pathlib import Path

from collector.depth20_collector import Depth20Collector
from collector.futures_collectors import (
    FuturesAggTradeCollector,
    FuturesLiquidationCollector,
    FuturesMarkPriceCollector,
)
from collector.snapshot_writer import SnapshotWriter
from collector.trade_collector import TradeCollector


def test_depth20_parses_and_exposes_best_prices():
    with tempfile.TemporaryDirectory() as d:
        collector = Depth20Collector(symbol="btcusdt", data_dir=Path(d))

        record = collector.parse_depth20(
            {
                "lastUpdateId": 42,
                "bids": [["100.00", "1"], ["99.50", "2"]],
                "asks": [["100.10", "1"], ["100.20", "2"]],
            }
        )

        assert record["last_update_id"] == 42
        assert collector.get_best_bid() == ("100.00", "1")
        assert collector.get_best_ask() == ("100.10", "1")


def test_futures_agg_trade_parse_fields():
    with tempfile.TemporaryDirectory() as d:
        collector = FuturesAggTradeCollector(symbol="btcusdt", data_dir=Path(d))
        record = collector.parse_agg_trade(
            {
                "s": "BTCUSDT", "a": 1, "p": "100.0", "q": "1.0",
                "f": 10, "l": 12, "E": 1000, "T": 999, "m": True,
            }
        )
        assert record["agg_trade_id"] == 1
        assert record["buyer_is_maker"] is True
        assert "raw_futures" in collector.data_path.parts


def test_futures_mark_price_parse_fields():
    with tempfile.TemporaryDirectory() as d:
        collector = FuturesMarkPriceCollector(symbol="btcusdt", data_dir=Path(d))
        record = collector.parse_mark_price(
            {
                "s": "BTCUSDT", "p": "100.5", "i": "100.4",
                "r": "0.0001", "T": 123456, "E": 1000,
            }
        )
        assert record["mark_price"] == "100.5"
        assert record["funding_rate"] == "0.0001"


def test_futures_liquidation_parse_fields():
    with tempfile.TemporaryDirectory() as d:
        collector = FuturesLiquidationCollector(symbol="btcusdt", data_dir=Path(d))
        record = collector.parse_liquidation(
            {
                "E": 1000,
                "o": {
                    "s": "BTCUSDT", "S": "SELL", "o": "LIMIT", "q": "1.0",
                    "p": "100.0", "ap": "99.5", "X": "FILLED",
                    "l": "1.0", "z": "1.0", "T": 999,
                },
            }
        )
        assert record["side"] == "SELL"
        assert record["order_status"] == "FILLED"


def test_trade_gap_detection():
    with tempfile.TemporaryDirectory() as d:
        collector = TradeCollector(symbol="btcusdt", data_dir=Path(d))

        assert collector.check_gap(100) == 0
        assert collector.check_gap(101) == 0
        assert collector.check_gap(105) == 3
        assert collector.check_gap(106) == 0


def test_snapshot_writer_captures_top_levels():
    with tempfile.TemporaryDirectory() as d:
        from collector.depth_collector import DepthCollector

        depth = DepthCollector(symbol="btcusdt", data_dir=Path(d))
        depth.load_snapshot(
            {
                "lastUpdateId": 1,
                "bids": [["100.00", "1"], ["99.50", "2"]],
                "asks": [["100.10", "1"], ["100.20", "2"]],
            }
        )

        writer = SnapshotWriter(symbol="btcusdt", data_dir=Path(d), depth=1)
        writer.capture(depth)

        assert len(writer.buffer) == 1
        record = writer.buffer[0]
        assert record["bids"] == [("100.00", "1")]
        assert record["asks"] == [("100.10", "1")]
