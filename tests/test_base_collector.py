import tempfile
from pathlib import Path

import pyarrow.parquet as pq

from collector.base_collector import BaseCollector
from collector.event_logger import EventLogger
from collector.version import get_git_commit


def test_flush_swaps_buffer_and_tags_commit_hash():
    with tempfile.TemporaryDirectory() as d:
        collector = BaseCollector(
            symbol="btcusdt", stream_name="trades", buffer_size=2, data_dir=Path(d)
        )

        collector.add_to_buffer({"a": 1})
        collector.add_to_buffer({"a": 2})

        file_path = collector.flush_buffer()

        assert collector.buffer == []
        assert file_path.exists()

        table = pq.read_table(file_path)
        assert table.num_rows == 2
        assert table.schema.metadata[b"recorder_commit"].decode() == get_git_commit()


def test_flush_with_no_buffered_records_is_a_no_op():
    with tempfile.TemporaryDirectory() as d:
        collector = BaseCollector(
            symbol="btcusdt", stream_name="trades", data_dir=Path(d)
        )

        assert collector.flush_buffer() is None
        assert not collector.data_path.exists()


def test_futures_market_uses_separate_directory():
    with tempfile.TemporaryDirectory() as d:
        spot = BaseCollector(
            symbol="btcusdt", stream_name="depth", data_dir=Path(d), market="spot"
        )
        futures = BaseCollector(
            symbol="btcusdt", stream_name="depth", data_dir=Path(d), market="futures"
        )

        assert "raw" in spot.data_path.parts
        assert "raw_futures" in futures.data_path.parts
        assert spot.data_path != futures.data_path


def test_event_logger_records_expected_fields():
    with tempfile.TemporaryDirectory() as d:
        logger = EventLogger(symbol="btcusdt", data_dir=Path(d))
        logger.log("depth", "connected", "details")

        assert len(logger.buffer) == 1
        record = logger.buffer[0]
        assert record["symbol"] == "BTCUSDT"
        assert record["stream"] == "depth"
        assert record["event_type"] == "connected"
        assert record["detail"] == "details"
