import tempfile
import time
from pathlib import Path

import pytest

from collector.depth_collector import DepthCollector


def make_collector(**kwargs):
    tmp = tempfile.mkdtemp()
    return DepthCollector(symbol="btcusdt", data_dir=Path(tmp), **kwargs)


def snapshot(last_update_id, bids=None, asks=None):
    return {
        "lastUpdateId": last_update_id,
        "bids": bids or [["100.00", "1.0"]],
        "asks": asks or [["100.10", "1.0"]],
    }


def update(first_id, final_id, bids=None, asks=None, previous_update_id=None):
    return {
        "first_update_id": first_id,
        "final_update_id": final_id,
        "previous_update_id": previous_update_id,
        "bids": bids or [],
        "asks": asks or [],
    }


def make_futures_collector(**kwargs):
    tmp = tempfile.mkdtemp()
    return DepthCollector(
        symbol="btcusdt", data_dir=Path(tmp), market="futures", **kwargs
    )


class TestNormalSync:
    def test_single_matching_update_syncs(self):
        dc = make_collector()
        dc.load_snapshot(snapshot(100))

        dc.buffer_pending_update(update(95, 105, bids=[["99.00", "2.0"]]))

        assert dc.sync_pending_updates() is True
        assert dc.is_synced is True
        assert dc.last_update_id == 105
        assert dc.get_best_bid() == ("100.00", "1.0")

    def test_applies_all_buffered_updates_after_sync_point(self):
        dc = make_collector()
        dc.load_snapshot(snapshot(100))

        dc.buffer_pending_update(update(95, 105, bids=[["99.00", "2.0"]]))
        dc.buffer_pending_update(update(106, 110, asks=[["100.10", "3.0"]]))

        assert dc.sync_pending_updates() is True
        assert dc.last_update_id == 110
        assert dc.get_best_ask() == ("100.10", "3.0")

    def test_updates_entirely_before_snapshot_are_discarded(self):
        dc = make_collector()
        dc.load_snapshot(snapshot(100))

        dc.buffer_pending_update(update(80, 90))
        assert dc.sync_pending_updates() is False

        dc.buffer_pending_update(update(95, 105))
        assert dc.sync_pending_updates() is True


class TestGapHandling:
    def test_live_gap_is_detected_and_invalidates_sync(self):
        dc = make_collector()
        dc.load_snapshot(snapshot(100))
        dc.buffer_pending_update(update(95, 105))
        assert dc.sync_pending_updates() is True

        # Expected next first_update_id is 106; a jump to 110 is a gap.
        success = dc.process_live_update(update(110, 115))

        assert success is False
        assert dc.is_synced is False

    def test_continuous_live_updates_apply_normally(self):
        dc = make_collector()
        dc.load_snapshot(snapshot(100))
        dc.buffer_pending_update(update(95, 105))
        dc.sync_pending_updates()

        assert dc.process_live_update(update(106, 110, bids=[["99.50", "5.0"]])) is True
        assert dc.last_update_id == 110
        # 100.00 (from the snapshot) is still the higher, best bid; the new
        # level from the update just gets added underneath it.
        assert dc.get_best_bid() == ("100.00", "1.0")
        assert dc.local_bids[99.50] == ("99.50", "5.0")

    def test_stale_update_is_ignored_without_breaking_sync(self):
        dc = make_collector()
        dc.load_snapshot(snapshot(100))
        dc.buffer_pending_update(update(95, 105))
        dc.sync_pending_updates()

        # final_update_id <= last_update_id: already-applied update, replay-safe.
        assert dc.process_live_update(update(90, 105)) is True
        assert dc.is_synced is True


class TestStaleSnapshot:
    def test_sync_never_completes_if_snapshot_is_older_than_buffer(self):
        dc = make_collector()
        # Snapshot is far behind the first buffered update: no update's
        # range will ever straddle last_update_id + 1.
        dc.load_snapshot(snapshot(5))
        dc.buffer_pending_update(update(1000, 1010))

        assert dc.sync_pending_updates() is False
        assert dc.is_synced is False

    def test_is_sync_stalled_detects_the_situation(self):
        dc = make_collector()
        dc.load_snapshot(snapshot(5))

        assert dc.is_sync_stalled(timeout_seconds=1000) is False

        dc.sync_started_at = time.time() - 60
        assert dc.is_sync_stalled(timeout_seconds=30) is True


class TestFuturesSync:
    """
    Futures diff events use "pu" (previous event's u) for continuity instead
    of spot's U/u-vs-lastUpdateId+1 chaining, and the first synced event only
    needs U <= lastUpdateId <= u (no +1). See Binance's futures order book
    management docs.
    """

    def test_first_event_syncs_without_pu_matching_snapshot(self):
        dc = make_futures_collector()
        dc.load_snapshot(snapshot(100))

        # U <= 100 <= u, no +1; pu is unrelated to the snapshot's lastUpdateId.
        dc.buffer_pending_update(update(95, 105, previous_update_id=42))

        assert dc.sync_pending_updates() is True
        assert dc.last_update_id == 105

    def test_pu_chain_gap_breaks_continuity(self):
        dc = make_futures_collector()
        dc.load_snapshot(snapshot(100))
        dc.buffer_pending_update(update(95, 105, previous_update_id=42))
        dc.sync_pending_updates()

        # Next event's pu should be 105 (previous u); 999 is a gap.
        success = dc.process_live_update(update(106, 110, previous_update_id=999))

        assert success is False
        assert dc.is_synced is False

    def test_pu_chain_continuous_updates_apply(self):
        dc = make_futures_collector()
        dc.load_snapshot(snapshot(100))
        dc.buffer_pending_update(update(95, 105, previous_update_id=42))
        dc.sync_pending_updates()

        success = dc.process_live_update(
            update(106, 110, previous_update_id=105, bids=[["99.00", "3.0"]])
        )

        assert success is True
        assert dc.last_update_id == 110
        assert dc.local_bids[99.00] == ("99.00", "3.0")


class TestOrderBookQueries:
    def test_best_bid_ask_and_spread(self):
        dc = make_collector()
        dc.load_snapshot(
            snapshot(
                1,
                bids=[["100.00", "1"], ["99.50", "2"], ["99.00", "3"]],
                asks=[["100.10", "1"], ["100.20", "2"]],
            )
        )

        assert dc.get_best_bid() == ("100.00", "1")
        assert dc.get_best_ask() == ("100.10", "1")
        assert dc.get_spread() == pytest.approx(0.10)

    def test_top_n_levels_ordered_best_first(self):
        dc = make_collector()
        dc.load_snapshot(
            snapshot(
                1,
                bids=[["100.00", "1"], ["99.50", "2"], ["99.00", "3"]],
                asks=[["100.10", "1"], ["100.20", "2"], ["100.30", "3"]],
            )
        )

        assert dc.get_top_bids(2) == [("100.00", "1"), ("99.50", "2")]
        assert dc.get_top_asks(2) == [("100.10", "1"), ("100.20", "2")]

    def test_zero_quantity_removes_level(self):
        dc = make_collector()
        dc.load_snapshot(snapshot(100, bids=[["100.00", "1.0"]]))
        dc.buffer_pending_update(update(95, 101, bids=[["100.00", "0"]]))

        dc.sync_pending_updates()

        assert dc.get_best_bid() is None

    def test_empty_book_returns_none(self):
        dc = make_collector()
        assert dc.get_best_bid() is None
        assert dc.get_best_ask() is None
        assert dc.get_spread() is None
