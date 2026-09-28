import main as m
from collector.base_collector import UpdateIdHistory


class RecordingLogger:
    def __init__(self):
        self.events = []

    def log(self, stream, event_type, detail=""):
        self.events.append((stream, event_type, detail))


class FakeBookTicker:
    def __init__(self):
        self.history = UpdateIdHistory(window_seconds=5.0)

    def push(self, update_id, bid, ask):
        self.history.add(
            update_id, {"best_bid_price": bid, "best_ask_price": ask, "update_id": update_id}
        )


class FakeDepth:
    def __init__(self, bid, ask, update_id):
        self._bid = bid
        self._ask = ask
        self.last_update_id = update_id
        self.top_history = UpdateIdHistory(window_seconds=5.0)
        self.depth20_resolved_ids = UpdateIdHistory(window_seconds=10.0)

    def get_best_bid(self):
        return self._bid

    def get_best_ask(self):
        return self._ask


class FakeDepth20History:
    """A depth20 stream stand-in exposing just the `.history` lookup
    validate_local_book needs (keyed by lastUpdateId)."""

    def __init__(self):
        self.history = UpdateIdHistory(window_seconds=5.0)

    def push(self, update_id, record):
        self.history.add(update_id, record)


def make_depth20_record(update_id, bid, ask):
    return {"last_update_id": update_id, "bids": [[bid, "1"]], "asks": [[ask, "1"]]}


def test_validate_local_book_skips_check_when_book_ticker_stream_is_absent():
    depth = FakeDepth(("10.0", "1"), ("10.1", "1"), 100)
    logger = RecordingLogger()
    stats = m.new_validation_stats()

    m.validate_local_book(depth, {}, logger, stats)

    assert stats["book_ticker_checks"] == 0
    assert logger.events == []
    # The local top is still recorded, so a later depth20 lookup can use it.
    assert depth.top_history.get(100) == (("10.0", "1"), ("10.1", "1"))


def test_validate_local_book_counts_a_check_but_no_match_when_id_not_in_history():
    depth = FakeDepth(("10.0", "1"), ("10.1", "1"), 999)
    book_ticker = FakeBookTicker()
    book_ticker.push(1, "9.0", "9.1")
    logger = RecordingLogger()
    stats = m.new_validation_stats()

    m.validate_local_book(depth, {"book_ticker": book_ticker}, logger, stats)

    assert stats["book_ticker_checks"] == 1
    assert stats["book_ticker_found"] == 0
    assert logger.events == []


def test_validate_local_book_matching_id_and_equal_prices_is_not_a_mismatch():
    depth = FakeDepth(("10.0", "1"), ("10.1", "1"), 100)
    book_ticker = FakeBookTicker()
    book_ticker.push(100, "10.0", "10.1")
    logger = RecordingLogger()
    stats = m.new_validation_stats()

    m.validate_local_book(depth, {"book_ticker": book_ticker}, logger, stats)

    assert stats["book_ticker_found"] == 1
    assert stats["book_ticker_mismatches"] == 0
    assert logger.events == []


def test_validate_local_book_matching_id_with_different_price_is_a_mismatch():
    depth = FakeDepth(("10.0", "1"), ("10.1", "1"), 100)
    book_ticker = FakeBookTicker()
    book_ticker.push(100, "11.0", "11.1")
    logger = RecordingLogger()
    stats = m.new_validation_stats()

    m.validate_local_book(depth, {"book_ticker": book_ticker}, logger, stats)

    assert stats["book_ticker_found"] == 1
    assert stats["book_ticker_mismatches"] == 1
    event_types = {event_type for _, event_type, _ in logger.events}
    assert "best_bid_mismatch" in event_types
    assert "best_ask_mismatch" in event_types


def test_validate_local_book_ignores_a_newer_book_ticker_latest_reading():
    # book_ticker moved on to a newer id by the time depth reaches 100 --
    # this must not be compared against, only the id-100 entry should be.
    depth = FakeDepth(("10.0", "1"), ("10.1", "1"), 100)
    book_ticker = FakeBookTicker()
    book_ticker.push(100, "10.0", "10.1")
    book_ticker.push(105, "999", "999")
    logger = RecordingLogger()
    stats = m.new_validation_stats()

    m.validate_local_book(depth, {"book_ticker": book_ticker}, logger, stats)

    assert stats["book_ticker_mismatches"] == 0
    assert logger.events == []


def test_depth20_arriving_after_depth_is_matched_and_counted_once():
    # depth reaches update id 100 first (depth20 not running yet from
    # validate_local_book's point of view -- no depth20 ref registered),
    # then the depth20 message for that same id arrives afterwards.
    depth = FakeDepth(("10.0", "1"), ("10.1", "1"), 100)
    depth20 = FakeDepth20History()
    logger = RecordingLogger()
    stats = m.new_validation_stats()

    m.validate_local_book(depth, {"depth20": depth20}, logger, stats)
    assert stats["depth20_checks"] == 1
    assert stats["depth20_found"] == 0  # depth20 hasn't arrived yet

    record = make_depth20_record(100, "10.0", "10.1")
    depth20.push(100, record)
    m.validate_depth20_against_local(record, {"depth": depth}, logger, stats)

    # depth20_checks must not be incremented a second time by the depth20
    # side -- it's only counted once, from the depth side.
    assert stats["depth20_checks"] == 1
    assert stats["depth20_found"] == 1
    assert stats["depth20_mismatches"] == 0
    assert logger.events == []


def test_depth20_arriving_before_depth_is_still_matched_previously_missed_case():
    # depth20's message for update id 100 arrives *before* depth itself has
    # processed that id -- validate_local_book alone would never catch
    # this; it depends on validate_depth20_against_local looking the id up
    # in depth's top_history once depth records it.
    depth = FakeDepth(("10.0", "1"), ("10.1", "1"), 100)
    logger = RecordingLogger()
    stats = m.new_validation_stats()

    record = make_depth20_record(100, "10.0", "10.1")
    m.validate_depth20_against_local(record, {"depth": depth}, logger, stats)

    # depth hasn't reached id 100 yet -- nothing to compare against, and
    # nothing counted (counting happens from the depth side).
    assert stats["depth20_checks"] == 0
    assert stats["depth20_found"] == 0

    depth20 = FakeDepth20History()
    depth20.push(100, record)
    m.validate_local_book(depth, {"depth20": depth20}, logger, stats)

    assert stats["depth20_checks"] == 1
    assert stats["depth20_found"] == 1
    assert stats["depth20_mismatches"] == 0
    assert logger.events == []


def test_depth20_mismatch_is_logged_regardless_of_arrival_order():
    depth = FakeDepth(("10.0", "1"), ("10.1", "1"), 100)
    logger = RecordingLogger()
    stats = m.new_validation_stats()

    # depth20 arrives first with a genuinely different price.
    record = make_depth20_record(100, "12.0", "12.1")
    m.validate_depth20_against_local(record, {"depth": depth}, logger, stats)

    depth20 = FakeDepth20History()
    depth20.push(100, record)
    m.validate_local_book(depth, {"depth20": depth20}, logger, stats)

    assert stats["depth20_mismatches"] == 1
    event_types = {event_type for _, event_type, _ in logger.events}
    assert "depth20_bid_mismatch" in event_types
    assert "depth20_ask_mismatch" in event_types


def test_validate_depth20_no_check_when_depth_collector_is_absent():
    logger = RecordingLogger()
    stats = m.new_validation_stats()

    record = make_depth20_record(100, "10.0", "10.1")
    m.validate_depth20_against_local(record, {}, logger, stats)

    assert stats["depth20_checks"] == 0
    assert logger.events == []


def test_maybe_log_validation_stats_reports_and_resets_after_the_interval():
    logger = RecordingLogger()
    stats = m.new_validation_stats()
    stats["book_ticker_checks"] = 10
    stats["book_ticker_found"] = 4
    last_flush = [0.0]  # far enough in the past to always trigger

    m.maybe_log_validation_stats(stats, last_flush, logger)

    assert len(logger.events) == 1
    _, event_type, detail = logger.events[0]
    assert event_type == "validation_stats"
    assert "4/10" in detail
    assert stats["book_ticker_checks"] == 0


def test_maybe_log_validation_stats_does_not_report_before_the_interval():
    import time

    logger = RecordingLogger()
    stats = m.new_validation_stats()
    last_flush = [time.time()]

    m.maybe_log_validation_stats(stats, last_flush, logger)

    assert logger.events == []
