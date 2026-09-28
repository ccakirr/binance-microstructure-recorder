import time

from collector.base_collector import UpdateIdHistory


def test_get_returns_none_for_unknown_id():
    history = UpdateIdHistory(window_seconds=5.0)

    assert history.get(1) is None


def test_get_finds_a_recently_added_id_regardless_of_arrival_order():
    history = UpdateIdHistory(window_seconds=5.0)

    history.add(100, "a")
    history.add(101, "b")
    history.add(102, "c")

    assert history.get(100) == "a"
    assert history.get(101) == "b"
    assert history.get(102) == "c"


def test_entries_older_than_the_window_are_evicted():
    history = UpdateIdHistory(window_seconds=0.1)

    history.add(1, "old")
    time.sleep(0.15)
    history.add(2, "new")

    assert history.get(1) is None
    assert history.get(2) == "new"


def test_max_records_caps_memory_even_within_the_time_window():
    history = UpdateIdHistory(window_seconds=1000, max_records=5)

    for i in range(10):
        history.add(i, i)

    assert history.get(0) is None
    assert history.get(4) is None
    assert history.get(9) == 9


def test_re_adding_the_same_id_survives_eviction_of_the_older_entry():
    # A given update id can legitimately be add()-ed twice (e.g. the same
    # depth update reprocessed after a resync). The two entries share one
    # slot in `_by_id` but occupy two slots in the eviction queue; eviction
    # only runs inside add(), so it takes a subsequent add() (of anything)
    # to actually trigger it -- the older of the two same-id entries
    # expiring must not wipe out the newer value at that point.
    history = UpdateIdHistory(window_seconds=0.15)

    history.add(1, "first")
    time.sleep(0.1)
    history.add(1, "second")
    time.sleep(0.1)  # first entry's 0.15s window has now elapsed, second's hasn't
    history.add(2, "trigger_eviction")

    assert history.get(1) == "second"
