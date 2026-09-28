import time
import uuid
from collections import deque
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from collector.version import get_git_commit


class UpdateIdHistory:
    """
    Keeps the records a stream produced over the last `window_seconds`,
    keyed by their update id, so a consumer on a *different* connection can
    look one up by id instead of by "whatever is latest right now".

    This matters for cross-validation: book_ticker/depth20 arrive over
    separate websocket connections from the depth diff stream, so by the
    time a depth update is processed, `.latest` on either of them has
    usually already moved on to a newer id. Comparing only against
    `.latest` would make genuine matches rare (worst on the most active
    symbols, where validation matters most) and "0 mismatches" would
    quietly mean "0 checks" instead of "book confirmed correct". Searching a
    short window of recent ids instead of a single latest one makes the
    connections' timing difference irrelevant.

    The window is time-based rather than a fixed record count: on a busy
    symbol, book_ticker alone can emit hundreds of messages a second, so a
    fixed count (e.g. 100) can cover well under a second of history -- too
    short to still contain the id a slightly-delayed depth/depth20 packet
    needs, in exactly the busy symbols where validation matters most.
    `max_records` is only a safety cap against unbounded memory use if a
    stream misbehaves; under normal conditions it is `window_seconds` that
    determines how far back a lookup can reach.
    """

    def __init__(self, window_seconds: float = 5.0, max_records: int = 20000):
        self._window_seconds = window_seconds
        self._max_records = max_records
        self._order = deque()  # (monotonic_time, update_id)
        self._by_id = {}

    def add(self, update_id, record):
        now = time.monotonic()
        self._order.append((now, update_id))
        self._by_id[update_id] = record
        self._evict(now)

    def _evict(self, now):
        while self._order and (
            now - self._order[0][0] > self._window_seconds
            or len(self._order) > self._max_records
        ):
            _, oldest_id = self._order.popleft()
            self._by_id.pop(oldest_id, None)

    def get(self, update_id):
        return self._by_id.get(update_id)


class BaseCollector:
    """
    Shared buffer/flush/file-naming logic for every collector. A subclass
    only needs to set self.ws_url and implement its own parse_*() method;
    everything about how records are buffered, batched and written to
    parquet lives here in one place.
    """

    def __init__(
        self,
        symbol: str,
        stream_name: str,
        buffer_size: int = 5000,
        data_dir: Path = None,
        market: str = "spot",
    ):
        self.symbol = symbol.lower()
        self.stream_name = stream_name
        self.buffer_size = buffer_size
        self.market = market

        self.buffer = []
        self.last_flush_time = time.time()

        raw_dir = "raw" if market == "spot" else f"raw_{market}"

        self.data_path = (
            (data_dir or Path.cwd())
            / "data"
            / raw_dir
            / self.symbol.upper()
            / stream_name
        )

    def ensure_data_path(self):
        self.data_path.mkdir(parents=True, exist_ok=True)

    def add_to_buffer(self, record: dict) -> bool:
        self.buffer.append(record)
        return len(self.buffer) >= self.buffer_size

    def take_batch(self) -> list:
        """
        Atomically swaps out the in-memory buffer for an empty one and
        returns the swapped-out batch, so a write can proceed without
        blocking (or losing) records added afterwards.
        """

        batch, self.buffer = self.buffer, []
        return batch

    def should_flush_by_time(self, interval_seconds: float) -> bool:
        if not self.buffer:
            return False

        return (time.time() - self.last_flush_time) >= interval_seconds

    def flush_buffer(self, batch: list = None):
        """
        Writes a batch to a new parquet file tagged with the recorder's git
        commit hash. If no batch is given, the current buffer is swapped out
        and written (kept for direct/manual use and tests).
        """

        if batch is None:
            batch = self.take_batch()

        self.last_flush_time = time.time()

        if not batch:
            return None

        df = pd.DataFrame(batch)

        self.ensure_data_path()

        # A uuid suffix keeps two flushes landing in the same millisecond
        # (small buffer sizes, several streams flushing at once) from
        # overwriting each other.
        filename = (
            f"{self.stream_name}_{int(time.time() * 1000)}_"
            f"{uuid.uuid4().hex[:8]}.parquet"
        )
        file_path = self.data_path / filename

        table = pa.Table.from_pandas(df, preserve_index=False)
        table = table.replace_schema_metadata(
            {
                **(table.schema.metadata or {}),
                b"recorder_commit": get_git_commit().encode(),
            }
        )

        pq.write_table(table, file_path, compression="zstd")

        return file_path
