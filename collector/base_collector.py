import time
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from collector.version import get_git_commit


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

        filename = f"{self.stream_name}_{int(time.time() * 1000)}.parquet"
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
