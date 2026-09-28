import time
from pathlib import Path

from collector.base_collector import BaseCollector


class EventLogger(BaseCollector):
    """
    Records operational events (connect, disconnect, resync, sequence gaps,
    validation mismatches, clock offset, ...) to their own parquet stream so
    `check_data_quality`-style tooling has something concrete to read
    instead of grepping logs.
    """

    def __init__(
        self,
        symbol: str = "GLOBAL",
        buffer_size: int = 200,
        data_dir: Path = None,
    ):
        super().__init__(
            symbol=symbol,
            stream_name="events",
            buffer_size=buffer_size,
            data_dir=data_dir,
            market="spot",
        )

    def log(self, stream: str, event_type: str, detail: str = "") -> bool:
        record = {
            "timestamp_ms": int(time.time() * 1000),
            "symbol": self.symbol.upper(),
            "stream": stream,
            "event_type": event_type,
            "detail": detail,
        }

        return self.add_to_buffer(record)
