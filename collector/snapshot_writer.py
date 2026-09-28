import time
from pathlib import Path

from collector.base_collector import BaseCollector


class SnapshotWriter(BaseCollector):
    """
    Periodically captures the top-N levels of a synced DepthCollector's
    local order book, independent of the raw diff stream. Useful as a
    cheap, easy-to-read reference for research without replaying every
    diff, and as ground truth to validate the reconstructed book against.
    """

    def __init__(
        self,
        symbol: str = "xrpusdt",
        buffer_size: int = 200,
        data_dir: Path = None,
        market: str = "spot",
        depth: int = 20,
        interval_seconds: float = 1.0,
    ):
        super().__init__(
            symbol=symbol,
            stream_name="snapshots",
            buffer_size=buffer_size,
            data_dir=data_dir,
            market=market,
        )

        self.depth = depth
        self.interval_seconds = interval_seconds
        self.last_capture_time = 0.0

    def should_capture(self) -> bool:
        return (time.time() - self.last_capture_time) >= self.interval_seconds

    def capture(self, depth_collector) -> bool:
        self.last_capture_time = time.time()

        record = {
            "timestamp_ms": int(time.time() * 1000),
            "symbol": self.symbol.upper(),
            "last_update_id": depth_collector.last_update_id,
            "bids": depth_collector.get_top_bids(self.depth),
            "asks": depth_collector.get_top_asks(self.depth),
        }

        return self.add_to_buffer(record)
