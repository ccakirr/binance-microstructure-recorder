import time
from pathlib import Path

from collector.base_collector import BaseCollector


class Depth20Collector(BaseCollector):
    """
    Records Binance's own top-20 order book snapshots
    (`<symbol>@depth20@100ms`). These come pre-computed from Binance, so
    they need no local reconstruction/sync logic and act as a trustworthy
    reference to validate the diff-stream-based DepthCollector against.
    """

    def __init__(
        self,
        symbol: str = "xrpusdt",
        buffer_size: int = 5000,
        data_dir: Path = None,
    ):
        super().__init__(
            symbol=symbol,
            stream_name="depth20",
            buffer_size=buffer_size,
            data_dir=data_dir,
            market="spot",
        )

        self.ws_url = (
            f"wss://stream.binance.com:9443/ws/{self.symbol}@depth20@100ms"
        )

        self.latest = None

    def parse_depth20(self, data: dict) -> dict:
        record = {
            "last_update_id": data["lastUpdateId"],
            "bids": data["bids"],
            "asks": data["asks"],
            "local_receive_time": int(time.time() * 1000),
        }

        self.latest = record

        return record

    def get_best_bid(self):
        if not self.latest or not self.latest["bids"]:
            return None

        return tuple(self.latest["bids"][0])

    def get_best_ask(self):
        if not self.latest or not self.latest["asks"]:
            return None

        return tuple(self.latest["asks"][0])
