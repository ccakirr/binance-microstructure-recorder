import time
from pathlib import Path

from collector.base_collector import BaseCollector, UpdateIdHistory


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

        # Latest reading, plus a short by-id history, kept for cross-checking
        # against the locally reconstructed order book from the depth
        # stream (see UpdateIdHistory for why "latest" alone isn't enough).
        self.latest = None
        self.history = UpdateIdHistory(maxlen=100)

    def parse_depth20(self, data: dict) -> dict:
        record = {
            "last_update_id": data["lastUpdateId"],
            "bids": data["bids"],
            "asks": data["asks"],
            "local_receive_time": int(time.time() * 1000),
        }

        self.latest = record
        self.history.add(record["last_update_id"], record)

        return record

    @staticmethod
    def best_bid_of(record):
        if not record or not record["bids"]:
            return None

        return tuple(record["bids"][0])

    @staticmethod
    def best_ask_of(record):
        if not record or not record["asks"]:
            return None

        return tuple(record["asks"][0])

    def get_best_bid(self):
        return self.best_bid_of(self.latest)

    def get_best_ask(self):
        return self.best_ask_of(self.latest)
