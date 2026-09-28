import time
from pathlib import Path

from collector.base_collector import BaseCollector, UpdateIdHistory


class BookTickerCollector(BaseCollector):
    def __init__(
        self,
        symbol: str = "xrpusdt",
        buffer_size: int = 5000,
        data_dir: Path = None,
    ):
        super().__init__(
            symbol=symbol,
            stream_name="book_ticker",
            buffer_size=buffer_size,
            data_dir=data_dir,
            market="spot",
        )

        self.ws_url = f"wss://stream.binance.com:9443/ws/{self.symbol}@bookTicker"

        # Latest reading, plus a short by-id history, kept for cross-checking
        # against the locally reconstructed order book from the depth
        # stream (see UpdateIdHistory for why "latest" alone isn't enough).
        self.latest = None
        self.history = UpdateIdHistory(maxlen=100)

    def parse_book_ticker(self, data: dict) -> dict:
        record = {
            "best_bid_price": data["b"],
            "best_bid_quantity": data["B"],
            "best_ask_price": data["a"],
            "best_ask_quantity": data["A"],
            "update_id": data["u"],
            "local_receive_time": int(time.time() * 1000),
        }

        self.latest = record
        self.history.add(record["update_id"], record)

        return record
