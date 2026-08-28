import time
import pandas as pd
from pathlib import Path


class BookTickerCollector:
    def __init__(self, symbol: str = "xrpusdt", buffer_size: int = 5000):
        self.symbol = symbol
        self.buffer_size = buffer_size
        self.buffer = []
        self.ws_url = f"wss://stream.binance.com:9443/ws/{symbol}@bookTicker"

        self.data_path = (
            Path.cwd() / "data" / "raw" / self.symbol.upper() / "book_ticker"
        )

    def parse_book_ticker(self, data: dict) -> dict:
        return {
            "best_bid_price": data["b"],
            "best_bid_quantity": data["B"],
            "best_ask_price": data["a"],
            "best_ask_quantity": data["A"],
            "update_id": data["u"],
            "local_receive_time": int(time.time() * 1000)
        }

    def ensure_data_path(self):
        self.data_path.mkdir(parents=True, exist_ok=True)

    def add_to_buffer(self, book_ticker: dict) -> bool:
        self.buffer.append(book_ticker)
        return len(self.buffer) >= self.buffer_size

    def flush_buffer(self):
        if not self.buffer:
            return

        df = pd.DataFrame(self.buffer)

        self.ensure_data_path()

        filename = f"book_ticker_{int(time.time() * 1000)}.parquet"
        file_path = self.data_path / filename

        df.to_parquet(file_path)
        self.buffer.clear()
