import time
import pandas as pd
from pathlib import Path


class TradeCollector:
    def __init__(self, symbol: str = "xrpusdt", buffer_size: int = 5000):
        self.ws_url = f"wss://stream.binance.com:9443/ws/{symbol}@trade"
        self.buffer_size = buffer_size
        self.symbol = symbol
        self.buffer = []

        self.data_path = (
            Path.cwd() / "data" / "raw" / self.symbol.upper() / "trades"
        )

    def parse_trade(self, data: dict) -> dict:
        return {
            "symbol": data["s"],
            "trade_id": data["t"],
            "price": data["p"],
            "quantity": data["q"],
            "event_time": data["E"],
            "trade_time": data["T"],
            "buyer_is_maker": data["m"],
            "local_receive_time": int(time.time() * 1000),
        }

    def ensure_data_path(self):
        self.data_path.mkdir(parents=True, exist_ok=True)

    def add_to_buffer(self, trade: dict) -> bool:
        self.buffer.append(trade)
        return len(self.buffer) >= self.buffer_size

    def flush_buffer(self):
        if not self.buffer:
            return

        df = pd.DataFrame(self.buffer)

        self.ensure_data_path()

        filename = f"trades_{int(time.time() * 1000)}.parquet"
        file_path = self.data_path / filename

        df.to_parquet(file_path)
        self.buffer.clear()
