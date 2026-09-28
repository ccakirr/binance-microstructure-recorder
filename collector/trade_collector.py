import time
from pathlib import Path

from collector.base_collector import BaseCollector


class TradeCollector(BaseCollector):
    def __init__(
        self,
        symbol: str = "xrpusdt",
        buffer_size: int = 5000,
        data_dir: Path = None,
    ):
        super().__init__(
            symbol=symbol,
            stream_name="trades",
            buffer_size=buffer_size,
            data_dir=data_dir,
            market="spot",
        )

        self.ws_url = f"wss://stream.binance.com:9443/ws/{self.symbol}@trade"

        # Used to detect gaps in the trade id sequence.
        self.last_trade_id = None

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

    def check_gap(self, trade_id: int):
        """
        Trade ids are sequential, so a gap is detected the instant a message
        is skipped. Returns the number of missing ids (0 if none / unknown).
        """

        gap = 0

        if self.last_trade_id is not None and trade_id > self.last_trade_id + 1:
            gap = trade_id - self.last_trade_id - 1

        self.last_trade_id = trade_id

        return gap
