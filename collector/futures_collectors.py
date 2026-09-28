import time
from pathlib import Path

import aiohttp

from collector.base_collector import BaseCollector

FUTURES_WS_BASE = "wss://fstream.binance.com/ws"
FUTURES_REST_BASE = "https://fapi.binance.com"


class FuturesAggTradeCollector(BaseCollector):
    """
    Perpetual futures aggregated trades (`<symbol>@aggTrade`). Combined
    with spot trades, funding rate and open interest this is what lets you
    look at the basis/perp-spot relationship instead of just one leg of it.
    """

    def __init__(
        self,
        symbol: str = "xrpusdt",
        buffer_size: int = 5000,
        data_dir: Path = None,
    ):
        super().__init__(
            symbol=symbol,
            stream_name="agg_trades",
            buffer_size=buffer_size,
            data_dir=data_dir,
            market="futures",
        )

        self.ws_url = f"{FUTURES_WS_BASE}/{self.symbol}@aggTrade"

    def parse_agg_trade(self, data: dict) -> dict:
        return {
            "symbol": data["s"],
            "agg_trade_id": data["a"],
            "price": data["p"],
            "quantity": data["q"],
            "first_trade_id": data["f"],
            "last_trade_id": data["l"],
            "event_time": data["E"],
            "trade_time": data["T"],
            "buyer_is_maker": data["m"],
            "local_receive_time": int(time.time() * 1000),
        }


class FuturesMarkPriceCollector(BaseCollector):
    """
    Mark price, index price and funding rate (`<symbol>@markPrice@1s`) —
    the core inputs for basis and funding-driven signals.
    """

    def __init__(
        self,
        symbol: str = "xrpusdt",
        buffer_size: int = 1000,
        data_dir: Path = None,
    ):
        super().__init__(
            symbol=symbol,
            stream_name="mark_price",
            buffer_size=buffer_size,
            data_dir=data_dir,
            market="futures",
        )

        self.ws_url = f"{FUTURES_WS_BASE}/{self.symbol}@markPrice@1s"

    def parse_mark_price(self, data: dict) -> dict:
        return {
            "symbol": data["s"],
            "mark_price": data["p"],
            "index_price": data["i"],
            "estimated_settle_price": data.get("P"),
            "funding_rate": data.get("r"),
            "next_funding_time": data.get("T"),
            "event_time": data["E"],
            "local_receive_time": int(time.time() * 1000),
        }


class FuturesLiquidationCollector(BaseCollector):
    """
    Forced-liquidation orders (`<symbol>@forceOrder`) — a direct signal of
    leveraged positions getting wiped out, unavailable on spot.
    """

    def __init__(
        self,
        symbol: str = "xrpusdt",
        buffer_size: int = 500,
        data_dir: Path = None,
    ):
        super().__init__(
            symbol=symbol,
            stream_name="liquidations",
            buffer_size=buffer_size,
            data_dir=data_dir,
            market="futures",
        )

        self.ws_url = f"{FUTURES_WS_BASE}/{self.symbol}@forceOrder"

    def parse_liquidation(self, data: dict) -> dict:
        order = data["o"]

        return {
            "symbol": order["s"],
            "side": order["S"],
            "order_type": order["o"],
            "quantity": order["q"],
            "price": order["p"],
            "average_price": order["ap"],
            "order_status": order["X"],
            "last_filled_qty": order["l"],
            "filled_accum_qty": order["z"],
            "trade_time": order["T"],
            "event_time": data["E"],
            "local_receive_time": int(time.time() * 1000),
        }


class FuturesOpenInterestCollector(BaseCollector):
    """
    Open interest has no push stream, so it is polled over REST
    periodically instead of listening on a websocket.
    """

    def __init__(
        self,
        symbol: str = "xrpusdt",
        buffer_size: int = 100,
        data_dir: Path = None,
    ):
        super().__init__(
            symbol=symbol,
            stream_name="open_interest",
            buffer_size=buffer_size,
            data_dir=data_dir,
            market="futures",
        )

        self.rest_url = (
            f"{FUTURES_REST_BASE}/fapi/v1/openInterest"
            f"?symbol={self.symbol.upper()}"
        )

    async def fetch(self, session: aiohttp.ClientSession) -> dict:
        async with session.get(self.rest_url) as response:
            response.raise_for_status()
            payload = await response.json()

        record = {
            "symbol": payload["symbol"],
            "open_interest": payload["openInterest"],
            "exchange_time": payload["time"],
            "local_receive_time": int(time.time() * 1000),
        }

        return record
