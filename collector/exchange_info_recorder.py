import time
from pathlib import Path

import aiohttp

from collector.base_collector import BaseCollector

EXCHANGE_INFO_URL = "https://data-api.binance.vision/api/v3/exchangeInfo"


class ExchangeInfoRecorder(BaseCollector):
    """
    Snapshots tick size, lot size and trading status for the tracked
    symbols once a day. These rarely change, but when they do (a symbol
    gets halted, filters get retuned) it can silently invalidate analysis
    that assumes fixed precision/limits.
    """

    def __init__(self, symbols: list, data_dir: Path = None):
        super().__init__(
            symbol="_GLOBAL",
            stream_name="exchange_info",
            buffer_size=10_000,
            data_dir=data_dir,
            market="spot",
        )

        self.symbols = [s.upper() for s in symbols]

    @staticmethod
    def _extract_filters(symbol_info: dict) -> dict:
        filters = {f["filterType"]: f for f in symbol_info.get("filters", [])}

        price_filter = filters.get("PRICE_FILTER", {})
        lot_size = filters.get("LOT_SIZE", {})

        return {
            "tick_size": price_filter.get("tickSize"),
            "step_size": lot_size.get("stepSize"),
            "min_qty": lot_size.get("minQty"),
        }

    async def fetch_and_flush(self, session: aiohttp.ClientSession):
        params = {"symbols": str(self.symbols).replace("'", '"')}

        async with session.get(EXCHANGE_INFO_URL, params=params) as response:
            response.raise_for_status()
            payload = await response.json()

        now_ms = int(time.time() * 1000)

        for symbol_info in payload.get("symbols", []):
            record = {
                "timestamp_ms": now_ms,
                "symbol": symbol_info["symbol"],
                "status": symbol_info["status"],
                "base_asset": symbol_info["baseAsset"],
                "quote_asset": symbol_info["quoteAsset"],
                **self._extract_filters(symbol_info),
            }

            self.add_to_buffer(record)

        return self.flush_buffer()
