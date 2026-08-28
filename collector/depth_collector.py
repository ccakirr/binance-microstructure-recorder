import time
from pathlib import Path

import aiohttp
import pandas as pd


class DepthCollector:
    def __init__(self, symbol: str = "xrpusdt", buffer_size: int = 5000):
        self.symbol = symbol.lower()
        self.buffer_size = buffer_size

        self.buffer = []

        self.pending_updates = []

        self.ws_url = (
            f"wss://stream.binance.com:9443/ws/"
            f"{self.symbol}@depth@100ms"
        )

        self.snapshot_url = (
            "https://data-api.binance.vision/api/v3/depth"
            f"?symbol={self.symbol.upper()}&limit=5000"
        )

        self.data_path = (
            Path.cwd()
            / "data"
            / "raw"
            / self.symbol.upper()
            / "depth"
        )

        # Local order book state
        self.last_update_id = None
        self.local_bids = {}
        self.local_asks = {}
        self.is_synced = False

    def ensure_data_path(self):
        self.data_path.mkdir(parents=True, exist_ok=True)

    def parse_depth(self, data: dict) -> dict:
        return {
            "symbol": data["s"],
            "event_time": data["E"],
            "first_update_id": data["U"],
            "final_update_id": data["u"],
            "bids": data["b"],
            "asks": data["a"],
            "local_receive_time": int(time.time() * 1000),
        }

    def add_to_buffer(self, depth: dict) -> bool:
        self.buffer.append(depth)
        return len(self.buffer) >= self.buffer_size

    def flush_buffer(self):
        if not self.buffer:
            return

        df = pd.DataFrame(self.buffer)

        self.ensure_data_path()

        filename = f"depth_{int(time.time() * 1000)}.parquet"
        file_path = self.data_path / filename

        df.to_parquet(file_path, index=False)

        self.buffer.clear()

    async def fetch_snapshot(self, session: aiohttp.ClientSession) -> dict:
        async with session.get(self.snapshot_url) as response:
            response.raise_for_status()
            snapshot = await response.json()

        return snapshot

    def load_snapshot(self, snapshot: dict):
        self.last_update_id = snapshot["lastUpdateId"]

        self.local_bids = {
            price: quantity
            for price, quantity in snapshot["bids"]
        }

        self.local_asks = {
            price: quantity
            for price, quantity in snapshot["asks"]
        }

        self.is_synced = False

    def buffer_pending_update(self, depth: dict):
        self.pending_updates.append(depth)

    def apply_side_updates(self, book: dict, updates: list):
        for price, quantity in updates:
            if float(quantity) == 0:
                book.pop(price, None)
            else:
                book[price] = quantity

    def apply_depth_update(self, depth: dict):
        self.apply_side_updates(
            self.local_bids,
            depth["bids"],
        )

        self.apply_side_updates(
            self.local_asks,
            depth["asks"],
        )

        self.last_update_id = depth["final_update_id"]

    def discard_old_pending_updates(self):
        if self.last_update_id is None:
            return

        self.pending_updates = [
            update
            for update in self.pending_updates
            if update["final_update_id"] > self.last_update_id
        ]

    def find_first_sync_update_index(self):
        if self.last_update_id is None:
            return None

        expected_update_id = self.last_update_id + 1

        for index, update in enumerate(self.pending_updates):
            first_id = update["first_update_id"]
            final_id = update["final_update_id"]

            if first_id <= expected_update_id <= final_id:
                return index

        return None

    def sync_pending_updates(self) -> bool:
        if self.last_update_id is None:
            return False

        self.discard_old_pending_updates()

        if not self.pending_updates:
            return False

        first_sync_index = self.find_first_sync_update_index()

        if first_sync_index is None:
            return False

        self.pending_updates = self.pending_updates[first_sync_index:]

        for update in self.pending_updates:
            if not self._is_update_continuous(update):
                self.invalidate_sync()
                return False

            self.apply_depth_update(update)

        self.pending_updates.clear()
        self.is_synced = True

        return True

    def _is_update_continuous(self, depth: dict) -> bool:
        if self.last_update_id is None:
            return False

        expected_id = self.last_update_id + 1

        return (
            depth["first_update_id"]
            <= expected_id
            <= depth["final_update_id"]
        )

    def process_live_update(self, depth: dict) -> bool:
        """
        Applies an incoming websocket update if the local order book is
        already synced.

        Returns:
            True  -> update applied successfully
            False -> sequence gap detected
        """

        if not self.is_synced:
            self.buffer_pending_update(depth)
            return False

        if depth["final_update_id"] <= self.last_update_id:
            return True

        if not self._is_update_continuous(depth):
            self.invalidate_sync()
            return False

        self.apply_depth_update(depth)

        return True

    def invalidate_sync(self):
        self.is_synced = False
        self.last_update_id = None

        self.local_bids.clear()
        self.local_asks.clear()

        self.pending_updates.clear()

    def get_best_bid(self):
        if not self.local_bids:
            return None

        price = max(
            self.local_bids.keys(),
            key=float,
        )

        return price, self.local_bids[price]

    def get_best_ask(self):
        if not self.local_asks:
            return None

        price = min(
            self.local_asks.keys(),
            key=float,
        )

        return price, self.local_asks[price]

    def get_spread(self):
        best_bid = self.get_best_bid()
        best_ask = self.get_best_ask()

        if best_bid is None or best_ask is None:
            return None

        bid_price = float(best_bid[0])
        ask_price = float(best_ask[0])

        return ask_price - bid_price
