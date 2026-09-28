import time
from pathlib import Path

import aiohttp
from sortedcontainers import SortedDict

from collector.base_collector import BaseCollector


class DepthCollector(BaseCollector):
    def __init__(
        self,
        symbol: str = "xrpusdt",
        buffer_size: int = 5000,
        data_dir: Path = None,
        market: str = "spot",
    ):
        super().__init__(
            symbol=symbol,
            stream_name="depth",
            buffer_size=buffer_size,
            data_dir=data_dir,
            market=market,
        )

        self.pending_updates = []
        self.sync_started_at = None

        if market == "spot":
            self.ws_url = (
                f"wss://stream.binance.com:9443/ws/{self.symbol}@depth@100ms"
            )
            self.snapshot_url = (
                "https://data-api.binance.vision/api/v3/depth"
                f"?symbol={self.symbol.upper()}&limit=5000"
            )
        else:
            self.ws_url = f"wss://fstream.binance.com/ws/{self.symbol}@depth@100ms"
            self.snapshot_url = (
                "https://fapi.binance.com/fapi/v1/depth"
                f"?symbol={self.symbol.upper()}&limit=1000"
            )

        # Local order book state. Keyed by float(price) so best bid/ask and
        # top-N snapshots are O(log n) / O(k) instead of scanning every
        # level on every call; the value keeps the original price string so
        # precision/formatting from the exchange is never lost.
        self.last_update_id = None
        self.local_bids = SortedDict()
        self.local_asks = SortedDict()
        self.is_synced = False

    def parse_depth(self, data: dict) -> dict:
        return {
            "symbol": data["s"],
            "event_time": data["E"],
            "first_update_id": data["U"],
            "final_update_id": data["u"],
            # Only present on futures diff events; used instead of U/u for
            # continuity checks there (see _is_update_continuous).
            "previous_update_id": data.get("pu"),
            "bids": data["b"],
            "asks": data["a"],
            "local_receive_time": int(time.time() * 1000),
        }

    async def fetch_snapshot(self, session: aiohttp.ClientSession) -> dict:
        async with session.get(self.snapshot_url) as response:
            response.raise_for_status()
            snapshot = await response.json()

        return snapshot

    def load_snapshot(self, snapshot: dict):
        self.last_update_id = snapshot["lastUpdateId"]

        self.local_bids = SortedDict(
            (float(price), (price, quantity))
            for price, quantity in snapshot["bids"]
        )

        self.local_asks = SortedDict(
            (float(price), (price, quantity))
            for price, quantity in snapshot["asks"]
        )

        self.is_synced = False
        self.sync_started_at = time.time()

    def is_sync_stalled(self, timeout_seconds: float = 30.0) -> bool:
        """
        True if we have been waiting for a matching update for longer than
        timeout_seconds. Per Binance's docs, if the snapshot's lastUpdateId
        is older than the buffered updates by that much, the snapshot is
        stale and should be refetched instead of waited on forever.
        """

        if self.sync_started_at is None:
            return False

        return (time.time() - self.sync_started_at) >= timeout_seconds

    def buffer_pending_update(self, depth: dict):
        self.pending_updates.append(depth)

    def apply_side_updates(self, book: SortedDict, updates: list):
        for price, quantity in updates:
            key = float(price)

            if float(quantity) == 0:
                book.pop(key, None)
            else:
                book[key] = (price, quantity)

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

        if self.market == "futures":
            # Binance's futures docs: drop events with u < lastUpdateId.
            self.pending_updates = [
                update
                for update in self.pending_updates
                if update["final_update_id"] >= self.last_update_id
            ]
        else:
            # Spot docs: drop events with u <= lastUpdateId.
            self.pending_updates = [
                update
                for update in self.pending_updates
                if update["final_update_id"] > self.last_update_id
            ]

    def find_first_sync_update_index(self):
        if self.last_update_id is None:
            return None

        # Spot: first event must satisfy U <= lastUpdateId+1 <= u.
        # Futures: first event must satisfy U <= lastUpdateId <= u (no +1).
        expected_update_id = (
            self.last_update_id
            if self.market == "futures"
            else self.last_update_id + 1
        )

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

        for index, update in enumerate(self.pending_updates):
            # The first applied update was already validated against the
            # snapshot by find_first_sync_update_index (U/u vs lastUpdateId).
            # On futures its "pu" chains to the *previous stream event*, not
            # to the snapshot, so it must not be continuity-checked here.
            if index > 0 and not self._is_update_continuous(update):
                self.invalidate_sync()
                return False

            self.apply_depth_update(update)

        self.pending_updates.clear()
        self.is_synced = True

        return True

    def _is_update_continuous(self, depth: dict) -> bool:
        if self.last_update_id is None:
            return False

        if self.market == "futures":
            # Futures docs: each event's pu must equal the previous event's u.
            return depth.get("previous_update_id") == self.last_update_id

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
        self.sync_started_at = None

        self.local_bids.clear()
        self.local_asks.clear()

        self.pending_updates.clear()

    def get_best_bid(self):
        if not self.local_bids:
            return None

        # Ascending order, so the highest price (best bid) is last.
        return self.local_bids.peekitem(-1)[1]

    def get_best_ask(self):
        if not self.local_asks:
            return None

        # Ascending order, so the lowest price (best ask) is first.
        return self.local_asks.peekitem(0)[1]

    def get_top_bids(self, n: int = 20):
        if not self.local_bids:
            return []

        return [value for _, value in reversed(self.local_bids.items()[-n:])]

    def get_top_asks(self, n: int = 20):
        return [value for _, value in self.local_asks.items()[:n]]

    def get_spread(self):
        best_bid = self.get_best_bid()
        best_ask = self.get_best_ask()

        if best_bid is None or best_ask is None:
            return None

        bid_price = float(best_bid[0])
        ask_price = float(best_ask[0])

        return ask_price - bid_price
