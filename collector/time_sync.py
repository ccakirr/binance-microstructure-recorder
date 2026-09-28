import time

import aiohttp

SERVER_TIME_URL = "https://data-api.binance.vision/api/v3/time"


async def measure_clock_offset(session: aiohttp.ClientSession) -> int:
    """
    Compares Binance's server clock to the local clock, so latency
    analysis (local_receive_time - event_time) can be corrected for clock
    drift instead of silently absorbing it. Keep the host's clock
    synchronized with chrony/ntpd; this only measures the residual offset.
    """

    request_sent_at = time.time()

    async with session.get(SERVER_TIME_URL) as response:
        response.raise_for_status()
        payload = await response.json()

    round_trip = time.time() - request_sent_at
    local_time_ms = int((request_sent_at + round_trip / 2) * 1000)

    return payload["serverTime"] - local_time_ms
