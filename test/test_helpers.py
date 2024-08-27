from datetime import datetime

from pgqueuer.helpers import perf_counter_dt


async def test_perf_counter_dt() -> None:
    assert isinstance(perf_counter_dt(), datetime)
    assert perf_counter_dt().tzinfo is not None
