import asyncio
from typing import Any

import pytest

from async_q.async_worker import AsyncQueue


@pytest.mark.asyncio
async def test_async_queue_full_semantics():
    # The queue uses both internal size and active async tasks (named with get_task_key)
    q = AsyncQueue(maxsize=1)

    await q.put("item1")
    assert q.full() is True  # maxsize reached

    # Remove the item and ensure not full
    _ = await q.get()
    q.task_done()
    assert q.full() is False


@pytest.mark.asyncio
async def test_get_nowait_behavior():
    q = AsyncQueue(maxsize=2)
    with pytest.raises(asyncio.QueueEmpty):
        q.get_nowait()

    # put_nowait checks full(), which inspects running tasks; needs a running loop
    q.put_nowait("x")
    v = q.get_nowait()
    assert v == "x"


