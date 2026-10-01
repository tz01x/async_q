import pytest


@pytest.mark.asyncio
async def test_async_worker_requires_initialized_app(reset_async_task_queue):
    from async_q.async_worker import async_worker
    with pytest.raises(RuntimeError, match="not initialized"):
        await async_worker()
