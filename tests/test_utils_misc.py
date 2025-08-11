import asyncio

import pytest

from async_q.utils import (
    get_redis_q_key,
    get_redis_q_backup_key,
    get_task_key,
    to_thread,
)


def test_key_builders():
    assert get_redis_q_key("default") == "async_task:default"
    assert get_redis_q_backup_key("default") == "async_task_backup:default"
    assert get_task_key("123") == "async-q-task:123"


@pytest.mark.asyncio
async def test_to_thread_executes_in_executor():
    def blocking_add(a, b):
        import time
        time.sleep(0.01)
        return a + b

    result = await to_thread(blocking_add, 2, 3)
    assert result == 5


