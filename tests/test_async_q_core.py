import inspect
from typing import Any

import pytest

from async_q.async_q import AsyncTaskQueue, RedisBuilder, submit_task
from async_q.utils import deserialize, get_redis_q_key


class FakeRedis:
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        self.lists = {}

    # mimic LPUSH semantics (push to the head/left)
    def lpush(self, key: str, value: bytes) -> int:
        self.lists.setdefault(key, [])
        self.lists[key].insert(0, value)
        return len(self.lists[key])


def test_submit_task_raises_without_init(reset_async_task_queue):
    async def f():
        return 1

    with pytest.raises(Exception):
        submit_task(f)


def test_submit_task_pushes_to_redis(monkeypatch, reset_async_task_queue):
    # Replace redis.Redis with FakeRedis inside the module
    import async_q.async_q as aq

    monkeypatch.setattr(aq.redis, "Redis", FakeRedis)

    rb = RedisBuilder()
    app = AsyncTaskQueue(redis_builder=rb)
    app.distribute_qname = "default"

    async def my_task(x, *, y=0):
        return x + y

    submit_task(my_task, args=[1], kwargs={"y": 2}, queue_name="default")

    redis_key = get_redis_q_key("default")
    assert isinstance(rb.get_redis(), FakeRedis)
    assert redis_key in rb.get_redis().lists
    assert len(rb.get_redis().lists[redis_key]) == 1

    packed = rb.get_redis().lists[redis_key][0]
    data = deserialize(packed)

    assert data.func_name == my_task.__name__
    assert data.path == inspect.getfile(my_task)
    assert data.args == [1]
    assert data.kwargs == {"y": 2}
    assert data.status == "submitted"
    assert data.id and isinstance(data.id, str)


