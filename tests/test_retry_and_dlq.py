import asyncio
import time

import pytest

from async_q.async_worker import _task_done
from async_q.utils import (
    TaskMetaData,
    QueueItem,
    serialize,
    get_redis_q_key,
    get_redis_q_backup_key,
    get_task_key,
    get_retry_zset_key,
    get_dead_letter_key,
)


class FakeAsyncRedis:
    def __init__(self):
        self.lists = {}
        self.kv = {}
        self.zsets = {}

    # list ops
    async def lpush(self, key, value):
        self.lists.setdefault(key, [])
        self.lists[key].insert(0, value)
        return len(self.lists[key])

    async def lrem(self, key, count, value):
        lst = self.lists.get(key, [])
        removed = 0
        if count == 0:
            # remove all occurrences
            new = [v for v in lst if v != value]
            removed = len(lst) - len(new)
            self.lists[key] = new
        else:
            new = []
            c = count
            for v in lst:
                if v == value and c > 0:
                    removed += 1
                    c -= 1
                else:
                    new.append(v)
            self.lists[key] = new
        return removed

    async def rpoplpush(self, src, dst):
        src_list = self.lists.get(src, [])
        if not src_list:
            return None
        val = src_list.pop()
        self.lists[src] = src_list
        await self.lpush(dst, val)
        return val

    # kv ops
    async def set(self, key, value, ex=None):
        self.kv[key] = value
        return True

    # zset ops
    async def zadd(self, key, mapping):
        z = self.zsets.setdefault(key, {})
        for member, score in mapping.items():
            z[member] = score
        return len(mapping)

    async def zrangebyscore(self, key, min, max, start=0, num=None):
        z = self.zsets.get(key, {})
        items = [(m, s) for m, s in z.items() if s <= max]
        items.sort(key=lambda x: x[1])
        members = [m for m, _ in items]
        if num is None:
            return members[start:]
        return members[start:start + num]

    async def zrem(self, key, member):
        z = self.zsets.get(key, {})
        if member in z:
            del z[member]
            return 1
        return 0


class FakeTask:
    def __init__(self, exc=None, result=None):
        self._exc = exc
        self._result = result

    def result(self):
        if self._exc:
            raise self._exc
        return self._result


@pytest.mark.asyncio
async def test_task_done_schedules_retry_when_under_limit():
    redis = FakeAsyncRedis()
    qname = 'default'
    backup_key = get_redis_q_backup_key(qname)
    retry_key = get_retry_zset_key(qname)

    meta = TaskMetaData(
        id='tid1',
        path='/tmp/x.py',
        func_name='foo',
        args=[],
        kwargs={},
        status='starting',
        attempt=0,
        max_retries=3,
        backoff_base=0.1,
        backoff_factor=2.0,
        backoff_max=1.0,
    )
    payload = serialize(meta)
    # simulate it currently in backup
    await redis.lpush(backup_key, payload)

    item = QueueItem(func_ref=None, deserialize_data=meta, original_data=payload)

    # Simulate task failure
    task = FakeTask(exc=RuntimeError('boom'))

    class DummyQ:
        def task_done(self):
            pass

    await _task_done(task, DummyQ(), qname, redis, logger=_NoopLogger(), item=item)

    # Should be scheduled for retry
    assert retry_key in redis.zsets
    assert payload in redis.zsets[retry_key]
    # removed from backup
    assert payload not in redis.lists.get(backup_key, [])


@pytest.mark.asyncio
async def test_task_done_moves_to_dlq_when_exhausted():
    redis = FakeAsyncRedis()
    qname = 'default'
    backup_key = get_redis_q_backup_key(qname)
    dlq_key = get_dead_letter_key(qname)

    meta = TaskMetaData(
        id='tid2',
        path='/tmp/x.py',
        func_name='foo',
        args=[],
        kwargs={},
        status='starting',
        attempt=3,
        max_retries=3,
    )
    payload = serialize(meta)
    await redis.lpush(backup_key, payload)

    item = QueueItem(func_ref=None, deserialize_data=meta, original_data=payload)
    task = FakeTask(exc=RuntimeError('boom'))

    class DummyQ:
        def task_done(self):
            pass

    await _task_done(task, DummyQ(), qname, redis, logger=_NoopLogger(), item=item)

    # Should be in DLQ
    assert dlq_key in redis.llists if False else True  # placeholder to avoid linter
    assert dlq_key in redis.lists
    assert redis.lists[dlq_key][0] == payload
    # removed from backup
    assert payload not in redis.lists.get(backup_key, [])


class _NoopLogger:
    def debug(self, *a, **k):
        pass

    def info(self, *a, **k):
        pass

    def warning(self, *a, **k):
        pass

    def error(self, *a, **k):
        pass


