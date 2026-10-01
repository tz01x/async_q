import asyncio
import time

import pytest

from fakes import FakeAsyncRedis, FakeTask, NoopLogger
from async_q.async_worker import _task_done
from async_q.utils import (
    TaskMetaData,
    QueueItem,
    serialize,
    deserialize,
    get_redis_q_key,
    get_redis_q_backup_key,
    get_task_key,
    get_retry_zset_key,
    get_dead_letter_key,
)


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

    await _task_done(task, DummyQ(), qname, redis, logger=NoopLogger(), item=item)

    # Should be scheduled for retry
    assert retry_key in redis.zsets
    (scheduled,) = redis.zsets[retry_key]
    assert deserialize(scheduled).attempt == 1
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

    await _task_done(task, DummyQ(), qname, redis, logger=NoopLogger(), item=item)

    # Should be in DLQ
    assert dlq_key in redis.lists
    assert deserialize(redis.lists[dlq_key][0]).id == 'tid2'
    # removed from backup
    assert payload not in redis.lists.get(backup_key, [])


@pytest.mark.asyncio
async def test_task_done_callback_holds_reference_until_done():
    from async_q import async_worker as aw
    redis = FakeAsyncRedis()
    meta = TaskMetaData(id='t3', path='/x.py', func_name='f', args=[], kwargs={}, status='starting')
    payload = serialize(meta)
    item = QueueItem(func_ref=None, deserialize_data=meta, original_data=payload)

    class DummyQ:
        def task_done(self):
            pass

    aw.task_done_callback(FakeTask(result=item), DummyQ(), 'default', redis, NoopLogger(), item)
    assert len(aw._background_tasks) == 1
    await asyncio.gather(*aw._background_tasks)
    await asyncio.sleep(0)  # let discard callback run
    assert len(aw._background_tasks) == 0


@pytest.mark.asyncio
async def test_failing_task_reaches_dlq_after_max_retries():
    from async_q.utils import deserialize
    redis = FakeAsyncRedis()
    qname = 'default'
    retry_key = get_retry_zset_key(qname)
    dlq_key = get_dead_letter_key(qname)
    meta = TaskMetaData(id='t4', path='/x.py', func_name='f', args=[], kwargs={},
                        status='submitted', max_retries=2, backoff_base=1.0, backoff_factor=2.0)
    payload = serialize(meta)

    class DummyQ:
        def task_done(self):
            pass

    delays = []
    for _ in range(meta.max_retries + 1):
        await redis.lpush(get_redis_q_backup_key(qname), payload)  # picked up by worker
        item = QueueItem(func_ref=None, deserialize_data=deserialize(payload), original_data=payload)
        before = time.time()
        await _task_done(FakeTask(exc=RuntimeError('boom')), DummyQ(), qname, redis, NoopLogger(), item)
        due = redis.zsets.get(retry_key, {})
        if not due:
            break
        (payload, score), = due.items()  # promote the retry entry
        delays.append(round(score - before))
        del redis.zsets[retry_key][payload]

    assert delays == [1, 2]
    assert len(redis.lists[dlq_key]) == 1
    assert deserialize(redis.lists[dlq_key][0]).attempt == 2
