import time

import pytest

from fakes import FakeAsyncRedis, FakeTask, NoopLogger
from async_q.utils import TaskMetaData, QueueItem, serialize

pytestmark = pytest.mark.usefixtures("fake_ack_scripts")


@pytest.mark.asyncio
async def test_failing_task_reaches_dlq_after_max_retries():
    from async_q.async_worker import Worker
    from async_q.utils import deserialize
    w = Worker(FakeAsyncRedis(), 'default', 1, NoopLogger(), wid='w1')
    meta = TaskMetaData(id='t4', path='/x.py', func_name='f', args=[], kwargs={},
                        status='submitted', max_retries=2, backoff_base=1.0, backoff_factor=2.0)
    payload = serialize(meta)

    delays = []
    for _ in range(meta.max_retries + 1):
        await w.redis.lpush(w.backup_key, payload)  # picked up by worker
        item = QueueItem(func_ref=None, deserialize_data=deserialize(payload), original_data=payload)
        before = time.time()
        await w.task_done(FakeTask(exc=RuntimeError('boom')), item)
        due = w.redis.zsets.get(w.retry_key, {})
        if not due:
            break
        (payload, score), = due.items()  # promote the retry entry
        delays.append(round(score - before))
        del w.redis.zsets[w.retry_key][payload]

    assert delays == [1, 2]
    assert len(w.redis.lists[w.dead_key]) == 1
    assert deserialize(w.redis.lists[w.dead_key][0]).attempt == 2
