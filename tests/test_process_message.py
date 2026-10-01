import asyncio

import msgpack
import pytest

from fakes import FakeAsyncRedis, NoopLogger
from async_q.async_worker import _process_message
from async_q.utils import (
    TaskMetaData, deserialize, serialize, get_function_ref,
    get_redis_q_backup_key, get_dead_letter_key, get_task_key,
)

Q = "default"


async def _run(payload, redis=None):
    redis = redis or FakeAsyncRedis()
    await redis.lpush(get_redis_q_backup_key(Q), payload)  # as brpoplpush would
    queue = asyncio.Queue()
    await _process_message(payload, Q, redis, queue, NoopLogger())
    return redis, queue


def _meta(path, func_name="job"):
    # built WITHOUT retry fields: simulates payload from an older producer
    return TaskMetaData(id="t1", path=path, func_name=func_name,
                        args=[], kwargs={}, status="submitted")


@pytest.mark.asyncio
async def test_valid_message_is_queued(tmp_path):
    get_function_ref.cache_clear()
    mod = tmp_path / "ok.py"
    mod.write_text("async def job():\n    pass\n")
    payload = serialize(_meta(str(mod)))
    redis, queue = await _run(payload)
    item = queue.get_nowait()
    assert item.original_data == payload
    assert deserialize(redis.kv[get_task_key("t1")]).status == "pending"
    assert payload in redis.lists[get_redis_q_backup_key(Q)]


@pytest.mark.asyncio
@pytest.mark.parametrize("payload", [b"\xc1not-msgpack", msgpack.packb({"foo": 1})])
async def test_undecodable_payload_goes_to_dlq(payload):
    redis, queue = await _run(payload)
    assert queue.empty()
    assert redis.lists[get_dead_letter_key(Q)] == [payload]
    assert payload not in redis.lists[get_redis_q_backup_key(Q)]


@pytest.mark.asyncio
async def test_missing_file_goes_to_dlq_with_failed_status(tmp_path):
    payload = serialize(_meta(str(tmp_path / "gone.py")))
    redis, queue = await _run(payload)
    assert queue.empty()
    assert redis.lists[get_dead_letter_key(Q)] == [payload]
    assert payload not in redis.lists[get_redis_q_backup_key(Q)]
    assert deserialize(redis.kv[get_task_key("t1")]).status == "failed"


@pytest.mark.asyncio
async def test_missing_attribute_goes_to_dlq(tmp_path):
    get_function_ref.cache_clear()
    mod = tmp_path / "noattr.py"
    mod.write_text("x = 1\n")
    payload = serialize(_meta(str(mod), "renamed"))
    redis, queue = await _run(payload)
    assert queue.empty()
    assert redis.lists[get_dead_letter_key(Q)] == [payload]


@pytest.mark.asyncio
async def test_sync_function_goes_to_dlq(tmp_path):
    get_function_ref.cache_clear()
    mod = tmp_path / "sync.py"
    mod.write_text("def job():\n    pass\n")
    payload = serialize(_meta(str(mod)))
    redis, queue = await _run(payload)
    assert queue.empty()
    assert redis.lists[get_dead_letter_key(Q)] == [payload]
