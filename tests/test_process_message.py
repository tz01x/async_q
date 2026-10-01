import msgpack
import pytest

from fakes import FakeAsyncRedis, NoopLogger
from async_q.async_worker import Worker
from async_q.utils import (
    TaskMetaData, deserialize, serialize, get_function_ref, get_task_key,
)

pytestmark = pytest.mark.usefixtures("fake_ack_scripts")

Q = "default"


async def _run(payload, redis=None):
    w = Worker(redis or FakeAsyncRedis(), Q, 1, NoopLogger(), wid="w1")
    await w.redis.lpush(w.backup_key, payload)  # as brpoplpush would
    item = await w.process_message(payload)
    return w, item


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
    w, item = await _run(payload)
    assert item.original_data == payload
    assert deserialize(w.redis.kv[get_task_key("t1")]).status == "pending"
    assert payload in w.redis.lists[w.backup_key]


@pytest.mark.asyncio
@pytest.mark.parametrize("payload", [b"\xc1not-msgpack", msgpack.packb({"foo": 1})])
async def test_undecodable_payload_goes_to_dlq(payload):
    w, item = await _run(payload)
    assert item is None
    assert w.redis.lists[w.dead_key] == [payload]
    assert payload not in w.redis.lists[w.backup_key]


@pytest.mark.asyncio
async def test_missing_file_goes_to_dlq_with_failed_status(tmp_path):
    payload = serialize(_meta(str(tmp_path / "gone.py")))
    w, item = await _run(payload)
    assert item is None
    assert w.redis.lists[w.dead_key] == [payload]
    assert payload not in w.redis.lists[w.backup_key]
    assert deserialize(w.redis.kv[get_task_key("t1")]).status == "failed"


@pytest.mark.asyncio
async def test_missing_attribute_goes_to_dlq(tmp_path):
    get_function_ref.cache_clear()
    mod = tmp_path / "noattr.py"
    mod.write_text("x = 1\n")
    payload = serialize(_meta(str(mod), "renamed"))
    w, item = await _run(payload)
    assert item is None
    assert w.redis.lists[w.dead_key] == [payload]


@pytest.mark.asyncio
async def test_sync_function_goes_to_dlq(tmp_path):
    get_function_ref.cache_clear()
    mod = tmp_path / "sync.py"
    mod.write_text("def job():\n    pass\n")
    payload = serialize(_meta(str(mod)))
    w, item = await _run(payload)
    assert item is None
    assert w.redis.lists[w.dead_key] == [payload]
