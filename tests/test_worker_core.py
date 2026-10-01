import asyncio
import time

import pytest
from redis.exceptions import ConnectionError as RedisConnectionError

from async_q import scripts
from fakes import FakeAsyncRedis, FakeTask, NoopLogger
from async_q.async_worker import Worker
from async_q.utils import QueueItem, TaskMetaData, deserialize, serialize, get_task_key, get_function_ref

pytestmark = pytest.mark.usefixtures("fake_ack_scripts")

Q = "default"


def _worker(redis=None, **kw):
    return Worker(redis or FakeAsyncRedis(), Q, 2, NoopLogger(), wid="w1", **kw)


def _item(meta):
    payload = serialize(meta)
    return QueueItem(func_ref=None, deserialize_data=meta, original_data=payload), payload


def _meta(**kw):
    base = dict(id="t1", path="/x.py", func_name="f", args=[], kwargs={}, status="starting")
    base.update(kw)
    return TaskMetaData(**base)


@pytest.mark.asyncio
async def test_worker_uses_own_backup_key():
    w = _worker()
    assert w.backup_key == "async_task_backup:{default}:w1"
    assert w.hb_key == "async_task_hb:{default}:w1"


@pytest.mark.asyncio
async def test_success_acks_own_backup_and_sets_finished_with_ttl():
    w = _worker(status_ttl=123)
    item, payload = _item(_meta())
    await w.redis.lpush(w.backup_key, payload)
    await w.task_done(FakeTask(result=item), item)
    assert w.redis.lists[w.backup_key] == []
    assert deserialize(w.redis.kv[get_task_key("t1")]).status == "finished"
    assert w.redis.ttl[get_task_key("t1")] == 123


@pytest.mark.asyncio
async def test_every_status_write_has_ttl(tmp_path):
    get_function_ref.cache_clear()
    mod = tmp_path / "ok.py"
    mod.write_text("async def job(task_id=None):\n    pass\n")
    w = _worker(status_ttl=55)
    payload = serialize(_meta(path=str(mod), func_name="job", status="submitted"))
    await w.redis.lpush(w.backup_key, payload)
    item = await w.process_message(payload)       # pending
    await w.run_task(item)                         # starting
    await w.task_done(FakeTask(result=item), item) # finished
    assert w.redis.ttl[get_task_key("t1")] == 55


@pytest.mark.asyncio
async def test_failure_schedules_retry_with_bumped_attempt():
    w = _worker()
    item, payload = _item(_meta(max_retries=2, backoff_base=1.0))
    await w.redis.lpush(w.backup_key, payload)
    before = time.time()
    await w.task_done(FakeTask(exc=RuntimeError("boom")), item)
    ((scheduled, score),) = w.redis.zsets[w.retry_key].items()
    assert deserialize(scheduled).attempt == 1
    assert round(score - before) == 1
    assert w.redis.lists[w.backup_key] == []


@pytest.mark.asyncio
async def test_exhausted_failure_goes_to_dlq():
    w = _worker()
    item, payload = _item(_meta(attempt=2, max_retries=2))
    await w.redis.lpush(w.backup_key, payload)
    await w.task_done(FakeTask(exc=RuntimeError("boom")), item)
    assert deserialize(w.redis.lists[w.dead_key][0]).id == "t1"
    assert deserialize(w.redis.kv[get_task_key("t1")]).status == "failed"


@pytest.mark.asyncio
async def test_late_ack_after_reclaim_does_not_retry():
    w = _worker()
    item, _ = _item(_meta(max_retries=5))
    # payload NOT in backup: a peer already reclaimed it
    await w.task_done(FakeTask(exc=RuntimeError("boom")), item)
    assert w.retry_key not in w.redis.zsets
    assert w.dead_key not in w.redis.lists


@pytest.mark.asyncio
async def test_cancelled_task_requeued_to_front():
    w = _worker()
    item, payload = _item(_meta())
    await w.redis.lpush(w.main_key, b"other")
    await w.redis.lpush(w.backup_key, payload)
    await w.task_done(FakeTask(exc=asyncio.CancelledError()), item)
    assert w.redis.lists[w.main_key] == [b"other", payload]  # right end = fetched next
    assert deserialize(w.redis.kv[get_task_key("t1")]).status == "submitted"


@pytest.mark.asyncio
async def test_cancelled_task_already_reclaimed_is_not_requeued_again():
    w = _worker()
    item, _ = _item(_meta())
    await w.task_done(FakeTask(exc=asyncio.CancelledError()), item)
    assert w.redis.lists.get(w.main_key, []) == []


@pytest.mark.asyncio
async def test_task_done_releases_concurrency_slot():
    w = _worker()
    await w.sem.acquire()
    await w.sem.acquire()
    assert w.sem.locked()
    item, payload = _item(_meta())
    await w.redis.lpush(w.backup_key, payload)
    await w.task_done(FakeTask(result=item), item)
    assert not w.sem.locked()


def _broken(monkeypatch, name):
    async def boom(*a, **k):
        raise RedisConnectionError("down")
    monkeypatch.setattr(scripts, name, boom)


@pytest.mark.asyncio
@pytest.mark.parametrize("attempt", [0, 2])  # retry path, DLQ path
async def test_failure_ack_error_keeps_payload_in_backup(monkeypatch, attempt):
    _broken(monkeypatch, "fail_ack")
    w = _worker()
    item, payload = _item(_meta(attempt=attempt, max_retries=2))
    await w.redis.lpush(w.backup_key, payload)
    with pytest.raises(RedisConnectionError):
        await w.task_done(FakeTask(exc=RuntimeError("boom")), item)
    assert w.redis.lists[w.backup_key] == [payload]  # peers can still reclaim it


@pytest.mark.asyncio
async def test_cancel_requeue_error_keeps_payload_in_backup(monkeypatch):
    _broken(monkeypatch, "requeue")
    w = _worker()
    item, payload = _item(_meta())
    await w.redis.lpush(w.backup_key, payload)
    with pytest.raises(RedisConnectionError):
        await w.task_done(FakeTask(exc=asyncio.CancelledError()), item)
    assert w.redis.lists[w.backup_key] == [payload]


@pytest.mark.asyncio
async def test_reject_error_keeps_payload_in_backup(monkeypatch):
    _broken(monkeypatch, "fail_ack")
    w = _worker()
    await w.redis.lpush(w.backup_key, b"\xc1garbage")
    with pytest.raises(RedisConnectionError):
        await w.process_message(b"\xc1garbage")
    assert w.redis.lists[w.backup_key] == [b"\xc1garbage"]


@pytest.mark.asyncio
async def test_cancelled_after_reclaim_does_not_overwrite_status():
    w = _worker()
    item, _ = _item(_meta())
    await w.task_done(FakeTask(exc=asyncio.CancelledError()), item)
    assert get_task_key("t1") not in w.redis.kv  # a peer owns it now


@pytest.mark.asyncio
async def test_failed_after_reclaim_does_not_overwrite_status():
    w = _worker()
    item, _ = _item(_meta(max_retries=5))
    await w.task_done(FakeTask(exc=RuntimeError("boom")), item)
    assert get_task_key("t1") not in w.redis.kv


@pytest.mark.asyncio
@pytest.mark.parametrize("first", ["success", "failure", "cancel"])
async def test_refetched_own_reclaimed_payload_acks_only_last_copy(first):
    # A peer reclaimed our backup during an outage and we fetched the same bytes again:
    # two in-flight copies, one backup entry. Only the last copy may ack it.
    futs = []

    async def job(task_id=None):
        fut = asyncio.get_running_loop().create_future()
        futs.append(fut)
        await fut

    w = _worker()
    meta = _meta(max_retries=3)
    payload = serialize(meta)
    await w.redis.lpush(w.backup_key, payload)
    copies = [QueueItem(func_ref=job, deserialize_data=meta, original_data=payload) for _ in range(2)]
    for item in copies:
        await w.sem.acquire()
        w.start_task(item)
    assert await _until(lambda: len(futs) == 2)

    if first == "success":
        futs[0].set_result(None)
    elif first == "failure":
        futs[0].set_exception(RuntimeError("boom"))
    else:
        futs[0].cancel()
    assert await _until(lambda: len(w.inflight) == 1 and not w.background)
    assert w.redis.lists[w.backup_key] == [payload]       # still owned by the running copy
    assert w.retry_key not in w.redis.zsets and w.redis.lists.get(w.main_key, []) == []

    futs[1].set_result(None)
    assert await _until(lambda: not w.inflight and not w.background)
    assert w.redis.lists[w.backup_key] == []
    assert deserialize(w.redis.kv[get_task_key("t1")]).status == "finished"
    assert not w.sem.locked() and w.sem._value == 2      # both slots released
    assert not w.copies


async def _until(pred, timeout=2.0):
    for _ in range(int(timeout / 0.01)):
        if pred():
            return True
        await asyncio.sleep(0.01)
    return False


class RecordingLogger(NoopLogger):
    def __init__(self):
        self.errors = []

    def error(self, msg, *a, **k):
        self.errors.append(a)


@pytest.mark.asyncio
async def test_ack_failure_is_logged(monkeypatch):
    async def broken_lrem(*a, **k):
        raise RedisConnectionError("ack lost")

    async def job(task_id=None):
        pass

    log = RecordingLogger()
    w = Worker(FakeAsyncRedis(), Q, 1, log, wid="w1")
    w.redis.lrem = broken_lrem
    await w.sem.acquire()
    w.start_task(QueueItem(func_ref=job, deserialize_data=_meta(), original_data=b"p"))
    assert await _until(lambda: not w.inflight and not w.background)
    await asyncio.sleep(0)
    assert any(isinstance(arg, RedisConnectionError) for args in log.errors for arg in args)
