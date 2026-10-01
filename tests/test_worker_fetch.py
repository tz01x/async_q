import asyncio

import pytest
from redis.exceptions import ConnectionError as RedisConnectionError

from fakes import FakeAsyncRedis, NoopLogger, fake_requeue
from async_q.async_worker import Worker, _next_backoff
from async_q.utils import TaskMetaData, serialize, get_function_ref

pytestmark = pytest.mark.usefixtures("fake_ack_scripts")

Q = "default"


def _module(tmp_path, body):
    get_function_ref.cache_clear()
    mod = tmp_path / "jobs.py"
    mod.write_text(body)
    return str(mod)


def _payload(path, tid, func="job"):
    return serialize(TaskMetaData(id=tid, path=path, func_name=func, args=[], kwargs={}, status="submitted"))


async def _until(pred, timeout=2.0):
    for _ in range(int(timeout / 0.01)):
        if pred():
            return True
        await asyncio.sleep(0.01)
    return False


@pytest.mark.asyncio
async def test_fetch_loop_respects_concurrency(tmp_path):
    path = _module(tmp_path,
        "import asyncio\n"
        "gate = asyncio.Event()\n"
        "started = []\n"
        "async def job(task_id=None):\n"
        "    started.append(task_id)\n"
        "    await gate.wait()\n")
    w = Worker(FakeAsyncRedis(), Q, 1, NoopLogger(), wid="w1")
    for tid in ("a", "b"):
        await w.redis.lpush(w.main_key, _payload(path, tid))
    loop_task = asyncio.create_task(w.fetch_loop())
    jobs = get_function_ref(path, "job").__globals__
    assert await _until(lambda: len(jobs["started"]) == 1)
    await asyncio.sleep(0.1)
    assert len(jobs["started"]) == 1           # second waits for a free slot
    assert len(w.redis.lists[w.main_key]) == 1  # ...and stays in main, not in our backup
    jobs["gate"].set()
    assert await _until(lambda: len(jobs["started"]) == 2)
    loop_task.cancel()
    await asyncio.gather(loop_task, return_exceptions=True)


@pytest.mark.asyncio
async def test_rejected_message_frees_slot(tmp_path):
    path = _module(tmp_path, "ran = []\nasync def job(task_id=None):\n    ran.append(1)\n")
    w = Worker(FakeAsyncRedis(), Q, 1, NoopLogger(), wid="w1")
    await w.redis.lpush(w.main_key, b"\xc1garbage")
    await w.redis.lpush(w.main_key, _payload(path, "ok"))
    loop_task = asyncio.create_task(w.fetch_loop())
    jobs = get_function_ref(path, "job").__globals__
    assert await _until(lambda: jobs["ran"] == [1])
    assert w.redis.lists[w.dead_key] == [b"\xc1garbage"]
    loop_task.cancel()
    await asyncio.gather(loop_task, return_exceptions=True)


@pytest.mark.asyncio
async def test_fetch_loop_survives_connection_error(tmp_path):
    path = _module(tmp_path, "ran = []\nasync def job(task_id=None):\n    ran.append(1)\n")
    redis = FakeAsyncRedis()
    real = redis.brpoplpush
    calls = {"n": 0}

    async def flaky(src, dst, timeout=0):
        calls["n"] += 1
        if calls["n"] == 1:
            raise RedisConnectionError("down")
        return await real(src, dst, timeout)

    redis.brpoplpush = flaky
    w = Worker(redis, Q, 2, NoopLogger(), wid="w1")
    await redis.lpush(w.main_key, _payload(path, "ok"))
    loop_task = asyncio.create_task(w.fetch_loop())
    jobs = get_function_ref(path, "job").__globals__
    assert await _until(lambda: jobs["ran"] == [1], timeout=3.0)  # after one 1s backoff
    # the idle loop holds one slot while polling; a leaked slot from the error would lock both
    assert await _until(lambda: not w.sem.locked())
    loop_task.cancel()
    await asyncio.gather(loop_task, return_exceptions=True)


def test_next_backoff_doubles_and_caps():
    assert [_next_backoff(x) for x in (1, 2, 16, 30)] == [2, 4, 30, 30]


@pytest.mark.asyncio
async def test_inflight_tracks_running_tasks(tmp_path):
    path = _module(tmp_path,
        "import asyncio\ngate = asyncio.Event()\nasync def job(task_id=None):\n    await gate.wait()\n")
    w = Worker(FakeAsyncRedis(), Q, 2, NoopLogger(), wid="w1")
    await w.redis.lpush(w.main_key, _payload(path, "a"))
    loop_task = asyncio.create_task(w.fetch_loop())
    assert await _until(lambda: len(w.inflight) == 1)
    get_function_ref(path, "job").__globals__["gate"].set()
    assert await _until(lambda: not w.inflight and not w.background)
    assert w.redis.lists[w.backup_key] == []
    loop_task.cancel()
    await asyncio.gather(loop_task, return_exceptions=True)


def _fake_requeue(monkeypatch, fail_first=False):
    calls = {"n": 0}

    async def requeue(redis, q, wid, value):
        calls["n"] += 1
        if fail_first and calls["n"] == 1:
            raise RedisConnectionError("down")
        return await fake_requeue(redis, q, wid, value)

    monkeypatch.setattr("async_q.async_worker.scripts.requeue", requeue)
    return calls


async def _stranded_run(tmp_path):
    path = _module(tmp_path, "ran = []\nasync def job(task_id=None):\n    ran.append(1)\n")
    redis = FakeAsyncRedis()
    real_set = redis.set
    sets = {"n": 0}

    async def flaky_set(*a, **kw):
        sets["n"] += 1
        if sets["n"] == 1:
            raise RedisConnectionError("down")
        return await real_set(*a, **kw)

    redis.set = flaky_set
    w = Worker(redis, Q, 2, NoopLogger(), wid="w1")
    await redis.lpush(w.main_key, _payload(path, "ok"))
    loop_task = asyncio.create_task(w.fetch_loop())
    jobs = get_function_ref(path, "job").__globals__
    return w, redis, loop_task, jobs


@pytest.mark.asyncio
async def test_payload_returned_to_main_after_post_fetch_redis_error(tmp_path, monkeypatch):
    calls = _fake_requeue(monkeypatch)
    w, redis, loop_task, jobs = await _stranded_run(tmp_path)
    assert await _until(lambda: jobs["ran"] == [1], timeout=3.0)
    assert await _until(lambda: redis.lists[w.backup_key] == [])
    assert calls["n"] == 1
    loop_task.cancel()
    await asyncio.gather(loop_task, return_exceptions=True)


@pytest.mark.asyncio
async def test_requeue_failure_is_retried(tmp_path, monkeypatch):
    calls = _fake_requeue(monkeypatch, fail_first=True)
    w, redis, loop_task, jobs = await _stranded_run(tmp_path)
    assert await _until(lambda: jobs["ran"] == [1], timeout=6.0)
    assert await _until(lambda: redis.lists[w.backup_key] == [])
    assert calls["n"] == 2
    loop_task.cancel()
    await asyncio.gather(loop_task, return_exceptions=True)


@pytest.mark.asyncio
async def test_fetch_loop_survives_cluster_down_error(tmp_path):
    from redis.exceptions import ClusterDownError
    path = _module(tmp_path, "ran = []\nasync def job(task_id=None):\n    ran.append(1)\n")
    redis = FakeAsyncRedis()
    real = redis.brpoplpush
    calls = {"n": 0}

    async def flaky(src, dst, timeout=0):
        calls["n"] += 1
        if calls["n"] == 1:
            raise ClusterDownError("CLUSTERDOWN")
        return await real(src, dst, timeout)

    redis.brpoplpush = flaky
    w = Worker(redis, Q, 2, NoopLogger(), wid="w1")
    await redis.lpush(w.main_key, _payload(path, "ok"))
    loop_task = asyncio.create_task(w.fetch_loop())
    jobs = get_function_ref(path, "job").__globals__
    assert await _until(lambda: jobs["ran"] == [1], timeout=3.0)
    assert not loop_task.done()
    loop_task.cancel()
    await asyncio.gather(loop_task, return_exceptions=True)
