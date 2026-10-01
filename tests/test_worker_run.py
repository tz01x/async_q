import asyncio
import time

import pytest
from redis.exceptions import ConnectionError as RedisConnectionError

from fakes import FakeAsyncRedis, NoopLogger
from async_q import async_worker as aw
from async_q.async_worker import Worker
from async_q.utils import TaskMetaData, serialize, get_function_ref

Q = "default"


@pytest.fixture
def no_lua(monkeypatch, fake_ack_scripts):
    """FakeAsyncRedis can't run Lua: record script calls instead."""
    calls = []

    async def reclaim(redis, q, wid):
        calls.append(("reclaim", wid))
        return -1

    async def promote_due(redis, q, now, limit=100):
        return 0

    async def release(redis, q, wid):
        calls.append(("release", wid))
        backup = redis.lists.get(aw.get_redis_q_backup_key(q, wid), [])
        while backup:
            await redis.lpush(aw.get_redis_q_key(q), backup.pop())
        return 0

    monkeypatch.setattr(aw.scripts, "reclaim", reclaim)
    monkeypatch.setattr(aw.scripts, "promote_due", promote_due)
    monkeypatch.setattr(aw.scripts, "release", release)
    return calls


def _payload(path, tid, func="job"):
    return serialize(TaskMetaData(id=tid, path=path, func_name=func, args=[], kwargs={}, status="submitted"))


def _module(tmp_path, body):
    get_function_ref.cache_clear()
    mod = tmp_path / "jobs.py"
    mod.write_text(body)
    return str(mod)


async def _until(pred, timeout=2.0):
    for _ in range(int(timeout / 0.01)):
        if pred():
            return True
        await asyncio.sleep(0.01)
    return False


def _w(**kw):
    kw.setdefault("heartbeat_interval", 0.05)
    kw.setdefault("heartbeat_ttl", 0.2)
    return Worker(FakeAsyncRedis(), Q, kw.pop("concurrency", 1), NoopLogger(), wid="me", **kw)


@pytest.mark.asyncio
async def test_run_drains_running_task_before_release(tmp_path, no_lua):
    path = _module(tmp_path, "import asyncio\ndone = []\nasync def job(task_id=None):\n    await asyncio.sleep(0.2)\n    done.append(1)\n")
    w = _w(shutdown_timeout=5)
    await w.redis.lpush(w.main_key, _payload(path, "a"))
    stop = asyncio.Event()
    run = asyncio.create_task(w.run(stop))
    assert await _until(lambda: w.inflight)
    stop.set()
    await asyncio.wait_for(run, 3)
    assert get_function_ref(path, "job").__globals__["done"] == [1]
    assert w.redis.lists[w.backup_key] == []
    assert w.redis.lists.get(w.main_key, []) == []
    assert ("release", "me") in no_lua


@pytest.mark.asyncio
async def test_stop_with_all_slots_busy_requeues_and_exits(tmp_path, no_lua):
    path = _module(tmp_path, "import asyncio\nasync def job(task_id=None):\n    await asyncio.sleep(60)\n")
    w = _w(shutdown_timeout=0.2, concurrency=1)
    await w.redis.lpush(w.main_key, _payload(path, "a"))
    await w.redis.lpush(w.main_key, _payload(path, "b"))
    stop = asyncio.Event()
    run = asyncio.create_task(w.run(stop))
    assert await _until(lambda: w.inflight)
    stop.set()
    t0 = time.monotonic()
    await asyncio.wait_for(run, 3)
    assert time.monotonic() - t0 < 2
    assert len(w.redis.lists[w.main_key]) == 2   # cancelled task requeued + untouched one
    assert w.redis.lists.get(w.backup_key, []) == []


@pytest.mark.asyncio
async def test_task_ignoring_cancel_does_not_hang_shutdown(tmp_path, no_lua):
    path = _module(tmp_path,
        "import asyncio\n"
        "async def job(task_id=None):\n"
        "    for _ in range(2):  # drain cancels once; loop teardown cancels the abandoned task again\n"
        "        try:\n"
        "            await asyncio.sleep(60)\n"
        "        except asyncio.CancelledError:\n"
        "            pass\n")
    w = _w(shutdown_timeout=0.2)
    await w.redis.lpush(w.main_key, _payload(path, "a"))
    stop = asyncio.Event()
    run = asyncio.create_task(w.run(stop))
    assert await _until(lambda: w.inflight)
    stop.set()
    await asyncio.wait_for(run, 3)
    assert ("release", "me") in no_lua


@pytest.mark.asyncio
async def test_release_failure_is_logged_not_raised(no_lua, monkeypatch):
    async def broken_release(redis, q, wid):
        raise RedisConnectionError("down")

    monkeypatch.setattr(aw.scripts, "release", broken_release)
    w = _w()
    stop = asyncio.Event()
    run = asyncio.create_task(w.run(stop))
    await asyncio.sleep(0.05)
    stop.set()
    await asyncio.wait_for(run, 3)  # no exception


@pytest.mark.asyncio
async def test_run_heartbeats_and_reaps_while_running(no_lua):
    w = _w()
    await w.redis.sadd(w.workers_key, "peer")
    stop = asyncio.Event()
    run = asyncio.create_task(w.run(stop))
    assert await _until(lambda: ("reclaim", "peer") in no_lua)
    assert w.hb_key in w.redis.kv
    stop.set()
    await asyncio.wait_for(run, 3)


@pytest.mark.asyncio
async def test_async_worker_runs_worker_from_app(monkeypatch, reset_async_task_queue):
    from async_q.async_q import AsyncTaskQueue
    seen = {}

    async def fake_run(self, stop_event):
        seen.update(q=self.q, ttl=self.status_ttl, timeout=self.shutdown_timeout)

    closed = {}

    class FakeBuilder:
        def get_redis_async(self):
            r = FakeAsyncRedis()

            async def close():
                closed["yes"] = True

            r.close = close
            return r

    monkeypatch.setattr(Worker, "run", fake_run)
    app = AsyncTaskQueue(redis_builder=FakeBuilder(), status_ttl=7, shutdown_timeout=9)
    app.distribute_qname = "mail"
    await aw.async_worker(asyncio.Event())
    assert seen == {"q": "mail", "ttl": 7, "timeout": 9}
    assert closed == {"yes": True}


@pytest.mark.asyncio
async def test_async_worker_prefers_aclose(monkeypatch, reset_async_task_queue):
    from async_q.async_q import AsyncTaskQueue
    calls = []

    class FakeBuilder:
        def get_redis_async(self):
            r = FakeAsyncRedis()

            async def aclose():
                calls.append("aclose")

            async def close():
                calls.append("close")

            r.aclose, r.close = aclose, close
            return r

    async def fake_run(self, stop_event):
        pass

    monkeypatch.setattr(Worker, "run", fake_run)
    AsyncTaskQueue(redis_builder=FakeBuilder())
    await aw.async_worker(asyncio.Event())
    assert calls == ["aclose"]
