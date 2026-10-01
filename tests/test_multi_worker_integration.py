import asyncio
import uuid

import pytest

from async_q.async_worker import Worker
from async_q.utils import TaskMetaData, serialize, get_function_ref, get_legacy_keys


class Log:
    def __init__(self):
        self.lines = []

    def _rec(self, msg, *a, **k):
        self.lines.append(msg % a if a else msg)

    debug = info = warning = error = _rec


def _q():
    return "it-" + uuid.uuid4().hex[:8]


def _module(tmp_path, body):
    get_function_ref.cache_clear()
    mod = tmp_path / ("jobs_" + uuid.uuid4().hex[:6] + ".py")
    mod.write_text(body)
    return str(mod)


def _payload(path, tid, func="job", args=None, **kw):
    return serialize(TaskMetaData(id=tid, path=path, func_name=func, args=args or [], kwargs={},
                                  status="submitted", **kw))


def _fast(redis, q, wid, **kw):
    return Worker(redis, q, kw.pop("concurrency", 5), Log(), wid=wid,
                  heartbeat_interval=0.1, heartbeat_ttl=0.3, shutdown_timeout=kw.pop("shutdown_timeout", 2), **kw)


async def _until(pred, timeout=8.0):
    for _ in range(int(timeout / 0.05)):
        if pred():
            return True
        await asyncio.sleep(0.05)
    return False


RECORD = (
    "import asyncio\n"
    "async def job(out, delay=0, task_id=None):\n"
    "    await asyncio.sleep(delay)\n"
    "    with open(out, 'a') as f:\n"
    "        f.write('x')\n"
)


@pytest.mark.asyncio
async def test_dead_worker_tasks_rerun_by_peer(real_redis, tmp_path):
    q, out = _q(), tmp_path / "out.txt"
    path = _module(tmp_path, RECORD)
    dead = _fast(real_redis, q, "dead")
    await dead.beat()                                        # registered, heartbeat ttl 0.3s
    await real_redis.lpush(dead.backup_key, _payload(path, "t1", args=[str(out)]))
    await asyncio.sleep(0.5)                                 # heartbeat expires; never released

    peer = _fast(real_redis, q, "peer")
    stop = asyncio.Event()
    run = asyncio.create_task(peer.run(stop))
    assert await _until(lambda: out.exists())
    stop.set()
    await run
    assert out.read_text() == "x"
    assert not await real_redis.exists(dead.backup_key)
    assert not await real_redis.sismember(peer.workers_key, "dead")


@pytest.mark.asyncio
async def test_graceful_stop_does_not_touch_peer_inflight(real_redis, tmp_path):
    q = _q()
    alive = Worker(real_redis, q, 1, Log(), wid="alive")     # default 30s heartbeat
    await alive.beat()
    await real_redis.lpush(alive.backup_key, b"in-flight-elsewhere")

    other = _fast(real_redis, q, "other")
    stop = asyncio.Event()
    run = asyncio.create_task(other.run(stop))
    await asyncio.sleep(0.5)                                 # several reaper ticks
    stop.set()
    await run
    assert await real_redis.lrange(alive.backup_key, 0, -1) == [b"in-flight-elsewhere"]
    assert await real_redis.llen(other.main_key) == 0


@pytest.mark.asyncio
async def test_long_task_on_live_worker_is_not_reclaimed(real_redis, tmp_path):
    q, out = _q(), tmp_path / "out.txt"
    path = _module(tmp_path, RECORD)
    a, b = _fast(real_redis, q, "a"), _fast(real_redis, q, "b")
    await real_redis.lpush(a.main_key, _payload(path, "long", args=[str(out), 1.0]))  # 1s > 0.3s ttl
    stop_a, stop_b = asyncio.Event(), asyncio.Event()
    run_a = asyncio.create_task(a.run(stop_a))
    assert await _until(lambda: a.inflight)
    run_b = asyncio.create_task(b.run(stop_b))
    assert await _until(lambda: out.exists())
    await asyncio.sleep(0.5)
    stop_a.set(); stop_b.set()
    await asyncio.gather(run_a, run_b)
    assert out.read_text() == "x"


@pytest.mark.asyncio
async def test_failing_task_retries_then_lands_in_dlq(real_redis, tmp_path):
    q = _q()
    path = _module(tmp_path, "runs = []\nasync def job(task_id=None):\n    runs.append(1)\n    raise RuntimeError('boom')\n")
    w = _fast(real_redis, q, "w")
    await real_redis.lpush(w.main_key, _payload(path, "t", max_retries=1, backoff_base=0.2))
    stop = asyncio.Event()
    run = asyncio.create_task(w.run(stop))
    assert await _until(lambda: get_function_ref(path, "job").__globals__["runs"] == [1, 1])
    await asyncio.sleep(0.3)
    stop.set()
    await run
    assert await real_redis.llen(w.dead_key) == 1
    assert await real_redis.zcard(w.retry_key) == 0


@pytest.mark.asyncio
async def test_legacy_keys_drained_on_start(real_redis, tmp_path):
    q, out = _q(), tmp_path / "out.txt"
    path = _module(tmp_path, RECORD)
    await real_redis.lpush(get_legacy_keys(q)["main"], _payload(path, "old", args=[str(out)]))
    w = _fast(real_redis, q, "w")
    stop = asyncio.Event()
    run = asyncio.create_task(w.run(stop))
    assert await _until(lambda: out.exists())
    stop.set()
    await run
    assert not await real_redis.exists(get_legacy_keys(q)["main"])
