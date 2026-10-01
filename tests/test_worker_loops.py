import asyncio

import pytest

from fakes import FakeAsyncRedis, NoopLogger
from async_q import async_worker as aw
from async_q.async_worker import Worker
from async_q.utils import get_legacy_keys

Q = "default"


class RecordingLogger(NoopLogger):
    def __init__(self):
        self.warnings = []

    def warning(self, msg, *a, **k):
        self.warnings.append(msg % a if a else msg)


def _w(redis=None, **kw):
    return Worker(redis or FakeAsyncRedis(), Q, 1, kw.pop("logger", NoopLogger()), wid="me", **kw)


@pytest.mark.asyncio
async def test_beat_writes_heartbeat_with_ttl_and_registers():
    w = _w(heartbeat_interval=1, heartbeat_ttl=3)
    await w.beat()
    assert w.redis.ttl[w.hb_key] == 3
    assert await w.redis.smembers(w.workers_key) == {b"me"}


@pytest.mark.asyncio
async def test_reap_reclaims_every_peer_but_self(monkeypatch):
    calls = []

    async def fake_reclaim(redis, q, wid):
        calls.append(wid)
        return 0

    monkeypatch.setattr(aw.scripts, "reclaim", fake_reclaim)
    w = _w()
    for wid in ("me", "peer1", "peer2"):
        await w.redis.sadd(w.workers_key, wid)
    await w.reap()
    assert sorted(calls) == ["peer1", "peer2"]


@pytest.mark.asyncio
async def test_promote_calls_promote_due_with_now(monkeypatch):
    seen = {}

    async def fake_promote(redis, q, now, limit=100):
        seen.update(q=q, now=now)
        return 0

    monkeypatch.setattr(aw.scripts, "promote_due", fake_promote)
    await _w().promote()
    assert seen["q"] == Q and seen["now"] > 0


@pytest.mark.asyncio
async def test_every_survives_exceptions():
    logger = RecordingLogger()
    w = _w(logger=logger)
    calls = {"n": 0}

    async def flaky():
        calls["n"] += 1
        if calls["n"] == 1:
            raise RuntimeError("blip")

    t = asyncio.create_task(w._every(0.01, flaky, "thing"))
    await asyncio.sleep(0.1)
    t.cancel()
    await asyncio.gather(t, return_exceptions=True)
    assert calls["n"] >= 3
    assert any("thing" in m and "blip" in m for m in logger.warnings)


@pytest.mark.asyncio
async def test_migrate_legacy_startup_moves_all_old_keys_preserving_order():
    w = _w()
    old = get_legacy_keys(Q)
    for v in (b"m1", b"m2"):
        await w.redis.lpush(old["main"], v)          # m1 older than m2
    await w.redis.lpush(old["backup"], b"b1")
    await w.redis.lpush(old["dead"], b"d1")
    await w.redis.zadd(old["retry"], {b"r1": 42.0})
    assert await w.migrate_legacy(startup=True) == 5
    assert w.redis.lists[w.main_key][-1] == b"m1"     # oldest still fetched first
    assert set(w.redis.lists[w.main_key]) == {b"m1", b"m2", b"b1"}
    assert w.redis.lists[w.dead_key] == [b"d1"]
    assert w.redis.zsets[w.retry_key] == {b"r1": 42.0}
    assert not w.redis.lists[old["main"]] and not w.redis.zsets[old["retry"]]


@pytest.mark.asyncio
async def test_migrate_legacy_tick_leaves_old_backup_alone():
    w = _w()
    old = get_legacy_keys(Q)
    await w.redis.lpush(old["backup"], b"b1")
    await w.redis.lpush(old["main"], b"m1")
    assert await w.migrate_legacy(startup=False) == 1
    assert w.redis.lists[old["backup"]] == [b"b1"]
