import asyncio

import pytest

from async_q import scripts
from async_q.utils import (
    get_redis_q_key, get_redis_q_backup_key, get_retry_zset_key,
    get_heartbeat_key, get_workers_key, get_dead_letter_key,
)

Q = "itest"


@pytest.mark.asyncio
async def test_reclaim_skips_live_worker(real_redis):
    await real_redis.set(get_heartbeat_key(Q, "w1"), "1", px=30000)
    await real_redis.sadd(get_workers_key(Q), "w1")
    await real_redis.lpush(get_redis_q_backup_key(Q, "w1"), b"p1")
    assert await scripts.reclaim(real_redis, Q, "w1") == -1
    assert await real_redis.lrange(get_redis_q_backup_key(Q, "w1"), 0, -1) == [b"p1"]
    assert await real_redis.sismember(get_workers_key(Q), "w1")


@pytest.mark.asyncio
async def test_reclaim_moves_dead_workers_backup(real_redis):
    await real_redis.sadd(get_workers_key(Q), "w1")
    await real_redis.lpush(get_redis_q_backup_key(Q, "w1"), b"p1", b"p2")
    assert await scripts.reclaim(real_redis, Q, "w1") == 2
    assert sorted(await real_redis.lrange(get_redis_q_key(Q), 0, -1)) == [b"p1", b"p2"]
    assert not await real_redis.exists(get_redis_q_backup_key(Q, "w1"))
    assert not await real_redis.sismember(get_workers_key(Q), "w1")


@pytest.mark.asyncio
async def test_reclaim_of_dead_worker_with_empty_backup_returns_zero(real_redis):
    await real_redis.sadd(get_workers_key(Q), "w1")
    assert await scripts.reclaim(real_redis, Q, "w1") == 0
    assert not await real_redis.sismember(get_workers_key(Q), "w1")


@pytest.mark.asyncio
async def test_promote_due_moves_only_due_items_to_front(real_redis):
    await real_redis.lpush(get_redis_q_key(Q), b"queued")
    await real_redis.zadd(get_retry_zset_key(Q), {b"due1": 100, b"due2": 200, b"later": 10_000})
    assert await scripts.promote_due(real_redis, Q, now=500) == 2
    # front of the queue = right end (BRPOPLPUSH pops from the right)
    assert await real_redis.lrange(get_redis_q_key(Q), 0, -1) == [b"queued", b"due1", b"due2"]
    assert await real_redis.zrange(get_retry_zset_key(Q), 0, -1) == [b"later"]


@pytest.mark.asyncio
async def test_promote_due_never_double_promotes_under_concurrency(real_redis):
    await real_redis.zadd(get_retry_zset_key(Q), {f"p{i}".encode(): i for i in range(50)})
    counts = await asyncio.gather(*[scripts.promote_due(real_redis, Q, now=1000, limit=7) for _ in range(20)])
    assert sum(counts) == 50
    main = await real_redis.lrange(get_redis_q_key(Q), 0, -1)
    assert len(main) == len(set(main)) == 50


@pytest.mark.asyncio
async def test_release_returns_backup_and_unregisters(real_redis):
    await real_redis.set(get_heartbeat_key(Q, "w1"), "1", px=30000)
    await real_redis.sadd(get_workers_key(Q), "w1", "w2")
    await real_redis.lpush(get_redis_q_backup_key(Q, "w1"), b"p1")
    assert await scripts.release(real_redis, Q, "w1") == 1
    assert await real_redis.lrange(get_redis_q_key(Q), 0, -1) == [b"p1"]
    assert not await real_redis.exists(get_heartbeat_key(Q, "w1"))
    assert await real_redis.smembers(get_workers_key(Q)) == {b"w2"}


@pytest.mark.asyncio
async def test_requeue_moves_to_front_and_is_idempotent(real_redis):
    await real_redis.lpush(get_redis_q_key(Q), b"queued")
    await real_redis.lpush(get_redis_q_backup_key(Q, "w1"), b"p1")
    assert await scripts.requeue(real_redis, Q, "w1", b"p1") == 1
    assert await real_redis.lrange(get_redis_q_key(Q), 0, -1) == [b"queued", b"p1"]
    assert await scripts.requeue(real_redis, Q, "w1", b"p1") == 0
    assert await real_redis.lrange(get_redis_q_key(Q), 0, -1) == [b"queued", b"p1"]


@pytest.mark.asyncio
async def test_fail_ack_retry_mode_moves_backup_to_retry(real_redis):
    await real_redis.lpush(get_redis_q_backup_key(Q, "w1"), b"orig")
    assert await scripts.fail_ack(real_redis, Q, "w1", b"orig", b"bumped", "retry", 123.5) == 1
    assert await real_redis.lrange(get_redis_q_backup_key(Q, "w1"), 0, -1) == []
    assert await real_redis.zrange(get_retry_zset_key(Q), 0, -1, withscores=True) == [(b"bumped", 123.5)]
    assert not await real_redis.exists(get_dead_letter_key(Q))


@pytest.mark.asyncio
async def test_fail_ack_dead_mode_moves_backup_to_dlq(real_redis):
    await real_redis.lpush(get_redis_q_backup_key(Q, "w1"), b"orig")
    assert await scripts.fail_ack(real_redis, Q, "w1", b"orig", b"final", "dead") == 1
    assert await real_redis.lrange(get_redis_q_backup_key(Q, "w1"), 0, -1) == []
    assert await real_redis.lrange(get_dead_letter_key(Q), 0, -1) == [b"final"]
    assert not await real_redis.exists(get_retry_zset_key(Q))


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["retry", "dead"])
async def test_fail_ack_not_in_backup_writes_nothing(real_redis, mode):
    await real_redis.lpush(get_redis_q_backup_key(Q, "w1"), b"other")
    assert await scripts.fail_ack(real_redis, Q, "w1", b"orig", b"new", mode, 1) == 0
    assert await real_redis.lrange(get_redis_q_backup_key(Q, "w1"), 0, -1) == [b"other"]
    assert not await real_redis.exists(get_retry_zset_key(Q))
    assert not await real_redis.exists(get_dead_letter_key(Q))


@pytest.mark.asyncio
async def test_fail_ack_rejects_unknown_mode(real_redis):
    await real_redis.lpush(get_redis_q_backup_key(Q, "w1"), b"orig")
    with pytest.raises(ValueError):
        await scripts.fail_ack(real_redis, Q, "w1", b"orig", b"new", "bogus")
    assert await real_redis.lrange(get_redis_q_backup_key(Q, "w1"), 0, -1) == [b"orig"]
