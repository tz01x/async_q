"""Atomic multi-key queue operations as Lua scripts.

Every key a script touches belongs to one queue and carries the same {q} hash
tag, so each script runs in a single Redis Cluster slot.
"""
from .utils import (
    get_dead_letter_key,
    get_heartbeat_key,
    get_redis_q_backup_key,
    get_redis_q_key,
    get_retry_zset_key,
    get_workers_key,
)

# KEYS: hb, backup, main, registry  ARGV: wid
_RECLAIM = """
if redis.call('EXISTS', KEYS[1]) == 1 then return -1 end
local n = 0
while redis.call('RPOPLPUSH', KEYS[2], KEYS[3]) do n = n + 1 end
redis.call('SREM', KEYS[4], ARGV[1])
return n
"""

# KEYS: retry, main  ARGV: now, limit
_PROMOTE_DUE = """
local due = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1], 'LIMIT', 0, ARGV[2])
for _, v in ipairs(due) do
  redis.call('ZREM', KEYS[1], v)
  redis.call('RPUSH', KEYS[2], v)
end
return #due
"""

# KEYS: backup, main, hb, registry  ARGV: wid
_RELEASE = """
local n = 0
while redis.call('RPOPLPUSH', KEYS[1], KEYS[2]) do n = n + 1 end
redis.call('DEL', KEYS[3])
redis.call('SREM', KEYS[4], ARGV[1])
return n
"""

# KEYS: backup, main  ARGV: value
_REQUEUE = """
local n = redis.call('LREM', KEYS[1], 1, ARGV[1])
if n == 1 then redis.call('RPUSH', KEYS[2], ARGV[1]) end
return n
"""

# KEYS: backup, retry, dead  ARGV: original, new_payload, score, mode
_FAIL_ACK = """
local n = redis.call('LREM', KEYS[1], 1, ARGV[1])
if n == 1 then
  if ARGV[4] == 'retry' then
    redis.call('ZADD', KEYS[2], ARGV[3], ARGV[2])
  else
    redis.call('LPUSH', KEYS[3], ARGV[2])
  end
end
return n
"""


async def reclaim(redis, q: str, wid: str) -> int:
    """Return a dead worker's in-flight payloads to the main queue; -1 if it is alive."""
    return await redis.eval(
        _RECLAIM, 4,
        get_heartbeat_key(q, wid), get_redis_q_backup_key(q, wid), get_redis_q_key(q), get_workers_key(q),
        wid,
    )


async def promote_due(redis, q: str, now: float, limit: int = 100) -> int:
    """Move due retries to the front of the main queue."""
    return await redis.eval(_PROMOTE_DUE, 2, get_retry_zset_key(q), get_redis_q_key(q), now, limit)


async def release(redis, q: str, wid: str) -> int:
    """Graceful exit: requeue own backup, drop heartbeat and registry entry."""
    return await redis.eval(
        _RELEASE, 4,
        get_redis_q_backup_key(q, wid), get_redis_q_key(q), get_heartbeat_key(q, wid), get_workers_key(q),
        wid,
    )


async def requeue(redis, q: str, wid: str, value) -> int:
    """Atomically move one payload from a worker's backup to the front of main; 0 if it was not there."""
    return await redis.eval(_REQUEUE, 2, get_redis_q_backup_key(q, wid), get_redis_q_key(q), value)



async def fail_ack(redis, q: str, wid: str, original, new_payload, mode: str, score: float = 0) -> int:
    """Atomically move a failed payload from a worker's backup to retry (mode 'retry', at score)
    or the DLQ (mode 'dead'); 0 if it was not there (a peer reclaimed it) and nothing is written."""
    if mode not in ('retry', 'dead'):
        raise ValueError(f'unknown fail_ack mode: {mode!r}')
    return await redis.eval(
        _FAIL_ACK, 3,
        get_redis_q_backup_key(q, wid), get_retry_zset_key(q), get_dead_letter_key(q),
        original, new_payload, score, mode,
    )
