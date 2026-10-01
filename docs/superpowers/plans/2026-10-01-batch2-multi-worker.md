# Batch 2 Multi-Worker Correctness Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make N async_q workers safe on one queue (Redis Cluster included, long tasks included): per-worker backup lists, heartbeat-based reclaim, atomic retry promotion, graceful drain, status TTLs.

**Architecture:** Replace the producer/consumer pair and `AsyncQueue` in `async_worker.py` with a `Worker` class (one fetch loop gated by `asyncio.Semaphore`, plus heartbeat/reaper/promoter loops). Atomic multi-key operations live as Lua in a new `scripts.py`. All per-queue keys carry a `{q}` Cluster hash tag; legacy keys are drained for one release.

**Tech Stack:** Python ≥3.8, asyncio, redis-py 5.0 (`redis.asyncio`, `RedisCluster`, `EVAL`), msgpack, pytest + pytest-asyncio 1.x.

**Spec:** `docs/superpowers/specs/2026-10-01-batch2-multi-worker-design.md`

## Global Constraints

- `requires-python >=3.8`: no `X | Y`, no `match`, no `set[str]`/`list[int]` annotations, no `asyncio.to_thread`, no `asyncio.timeout`.
- No new runtime or test dependencies.
- Key names exactly as in the spec table (`async_task:{q}`, `async_task_backup:{q}:<wid>`, `async_task_retry:{q}`, `async_task_dead:{q}`, `async_task_hb:{q}:<wid>`, `async_task_workers:{q}`, `async-q-task:<id>`).
- Defaults: `status_ttl=86400`, `heartbeat_interval=10`, `heartbeat_ttl=30`, `shutdown_timeout=30`, promoter every 1s, `promote_due` limit 100, fetch backoff 1s doubling to 30s.
- `Worker` must be constructed inside a running event loop (it creates an `asyncio.Semaphore`; on 3.8/3.9 that binds the current loop).
- Unit tests: `python -m pytest -q`. Integration tests need `ASYNC_Q_TEST_REDIS_URL` (see Task 0); they skip otherwise.
- After each task, check off its items in `IMPROVEMENTS.md` where noted.
- Commit messages end with `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`.
- Files on disk may be CRLF in the working tree. When editing with scripts, preserve line endings and open files as UTF-8 (`PYTHONUTF8=1`).

## Review Focus

1. Redis unreachable for longer than `heartbeat_ttl` while tasks run → peers reclaim; when the original worker's task finishes, the late ack must not schedule a retry or DLQ entry (that would duplicate). Test: Task 4 `test_late_ack_after_reclaim_does_not_retry`.
2. Stop requested while every concurrency slot is busy (fetch loop parked in `sem.acquire()`) → shutdown must not hang. Test: Task 7 `test_stop_with_all_slots_busy_requeues_and_exits`.
3. A task that swallows `CancelledError` → drain gives up after a second `shutdown_timeout`, worker still exits. Test: Task 7 `test_task_ignoring_cancel_does_not_hang_shutdown`.
4. Redis down at shutdown (`release` raises) → worker logs and exits, no exception escapes `run`. Test: Task 7 `test_release_failure_is_logged_not_raised`.
5. Worker killed between `BRPOPLPUSH` and task start → payload sits in its backup; a peer reclaims it after the heartbeat expires and runs it. Test: Task 7 integration `test_dead_worker_tasks_rerun_by_peer`.

---

### Task 0: Branch + local Redis

Branch `feat/batch2-multi-worker` already exists (stacked on `fix/batch1-quick-fixes`) and holds the spec commit.

- [ ] **Step 1:** `git branch --show-current` → Expected: `feat/batch2-multi-worker`.
- [ ] **Step 2:** Start a throwaway Redis: `docker run -d --rm --name async-q-test-redis -p 6390:6379 redis:7` → Expected: container id printed.
- [ ] **Step 3:** In every later test command, integration tests run with `ASYNC_Q_TEST_REDIS_URL=redis://localhost:6390/15`. Baseline: `python -m pytest -q` → Expected: `27 passed`.
- [ ] **Step 4:** Commit this plan:

```bash
git add docs/superpowers/plans/2026-10-01-batch2-multi-worker.md
git commit -m "docs: batch 2 implementation plan"
```

---

### Task 1: Hash-tagged keys, legacy keys, worker id

**Files:**
- Modify: `src/async_q/utils.py` (key builders)
- Modify: `tests/test_utils_misc.py` (`test_key_builders`)
- Test: `tests/test_keys.py` (create)

**Interfaces:**
- Produces (all in `async_q.utils`):
  - `get_redis_q_key(q: str = '') -> str` → `'async_task:{' + q + '}'`
  - `get_redis_q_backup_key(q: str = '', wid: str = '') -> str` → `'async_task_backup:{' + q + '}:' + wid`
  - `get_retry_zset_key(q: str = '') -> str` → `'async_task_retry:{' + q + '}'`
  - `get_dead_letter_key(q: str = '') -> str` → `'async_task_dead:{' + q + '}'`
  - `get_heartbeat_key(q: str, wid: str) -> str` → `'async_task_hb:{' + q + '}:' + wid`
  - `get_workers_key(q: str) -> str` → `'async_task_workers:{' + q + '}'`
  - `get_legacy_keys(q: str) -> Dict[str, str]` with keys `main`, `backup`, `retry`, `dead` → `'async_task:' + q`, `'async_task_backup:' + q`, `'async_task_retry:' + q`, `'async_task_dead:' + q`
  - `new_worker_id() -> str` → `f'{hostname}-{pid}-{6 hex}'`
  - `get_task_key` unchanged.

- [ ] **Step 1: Failing tests** — create `tests/test_keys.py`:

```python
import os
import re
import socket

from redis.crc import key_slot

from async_q.utils import (
    get_redis_q_key, get_redis_q_backup_key, get_retry_zset_key, get_dead_letter_key,
    get_heartbeat_key, get_workers_key, get_legacy_keys, new_worker_id,
)


def test_queue_keys_are_hash_tagged():
    assert get_redis_q_key("mail") == "async_task:{mail}"
    assert get_redis_q_backup_key("mail", "w1") == "async_task_backup:{mail}:w1"
    assert get_retry_zset_key("mail") == "async_task_retry:{mail}"
    assert get_dead_letter_key("mail") == "async_task_dead:{mail}"
    assert get_heartbeat_key("mail", "w1") == "async_task_hb:{mail}:w1"
    assert get_workers_key("mail") == "async_task_workers:{mail}"


def test_all_queue_keys_share_one_cluster_slot():
    keys = [get_redis_q_key("mail"), get_redis_q_backup_key("mail", "w1"), get_retry_zset_key("mail"),
            get_dead_letter_key("mail"), get_heartbeat_key("mail", "w1"), get_workers_key("mail")]
    assert len({key_slot(k.encode()) for k in keys}) == 1


def test_legacy_keys_match_pre_0_3_names():
    assert get_legacy_keys("mail") == {
        "main": "async_task:mail",
        "backup": "async_task_backup:mail",
        "retry": "async_task_retry:mail",
        "dead": "async_task_dead:mail",
    }


def test_worker_id_is_readable_and_unique():
    a, b = new_worker_id(), new_worker_id()
    assert a != b
    assert re.fullmatch(re.escape(socket.gethostname()) + "-" + str(os.getpid()) + "-[0-9a-f]{6}", a)
```

In `tests/test_utils_misc.py`, change `test_key_builders` to:

```python
def test_key_builders():
    assert get_redis_q_key("default") == "async_task:{default}"
    assert get_redis_q_backup_key("default", "w") == "async_task_backup:{default}:w"
    assert get_task_key("123") == "async-q-task:123"
```

- [ ] **Step 2:** `python -m pytest tests/test_keys.py tests/test_utils_misc.py -q` → Expected: FAIL (`ImportError: cannot import name 'get_heartbeat_key'`).

- [ ] **Step 3: Implement** — in `src/async_q/utils.py`, add imports `import os`, `import secrets`, `import socket`, extend `typing` import with `Dict`, and replace the five key builders (`get_redis_q_key` … `get_dead_letter_key`, keeping `get_task_key` as is) with:

```python
# Per-queue keys carry a {q} hash tag so every key of one queue lands in the
# same Redis Cluster slot (required for BRPOPLPUSH and the Lua scripts).
def get_redis_q_key(extra: str = ''):
    return 'async_task:{' + extra + '}'

def get_redis_q_backup_key(extra: str = '', wid: str = ''):
    return 'async_task_backup:{' + extra + '}:' + wid

def get_task_key(extra:str=''):
    return 'async-q-task:'+extra

def get_retry_zset_key(extra: str = ''):
    return 'async_task_retry:{' + extra + '}'

def get_dead_letter_key(extra: str = ''):
    return 'async_task_dead:{' + extra + '}'

def get_heartbeat_key(q: str, wid: str):
    return 'async_task_hb:{' + q + '}:' + wid

def get_workers_key(q: str):
    return 'async_task_workers:{' + q + '}'

def get_legacy_keys(q: str) -> Dict[str, str]:
    """Pre-0.3.0 key names. # ponytail: drain support for one release, then delete"""
    return {
        'main': 'async_task:' + q,
        'backup': 'async_task_backup:' + q,
        'retry': 'async_task_retry:' + q,
        'dead': 'async_task_dead:' + q,
    }

def new_worker_id() -> str:
    return f'{socket.gethostname()}-{os.getpid()}-{secrets.token_hex(3)}'
```

- [ ] **Step 4:** `python -m pytest -q` → Expected: all pass. (Existing tests that build keys via the helpers keep passing; `test_retry_and_dlq.py` / `test_process_message.py` use the helpers with one arg for the backup key, which now yields `async_task_backup:{default}:` — still self-consistent.)
- [ ] **Step 5:** Commit:

```bash
git add src/async_q/utils.py tests/test_keys.py tests/test_utils_misc.py
git commit -m "feat: hash-tag per-queue Redis keys for Cluster, add worker id and legacy key names"
```

---

### Task 2: Config on `AsyncTaskQueue` + Cluster client on `RedisBuilder`

**Files:**
- Modify: `src/async_q/async_q.py` (`RedisBuilder`, `AsyncTaskQueue.__init__`)
- Test: `tests/test_async_q_core.py` (append)

**Interfaces:**
- Produces: `RedisBuilder(host='localhost', port=6379, username=None, password=None, cluster: bool = False, **kwargs)`; `get_redis()` returns `redis.RedisCluster` when `cluster`, else `redis.Redis`; `get_redis_async()` likewise `redis.asyncio.RedisCluster` / `redis.asyncio.Redis`.
- Produces: `AsyncTaskQueue(redis_builder, *, status_ttl=86400, heartbeat_interval=10.0, heartbeat_ttl=30.0, shutdown_timeout=30.0)` storing each as a same-named public attribute. Raises `ValueError` if any ≤ 0 or `heartbeat_ttl < 2 * heartbeat_interval`.

- [ ] **Step 1: Failing tests** (append to `tests/test_async_q_core.py`):

```python
def test_async_task_queue_defaults(reset_async_task_queue):
    app = AsyncTaskQueue(redis_builder=RedisBuilder())
    assert (app.status_ttl, app.heartbeat_interval, app.heartbeat_ttl, app.shutdown_timeout) == (86400, 10.0, 30.0, 30.0)


@pytest.mark.parametrize("kwargs", [
    {"status_ttl": 0},
    {"heartbeat_interval": 0},
    {"shutdown_timeout": -1},
    {"heartbeat_interval": 10, "heartbeat_ttl": 15},
])
def test_async_task_queue_rejects_bad_config(kwargs, reset_async_task_queue):
    with pytest.raises(ValueError):
        AsyncTaskQueue(redis_builder=RedisBuilder(), **kwargs)


def test_redis_builder_cluster_selects_cluster_clients(monkeypatch):
    import async_q.async_q as aq
    made = []

    class Fake:
        def __init__(self, **kw):
            made.append((type(self).__name__, kw))

    class RedisCluster(Fake):
        pass

    class AioRedisCluster(Fake):
        pass

    monkeypatch.setattr(aq.redis, "RedisCluster", RedisCluster)
    monkeypatch.setattr(aq.aioredis, "RedisCluster", AioRedisCluster)
    rb = RedisBuilder(host="h", port=7000, cluster=True)
    rb.get_redis()
    rb.get_redis_async()
    assert [m[0] for m in made] == ["RedisCluster", "AioRedisCluster"]
    assert made[0][1]["host"] == "h" and made[0][1]["port"] == 7000
    assert "cluster" not in made[0][1]
```

- [ ] **Step 2:** `python -m pytest tests/test_async_q_core.py -q` → Expected: FAIL (`AttributeError: 'AsyncTaskQueue' object has no attribute 'status_ttl'` / `TypeError: ... unexpected keyword argument`).

- [ ] **Step 3: Implement** — `RedisBuilder`:

```python
class RedisBuilder:
    __redis_sync = None
    __redis_async = None

    def __init__(self, host: str = 'localhost', port: int = 6379, username: Optional[str] = None, password: Optional[str] = None, cluster: bool = False, **kwargs):
        self.cluster = cluster
        self.kwargs = {
            'host': host,
            'port': port,
            'password': password,
            'username': username,
            **kwargs
        }

    def get_redis(self) -> redis.Redis:
        if not self.__redis_sync:
            cls = redis.RedisCluster if self.cluster else redis.Redis
            self.__redis_sync = cls(**self.kwargs)
        return self.__redis_sync

    def get_redis_async(self) -> aioredis.Redis:
        if not self.__redis_async:
            cls = aioredis.RedisCluster if self.cluster else aioredis.Redis
            self.__redis_async = cls(**self.kwargs)
        return self.__redis_async
```

`AsyncTaskQueue.__init__`:

```python
    def __init__(
        self,
        redis_builder: RedisBuilder,
        *,
        status_ttl: int = 86400,
        heartbeat_interval: float = 10.0,
        heartbeat_ttl: float = 30.0,
        shutdown_timeout: float = 30.0,
    ):
        for name, value in (('status_ttl', status_ttl), ('heartbeat_interval', heartbeat_interval),
                            ('heartbeat_ttl', heartbeat_ttl), ('shutdown_timeout', shutdown_timeout)):
            if value <= 0:
                raise ValueError(f'{name} must be > 0')
        if heartbeat_ttl < 2 * heartbeat_interval:
            raise ValueError('heartbeat_ttl must be at least 2 * heartbeat_interval')

        self.distribute_qname = 'default'
        self.redis_builder = redis_builder
        self._concurrency = 100
        self.logger = logging.getLogger('async-q')
        self.status_ttl = status_ttl
        self.heartbeat_interval = heartbeat_interval
        self.heartbeat_ttl = heartbeat_ttl
        self.shutdown_timeout = shutdown_timeout

        if not AsyncTaskQueue._instance:
            AsyncTaskQueue._instance = self
```

- [ ] **Step 4:** `python -m pytest -q` → Expected: all pass.
- [ ] **Step 5:** Commit `feat: worker timing config on AsyncTaskQueue and Redis Cluster client option`.

---

### Task 3: Lua scripts (`reclaim`, `promote_due`, `release`) + integration fixture

**Files:**
- Create: `src/async_q/scripts.py`
- Modify: `tests/conftest.py` (add `real_redis` fixture)
- Test: `tests/test_scripts_integration.py` (create)

**Interfaces:**
- Produces (`async_q.scripts`):
  - `async def reclaim(redis, q: str, wid: str) -> int` — `-1` if worker `wid`'s heartbeat exists (alive, untouched); else moves its whole backup to main, removes it from the registry, returns count moved (≥0).
  - `async def promote_due(redis, q: str, now: float, limit: int = 100) -> int` — moves retry entries with score ≤ `now` (oldest first, up to `limit`) to the **front** of main (`RPUSH`, so they're fetched next); returns count.
  - `async def release(redis, q: str, wid: str) -> int` — moves own backup to main, deletes heartbeat, removes from registry; returns count moved.
- Produces (tests): `real_redis` async fixture — a `redis.asyncio.Redis` on `ASYNC_Q_TEST_REDIS_URL` with the DB flushed before and after; skips the test when the env var is unset.

- [ ] **Step 1: Fixture** — append to `tests/conftest.py`:

```python
import pytest_asyncio


@pytest_asyncio.fixture
async def real_redis():
    url = os.getenv("ASYNC_Q_TEST_REDIS_URL")
    if not url:
        pytest.skip("set ASYNC_Q_TEST_REDIS_URL to run Redis integration tests")
    import redis.asyncio as aioredis
    r = aioredis.Redis.from_url(url)
    await r.flushdb()
    yield r
    await r.flushdb()
    await r.close()
```

- [ ] **Step 2: Failing tests** — create `tests/test_scripts_integration.py`:

```python
import asyncio

import pytest

from async_q import scripts
from async_q.utils import (
    get_redis_q_key, get_redis_q_backup_key, get_retry_zset_key,
    get_heartbeat_key, get_workers_key,
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
```

- [ ] **Step 3:** `ASYNC_Q_TEST_REDIS_URL=redis://localhost:6390/15 python -m pytest tests/test_scripts_integration.py -q` → Expected: FAIL (`ImportError: cannot import name 'scripts'`). Also `python -m pytest tests/test_scripts_integration.py -q` without the env var → Expected: collection error now; after Step 4, `6 skipped`.

- [ ] **Step 4: Implement** — create `src/async_q/scripts.py`:

```python
"""Atomic multi-key queue operations as Lua scripts.

Every key a script touches belongs to one queue and carries the same {q} hash
tag, so each script runs in a single Redis Cluster slot.
"""
from .utils import (
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
```

- [ ] **Step 5:** `ASYNC_Q_TEST_REDIS_URL=redis://localhost:6390/15 python -m pytest -q` → Expected: all pass (unit + 6 integration). `python -m pytest -q` → Expected: unit pass, 6 skipped.
- [ ] **Step 6:** Commit `feat: Lua scripts for atomic reclaim, retry promotion and release`.

---

### Task 4: `Worker` core — message handling and acks (B10 TTLs, own backup, late-ack safety)

**Files:**
- Modify: `src/async_q/async_worker.py` (add `Worker` class with core methods; module-level `_task_done`, `_reject`, `_process_message`, `_task_inner`, `task_done_callback`, `_background_tasks` are deleted in Task 5 — leave them in place for now)
- Modify: `tests/fakes.py` (extend `FakeAsyncRedis`)
- Modify: `tests/test_process_message.py`, `tests/test_retry_and_dlq.py` (port to `Worker`)
- Test: `tests/test_worker_core.py` (create)

**Interfaces:**
- Consumes: Task 1 key builders + `new_worker_id`.
- Produces (`async_q.async_worker.Worker`):
  - `Worker(redis, queue_name: str, concurrency: int, logger, *, status_ttl=86400, heartbeat_interval=10.0, heartbeat_ttl=30.0, shutdown_timeout=30.0, wid: Optional[str] = None)`
  - attributes: `redis`, `q`, `wid`, `main_key`, `backup_key`, `hb_key`, `workers_key`, `retry_key`, `dead_key`, `sem` (`asyncio.Semaphore(concurrency)`), `inflight: set`, `background: set`, plus the config values.
  - `async set_status(meta: TaskMetaData, status: str) -> None` — sets `meta.status`, writes `async-q-task:<id>` with `ex=status_ttl`.
  - `async reject(value: bytes, reason: str, meta: Optional[TaskMetaData] = None) -> None`
  - `async process_message(value: bytes) -> Optional[QueueItem]` — `None` if rejected.
  - `async run_task(item: QueueItem) -> QueueItem`
  - `async task_done(task, item: QueueItem) -> None` — ack/retry/DLQ/requeue; always `sem.release()` in `finally`.
- Produces (tests): `FakeAsyncRedis` gains `set(key, value, ex=None, px=None)` recording `self.ttl[key]` (seconds), `rpush`, `rpop`, `brpoplpush`, `sadd`, `srem`, `smembers`, `exists`, `delete`, `zrange(key, start, end, withscores=False)`; `lrem` keeps `(key, count, value)`.

- [ ] **Step 1: Extend fakes** — in `tests/fakes.py`, inside `FakeAsyncRedis`: in `__init__` add `self.sets = {}` and `self.ttl = {}`; replace `set` and add the new methods:

```python
    async def set(self, key, value, ex=None, px=None):
        self.kv[key] = value
        self.ttl[key] = ex if ex is not None else (px / 1000 if px is not None else None)
        return True

    async def exists(self, key):
        return int(key in self.kv or bool(self.lists.get(key)) or bool(self.sets.get(key)))

    async def delete(self, key):
        self.kv.pop(key, None)
        self.lists.pop(key, None)
        self.sets.pop(key, None)

    async def rpush(self, key, value):
        self.lists.setdefault(key, []).append(value)
        return len(self.lists[key])

    async def rpop(self, key):
        lst = self.lists.get(key, [])
        return lst.pop() if lst else None

    async def brpoplpush(self, src, dst, timeout=0):
        val = await self.rpoplpush(src, dst)
        if val is None:
            await asyncio.sleep(0.01)  # ponytail: don't really block; just yield
        return val

    async def sadd(self, key, member):
        self.sets.setdefault(key, set()).add(member)

    async def srem(self, key, member):
        self.sets.get(key, set()).discard(member)

    async def smembers(self, key):
        return {m.encode() if isinstance(m, str) else m for m in self.sets.get(key, set())}

    async def zrange(self, key, start, end, withscores=False):
        items = sorted(self.zsets.get(key, {}).items(), key=lambda x: x[1])
        items = items[start:] if end == -1 else items[start:end + 1]
        return items if withscores else [m for m, _ in items]
```

Add `import asyncio` at the top of `tests/fakes.py`. Run `python -m pytest -q` → Expected: still all pass.

- [ ] **Step 2: Failing tests** — create `tests/test_worker_core.py`:

```python
import asyncio
import time

import pytest

from fakes import FakeAsyncRedis, FakeTask, NoopLogger
from async_q.async_worker import Worker
from async_q.utils import QueueItem, TaskMetaData, deserialize, serialize, get_task_key, get_function_ref

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
```

- [ ] **Step 3:** `python -m pytest tests/test_worker_core.py -q` → Expected: FAIL (`ImportError: cannot import name 'Worker'`).

- [ ] **Step 4: Implement** — in `src/async_q/async_worker.py`: add `import time` to the imports; extend the `.utils` import with `get_heartbeat_key, get_workers_key, new_worker_id`. Append the class at the end of the module:

```python
class Worker:
    """One worker process on one queue. Construct inside a running event loop."""

    def __init__(self, redis, queue_name: str, concurrency: int, logger, *,
                 status_ttl: int = 86400, heartbeat_interval: float = 10.0,
                 heartbeat_ttl: float = 30.0, shutdown_timeout: float = 30.0,
                 wid: Optional[str] = None):
        self.redis = redis
        self.q = queue_name
        self.logger = logger
        self.status_ttl = status_ttl
        self.heartbeat_interval = heartbeat_interval
        self.heartbeat_ttl = heartbeat_ttl
        self.shutdown_timeout = shutdown_timeout
        self.wid = wid or new_worker_id()
        self.main_key = get_redis_q_key(queue_name)
        self.backup_key = get_redis_q_backup_key(queue_name, self.wid)
        self.hb_key = get_heartbeat_key(queue_name, self.wid)
        self.workers_key = get_workers_key(queue_name)
        self.retry_key = get_retry_zset_key(queue_name)
        self.dead_key = get_dead_letter_key(queue_name)
        self.sem = asyncio.Semaphore(concurrency)
        self.inflight: set = set()
        self.background: set = set()  # strong refs: done-handlers must not be GC'd mid-flight

    async def set_status(self, meta: TaskMetaData, status: str) -> None:
        meta.status = status
        await self.redis.set(get_task_key(meta.id), serialize(meta), ex=self.status_ttl)

    async def reject(self, value, reason: str, meta: Optional[TaskMetaData] = None) -> None:
        """Move an unprocessable payload from backup to the DLQ so it can't crash the worker again."""
        self.logger.error('rejecting task to DLQ: %s', reason)
        await self.redis.lrem(self.backup_key, 1, value)
        await self.redis.lpush(self.dead_key, value)
        if meta is not None:
            await self.set_status(meta, 'failed')

    async def process_message(self, value) -> Optional[QueueItem]:
        try:
            data = deserialize(value)
        except Exception as e:
            await self.reject(value, f'undecodable payload: {e!r}')
            return None

        self.logger.debug("deserialize received value : %s", data)

        try:
            fun = await to_thread(get_function_ref, data.path, data.func_name)
        except Exception as e:
            await self.reject(value, f'cannot load {data.path}:{data.func_name}: {e!r}', data)
            return None

        if not inspect.iscoroutinefunction(fun):
            await self.reject(value, f'{data.func_name} is not a coroutine function', data)
            return None

        await self.set_status(data, 'pending')
        return QueueItem(func_ref=fun, deserialize_data=data, original_data=value)

    async def run_task(self, item: QueueItem) -> QueueItem:
        meta = copy.deepcopy(item.deserialize_data)
        await self.set_status(meta, 'starting')
        tkey = get_task_key(meta.id)
        self.logger.debug('starting a new task; task_id: %s', tkey)
        await item.func_ref(*meta.args, **{**meta.kwargs, 'task_id': tkey})
        return item

    async def task_done(self, task, item: QueueItem) -> None:
        meta = copy.deepcopy(item.deserialize_data)
        try:
            try:
                task.result()
            except asyncio.CancelledError:
                # Front of the queue: it was already running, re-run it first.
                if await self.redis.lrem(self.backup_key, 1, item.original_data):
                    await self.redis.rpush(self.main_key, item.original_data)
                await self.set_status(meta, 'submitted')
                self.logger.info('task %s cancelled; re-queued', meta.id)
                return
            except Exception as e:
                self.logger.error('task %s failed: %r', meta.id, e)
                self.logger.debug('traceback', exc_info=e)
                if not await self._ack(item, meta):
                    return
                if meta.attempt < meta.max_retries:
                    meta.attempt += 1
                    delay = min(meta.backoff_base * (meta.backoff_factor ** (meta.attempt - 1)), meta.backoff_max)
                    meta.next_retry_at = time.time() + delay
                    await self.set_status(meta, 'failed')
                    # Re-serialize so the bumped attempt count travels with the payload
                    await self.redis.zadd(self.retry_key, {serialize(meta): meta.next_retry_at})
                    self.logger.info('scheduled retry #%s in %.2fs for %s', meta.attempt, delay, meta.id)
                else:
                    await self.set_status(meta, 'failed')
                    await self.redis.lpush(self.dead_key, serialize(meta))
                    self.logger.warning('moved task %s to DLQ after %s attempts', meta.id, meta.attempt)
                return
            if await self._ack(item, meta):
                await self.set_status(meta, 'finished')
        finally:
            self.sem.release()

    async def _ack(self, item: QueueItem, meta: TaskMetaData) -> bool:
        """Remove from own backup. False means a peer reclaimed it (task may run twice)."""
        if await self.redis.lrem(self.backup_key, 1, item.original_data):
            return True
        self.logger.warning('task %s was reclaimed by another worker while running; it may run twice', meta.id)
        return False
```

- [ ] **Step 5:** `python -m pytest tests/test_worker_core.py -q` → Expected: PASS.

- [ ] **Step 6: Port Batch 1 tests to `Worker`** (they test the same behavior; the module functions are deleted in Task 5):
  - `tests/test_process_message.py`: replace the import `from async_q.async_worker import _process_message` with `from async_q.async_worker import Worker`; replace `_run` with:

```python
async def _run(payload, redis=None):
    w = Worker(redis or FakeAsyncRedis(), Q, 1, NoopLogger(), wid="w1")
    await w.redis.lpush(w.backup_key, payload)  # as brpoplpush would
    item = await w.process_message(payload)
    return w, item
```

  and update each test body: `redis, queue = await _run(...)` → `w, item = await _run(...)`; `queue.empty()` → `item is None`; `item = queue.get_nowait()` → (use `item` directly); `redis.lists[get_redis_q_backup_key(Q)]` → `w.redis.lists[w.backup_key]`; `redis.lists[get_dead_letter_key(Q)]` → `w.redis.lists[w.dead_key]`; `redis.kv[...]` → `w.redis.kv[...]`. Remove now-unused imports (`asyncio`, `get_redis_q_backup_key`, `get_dead_letter_key`).
  - `tests/test_retry_and_dlq.py`: delete `test_task_done_schedules_retry_when_under_limit`, `test_task_done_moves_to_dlq_when_exhausted` and `test_task_done_callback_holds_reference_until_done` (covered by `test_worker_core.py`). Port `test_failing_task_reaches_dlq_after_max_retries` to:

```python
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
```

  Remove unused imports from that file afterwards.
- [ ] **Step 7:** `python -m pytest -q` → Expected: all pass (integration skipped without env).
- [ ] **Step 8:** Check B10 in `IMPROVEMENTS.md`. Commit `feat: Worker core with per-worker backup acks, status TTLs and late-ack safety`.

---

### Task 5: Fetch loop with semaphore; delete `AsyncQueue` and legacy pipeline (D3)

**Files:**
- Modify: `src/async_q/async_worker.py` (add `fetch_loop`, `start_task`, `_on_done`, `_next_backoff`; delete `AsyncQueue`, `_task_done`, `_background_tasks`, `task_done_callback`, `_task_inner`, `create_async_task_from_queue`, `_reject`, `_process_message`, `listen_to_submitted_task`; drop the unused `re`, `logging`, `traceback`, `Any`, `Dict` imports)
- Modify: `tests/test_async_worker_queue.py` (delete the two `AsyncQueue` tests and the `AsyncQueue` import; keep `test_async_worker_requires_initialized_app`)
- Test: `tests/test_worker_fetch.py` (create)

**Interfaces:**
- Consumes: Task 4 `Worker`.
- Produces: `Worker.fetch_loop()` (runs until cancelled), `Worker.start_task(item)`, module function `_next_backoff(current: float) -> float` (`min(current * 2, 30)`).
- `async_worker()` is rewired in Task 7; between Task 5 and Task 7 it is broken — no test exercises it beyond the `RuntimeError` check, which still passes because the check runs first. Ruling recorded in plan: acceptable mid-branch state.

- [ ] **Step 1: Failing tests** — create `tests/test_worker_fetch.py`:

```python
import asyncio

import pytest
from redis.exceptions import ConnectionError as RedisConnectionError

from fakes import FakeAsyncRedis, NoopLogger
from async_q.async_worker import Worker, _next_backoff
from async_q.utils import TaskMetaData, serialize, get_function_ref

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
    w = Worker(redis, Q, 1, NoopLogger(), wid="w1")
    await redis.lpush(w.main_key, _payload(path, "ok"))
    loop_task = asyncio.create_task(w.fetch_loop())
    jobs = get_function_ref(path, "job").__globals__
    assert await _until(lambda: jobs["ran"] == [1], timeout=3.0)  # after one 1s backoff
    assert not w.sem.locked()
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
```

- [ ] **Step 2:** `python -m pytest tests/test_worker_fetch.py -q` → Expected: FAIL (`ImportError: cannot import name '_next_backoff'`).

- [ ] **Step 3: Implement** — in `async_worker.py` add near the top (after imports):

```python
from redis.exceptions import ConnectionError as RedisConnectionError, TimeoutError as RedisTimeoutError


def _next_backoff(current: float) -> float:
    return min(current * 2, 30)
```

Add to `Worker`:

```python
    async def fetch_loop(self) -> None:
        """Pull work only when a concurrency slot is free. Runs until cancelled."""
        backoff = 1.0
        while True:
            await self.sem.acquire()
            try:
                value = await self.redis.brpoplpush(self.main_key, self.backup_key, timeout=1)
                item = await self.process_message(value) if value is not None else None
            except (RedisConnectionError, RedisTimeoutError) as e:
                # ponytail: a payload fetched just before the error stays in our backup until release()
                self.sem.release()
                self.logger.warning('redis unavailable (%r); retrying in %.0fs', e, backoff)
                await asyncio.sleep(backoff)
                backoff = _next_backoff(backoff)
                continue
            except BaseException:
                self.sem.release()
                raise
            backoff = 1.0
            if item is None:
                self.sem.release()
                continue
            self.start_task(item)

    def start_task(self, item: QueueItem) -> None:
        task = asyncio.create_task(self.run_task(item), name=get_task_key(item.deserialize_data.id))
        self.inflight.add(task)
        task.add_done_callback(lambda t, _item=item: self._on_done(t, _item))

    def _on_done(self, task, item: QueueItem) -> None:
        self.inflight.discard(task)
        bg = asyncio.create_task(self.task_done(task, item))
        self.background.add(bg)
        bg.add_done_callback(self.background.discard)
```

Then delete `AsyncQueue`, `_task_done`, `_background_tasks`, `task_done_callback`, `_task_inner`, `create_async_task_from_queue`, `_reject`, `_process_message`, `listen_to_submitted_task`, and the body of `async_worker` after the `RuntimeError` check — replace that body for now with:

```python
    raise NotImplementedError('wired up in Task 7')  # ponytail: temporary, mid-branch only
```

Delete the two `AsyncQueue` tests and `from async_q.async_worker import AsyncQueue` in `tests/test_async_worker_queue.py`.

- [ ] **Step 4:** `python -m pytest -q` → Expected: all pass. (`test_cli_app_resolution` monkeypatches `async_worker`, unaffected.)
- [ ] **Step 5:** Check D3 in `IMPROVEMENTS.md`. Commit `refactor: semaphore-gated fetch loop replaces AsyncQueue producer/consumer`.

---

### Task 6: Heartbeat, reaper, retry promoter, legacy migration (B1 reclaim, B5)

**Files:**
- Modify: `src/async_q/async_worker.py` (`Worker.beat`, `Worker.reap`, `Worker.promote`, `Worker.migrate_legacy`, `Worker._every`; import `scripts` and `get_legacy_keys`)
- Test: `tests/test_worker_loops.py` (create)

**Interfaces:**
- Consumes: Task 3 `scripts.reclaim`, `scripts.promote_due`; Task 1 `get_legacy_keys`.
- Produces: `async beat()`, `async reap()`, `async promote()`, `async migrate_legacy(startup: bool) -> int` (items moved), `async _every(interval: float, fn, name: str)` (runs until cancelled; logs and continues on `Exception`).

- [ ] **Step 1: Failing tests** — create `tests/test_worker_loops.py`:

```python
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
```

- [ ] **Step 2:** `python -m pytest tests/test_worker_loops.py -q` → Expected: FAIL (`AttributeError: 'Worker' object has no attribute 'beat'`).

- [ ] **Step 3: Implement** — in `async_worker.py`: add `from . import scripts`; extend `.utils` import with `get_legacy_keys`. Add to `Worker`:

```python
    async def beat(self) -> None:
        await self.redis.set(self.hb_key, str(time.time()), px=int(self.heartbeat_ttl * 1000))
        # Re-register every beat: a worker wrongly reaped during a network blip comes back.
        await self.redis.sadd(self.workers_key, self.wid)

    async def reap(self) -> None:
        for member in await self.redis.smembers(self.workers_key):
            wid = member.decode() if isinstance(member, bytes) else member
            if wid == self.wid:
                continue
            n = await scripts.reclaim(self.redis, self.q, wid)
            if n > 0:
                self.logger.warning('reclaimed %d task(s) from dead worker %s', n, wid)
            elif n == 0:
                self.logger.info('removed dead worker %s', wid)
        await self.migrate_legacy(startup=False)

    async def promote(self) -> None:
        n = await scripts.promote_due(self.redis, self.q, time.time())
        if n:
            self.logger.debug('promoted %d due retr(y/ies)', n)

    async def migrate_legacy(self, startup: bool) -> int:
        """Move pre-0.3.0 keys into hash-tagged keys. Per item, not atomic across slots.
        # ponytail: delete one release after 0.3.0
        """
        old = get_legacy_keys(self.q)
        pairs = [(old['main'], self.main_key)]
        if startup:
            pairs += [(old['backup'], self.main_key), (old['dead'], self.dead_key)]
        moved = 0
        for src, dst in pairs:
            while True:
                v = await self.redis.rpop(src)
                if v is None:
                    break
                await self.redis.lpush(dst, v)
                moved += 1
                if src == old['backup']:
                    self.logger.warning('requeued legacy in-flight task; assumes no pre-0.3 workers are running')
        while True:
            items = await self.redis.zrange(old['retry'], 0, 99, withscores=True)
            if not items:
                break
            for v, score in items:
                await self.redis.zadd(self.retry_key, {v: score})  # add before remove: crash duplicates, never loses
                await self.redis.zrem(old['retry'], v)
                moved += 1
        if moved:
            self.logger.info('migrated %d item(s) from legacy keys', moved)
        return moved

    async def _every(self, interval: float, fn, name: str) -> None:
        while True:
            try:
                await fn()
            except Exception as e:
                self.logger.warning('%s failed: %r', name, e)
            await asyncio.sleep(interval)
```

- [ ] **Step 4:** `python -m pytest -q` → Expected: all pass.
- [ ] **Step 5:** Commit `feat: worker heartbeat, peer reaper, retry promoter and legacy key migration`.

---

### Task 7: `run()` orchestration, graceful drain, `async_worker` wiring (B1, B5, B6) + multi-worker integration

**Files:**
- Modify: `src/async_q/async_worker.py` (`Worker.run`, `Worker.drain`; rewrite `async_worker`)
- Test: `tests/test_worker_run.py` (create, unit)
- Test: `tests/test_multi_worker_integration.py` (create, real Redis)

**Interfaces:**
- Consumes: everything above; `AsyncTaskQueue` config attributes from Task 2.
- Produces: `async Worker.run(stop_event: asyncio.Event) -> None`, `async Worker.drain() -> None`; `async_worker(stop_event=None)` builds a `Worker` from the `AsyncTaskQueue` instance and runs it, closing Redis afterwards.

- [ ] **Step 1: Failing unit tests** — create `tests/test_worker_run.py`:

```python
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
def no_lua(monkeypatch):
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
        "    for _ in range(100):\n"
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
```

- [ ] **Step 1b: Failing integration tests** — create `tests/test_multi_worker_integration.py`:

```python
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
```

- [ ] **Step 2:** `ASYNC_Q_TEST_REDIS_URL=redis://localhost:6390/15 python -m pytest tests/test_worker_run.py tests/test_multi_worker_integration.py -q` → Expected: FAIL (`AttributeError: 'Worker' object has no attribute 'run'`) in both files.

- [ ] **Step 3: Implement** — add to `Worker`:

```python
    async def run(self, stop_event: asyncio.Event) -> None:
        await self.migrate_legacy(startup=True)
        await self.beat()
        loops = [
            asyncio.create_task(self._every(
                self.heartbeat_interval, self.beat,
                "heartbeat (peers may reclaim this worker's tasks if Redis stays unreachable)")),
            asyncio.create_task(self._every(self.heartbeat_interval, self.reap, 'reaper')),
            asyncio.create_task(self._every(1, self.promote, 'retry promoter')),
        ]
        fetch = asyncio.create_task(self.fetch_loop())
        stop_wait = asyncio.create_task(stop_event.wait())
        self.logger.info('worker %s listening on %s', self.wid, self.main_key)
        try:
            await asyncio.wait({fetch, stop_wait}, return_when=asyncio.FIRST_COMPLETED)
        finally:
            for t in (fetch, stop_wait):
                t.cancel()
            for r in await asyncio.gather(fetch, stop_wait, return_exceptions=True):
                if isinstance(r, Exception):
                    self.logger.error('fetch loop crashed: %r', r)
            await self.drain()
            for t in loops:
                t.cancel()
            await asyncio.gather(*loops, return_exceptions=True)
            try:
                n = await scripts.release(self.redis, self.q, self.wid)
                if n:
                    self.logger.info('requeued %d unstarted task(s) on exit', n)
            except Exception as e:
                self.logger.error('release failed (%r); peers will reclaim after heartbeat expiry', e)
            self.logger.info('worker %s stopped', self.wid)

    async def drain(self) -> None:
        """Let running tasks finish (up to shutdown_timeout), cancel the rest, flush acks."""
        if self.inflight:
            self.logger.info('waiting up to %ss for %d running task(s)', self.shutdown_timeout, len(self.inflight))
            _, pending = await asyncio.wait(set(self.inflight), timeout=self.shutdown_timeout)
            for t in pending:
                t.cancel()
            if pending:
                # ponytail: a task that swallows cancellation gets one more shutdown_timeout, then is abandoned
                await asyncio.wait(pending, timeout=self.shutdown_timeout)
        await asyncio.sleep(0)  # let done-callbacks schedule their ack tasks
        if self.background:
            await asyncio.gather(*set(self.background), return_exceptions=True)
```

Replace the temporary body of `async_worker` (the whole function) with:

```python
async def async_worker(stop_event: Optional[asyncio.Event] = None):
    app = AsyncTaskQueue.get_instance()
    if not app:
        raise RuntimeError('AsyncTaskQueue is not initialized')

    r = app.redis_builder.get_redis_async()
    worker = Worker(
        r, app.distribute_qname, app.get_concurrency(), app.logger,
        status_ttl=app.status_ttl,
        heartbeat_interval=app.heartbeat_interval,
        heartbeat_ttl=app.heartbeat_ttl,
        shutdown_timeout=app.shutdown_timeout,
    )
    if stop_event is None:
        stop_event = asyncio.Event()
    try:
        await worker.run(stop_event)
    finally:
        await r.close()
```

Note: a task abandoned by `drain` (ignored cancel twice) still holds its payload in our backup; `release` moves it back to main. Accepted (spec: at-least-once).

- [ ] **Step 4:** `python -m pytest -q` → Expected: all pass (integration skipped).
- [ ] **Step 5:** `ASYNC_Q_TEST_REDIS_URL=redis://localhost:6390/15 python -m pytest tests/test_multi_worker_integration.py -q` → Expected: PASS.
- [ ] **Step 6:** `ASYNC_Q_TEST_REDIS_URL=redis://localhost:6390/15 python -m pytest -q` → Expected: all pass. `python -m pytest -q` → Expected: unit pass, integration skipped.
- [ ] **Step 7:** Check B1, B5, B6 in `IMPROVEMENTS.md`. Commit `feat: graceful drain and multi-worker run loop with heartbeat reclaim`.

---

### Task 8: CLI `--shutdown-timeout` + Windows Ctrl+C fallback

**Files:**
- Modify: `src/async_q/__main__.py` (`main`)
- Test: `tests/test_cli_app_resolution.py` (append)

**Interfaces:**
- Consumes: Task 2 `AsyncTaskQueue.shutdown_timeout`.
- Produces: CLI flag `--shutdown-timeout SECONDS` (float, default `None` = keep app's value).

- [ ] **Step 1: Failing tests** (append; `asyncio` is already imported further up the file):

```python
def _app_file(tmp_path):
    app_file = tmp_path / "app_cli2.py"
    app_file.write_text(
        "from async_q import AsyncTaskQueue, RedisBuilder\n"
        "app = AsyncTaskQueue(redis_builder=RedisBuilder())\n"
    )
    return app_file


def test_main_shutdown_timeout_flag(tmp_path, monkeypatch, reset_async_task_queue):
    from async_q import __main__ as cli

    async def fake_worker(stop_event=None):
        pass

    async def fake_close():
        pass

    monkeypatch.setattr(cli, "async_worker", fake_worker)
    monkeypatch.setattr(cli, "close", fake_close)
    cli.main(["-a", f"{_app_file(tmp_path)}:app", "--shutdown-timeout", "600"])
    assert cli.AsyncTaskQueue.get_instance().shutdown_timeout == 600


def test_main_falls_back_to_signal_signal_when_loop_lacks_handlers(tmp_path, monkeypatch, reset_async_task_queue):
    from async_q import __main__ as cli
    installed = {}
    real_new_loop = asyncio.new_event_loop

    def loop_without_signal_support():
        loop = real_new_loop()

        def nope(*a, **k):
            raise NotImplementedError

        loop.add_signal_handler = nope
        return loop

    def fake_signal(sig, handler):
        installed[sig] = handler

    seen = {}

    async def fake_worker(stop_event=None):
        installed[cli.signal.SIGINT](cli.signal.SIGINT, None)   # simulate Ctrl+C
        await asyncio.wait_for(stop_event.wait(), 1)
        seen["stopped"] = True

    async def fake_close():
        pass

    monkeypatch.setattr(cli.asyncio, "new_event_loop", loop_without_signal_support)
    monkeypatch.setattr(cli.signal, "signal", fake_signal)
    monkeypatch.setattr(cli, "async_worker", fake_worker)
    monkeypatch.setattr(cli, "close", fake_close)
    cli.main(["-a", f"{_app_file(tmp_path)}:app"])
    assert seen == {"stopped": True}
```

- [ ] **Step 2:** `python -m pytest tests/test_cli_app_resolution.py -q` → Expected: FAIL (`SystemExit: 2` from argparse "unrecognized arguments: --shutdown-timeout"; second test `assert {} == {'stopped': True}` — the `KeyError` inside `fake_worker` is swallowed by `main`'s `except Exception`).

- [ ] **Step 3: Implement** — in `main()`, after the `-lf` argument:

```python
    parser.add_argument('--shutdown-timeout', default=None, type=float,
                        help='seconds to let running tasks finish on shutdown before re-queueing them (default 30)')
```

After `async_task_queue.distribute_qname = args.queue`:

```python
    if args.shutdown_timeout is not None:
        async_task_queue.shutdown_timeout = args.shutdown_timeout
```

Replace the signal block:

```python
        try:
            loop.add_signal_handler(signal.SIGINT, _request_shutdown)
            loop.add_signal_handler(signal.SIGTERM, _request_shutdown)
        except NotImplementedError:
            # Windows: no loop signal handlers. Route Ctrl+C / Ctrl+Break to the same graceful path.
            for sig in (signal.SIGINT, getattr(signal, 'SIGBREAK', None)):
                if sig is not None:
                    signal.signal(sig, lambda *_: loop.call_soon_threadsafe(_request_shutdown))
```

- [ ] **Step 4:** `python -m pytest -q` → Expected: all pass.
- [ ] **Step 5: Manual check (Windows, real Redis):** in one terminal `async_q -a <app>:app -q demo --shutdown-timeout 5` with `ASYNC_Q`'s app pointing at `localhost:6390`; press Ctrl+C → Expected log lines `shutdown signal received; stopping...` then `worker ... stopped`, and `redis-cli -p 6390 smembers "async_task_workers:{demo}"` (via `docker exec async-q-test-redis redis-cli ...`) → Expected: empty.
- [ ] **Step 6:** Commit `feat: --shutdown-timeout flag and graceful Ctrl+C on Windows`.

---

### Task 9: Docs, version, tracker

**Files:**
- Modify: `README.md` (new "Running multiple workers" section after "Running Worker Processes")
- Modify: `pyproject.toml` (`version = "0.3.0"`)
- Modify: `IMPROVEMENTS.md`

- [ ] **Step 1: README** — add:

````markdown
### Running multiple workers

Run as many worker processes per queue as you like (containers, pods, terminals). Each worker keeps its in-flight tasks in its own Redis list and refreshes a heartbeat every 10s. If a worker dies, a peer moves its in-flight tasks back to the queue within ~40s.

- **Delivery is at-least-once.** A task can run twice if its worker dies mid-task or loses Redis for longer than the heartbeat TTL. Make tasks idempotent.
- **Long tasks are fine.** Liveness is per worker, not per task, so a 2-hour task on a healthy worker is never taken away.
- **Graceful shutdown.** On SIGTERM/Ctrl+C a worker stops fetching, waits up to `--shutdown-timeout` seconds (default 30) for running tasks, then re-queues the rest. Raise it for long tasks, and set your orchestrator's grace period (e.g. Kubernetes `terminationGracePeriodSeconds`) a bit higher.
- **Redis Cluster.** Use `RedisBuilder(host=..., port=..., cluster=True)`. Every key of a queue shares one hash slot (`async_task:{<queue>}` …).
- **Tuning.** `AsyncTaskQueue(redis_builder, status_ttl=86400, heartbeat_interval=10, heartbeat_ttl=30, shutdown_timeout=30)`.

**Upgrading from 0.2.x:** key names changed. New workers move tasks from the old keys automatically, and keep draining the old queue key for producers that are still on 0.2.x. Stop all 0.2.x **workers** before starting 0.3 workers.
````

- [ ] **Step 2:** `pyproject.toml`: `version = "0.2.1"` → `version = "0.3.0"`.
- [ ] **Step 3: Tracker** — in `IMPROVEMENTS.md` confirm B1, B5, B6, B10, D3 are checked; under "Batch 2" add:

```markdown
- [ ] **B11 — Remove legacy (pre-0.3.0) key drain** (`get_legacy_keys`, `Worker.migrate_legacy`) one release after 0.3.0.
- [ ] **B12 — Payload fetched right before a Redis error stays in the worker's own backup until shutdown.** Rare; reclaim only happens on worker death. Fix: retry the ack/requeue once Redis is back.
```

- [ ] **Step 4:** `ASYNC_Q_TEST_REDIS_URL=redis://localhost:6390/15 python -m pytest -q` → Expected: all pass.
- [ ] **Step 5:** Commit `docs: multi-worker guide, 0.3.0, tracker`.
- [ ] **Step 6:** `docker stop async-q-test-redis`.
- [ ] **Step 7:** Use superpowers:finishing-a-development-branch.
