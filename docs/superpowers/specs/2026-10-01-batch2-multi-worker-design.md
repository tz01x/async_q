# Batch 2: Multi-worker correctness — Design

**Date:** 2026-10-01
**Tracker items:** B1, B5, B6, B10 (+ D3 folded in) from `IMPROVEMENTS.md`
**Builds on:** Batch 1 (`fix/batch1-quick-fixes`, PR #32)

## Goal

Make it safe to run N async_q worker processes on the same queue, including on Redis Cluster and with tasks that run for hours, without losing tasks or re-running other workers' in-flight tasks during normal operation.

## Context and constraints

- **Deployment:** N worker processes/pods per queue; some tasks run 10+ minutes. A fixed per-task visibility timeout is therefore unacceptable — liveness must be per worker, not per task.
- **Redis:** must work on Redis Cluster (and therefore single Redis / Sentinel). All multi-key operations on a queue must hit one hash slot.
- **Finding:** async_q does not work on Cluster today — `BRPOPLPUSH async_task:<q> async_task_backup:<q>` spans two slots (`CROSSSLOT`). Existing deployments are necessarily non-Cluster.
- **Delivery guarantee:** at-least-once. Never lose a task; duplicates allowed after a crash or heartbeat outage. Tasks should be idempotent (documented).
- **Compatibility:** tasks sitting in old-name keys at upgrade time must survive. Producers and workers may be upgraded in any order; old-version *workers* must not run alongside new ones.
- No new runtime dependencies. No new test dependencies. `requires-python >=3.8` unchanged.

## Approach

Keep the Redis list model. Give each worker its own backup list, detect dead workers with a per-worker heartbeat key, and reclaim a dead worker's backup atomically with Lua. Rejected alternatives: Redis Streams consumer groups (rewrite + wire-format change + per-message claim refresh for long tasks); visibility-timeout ZSET (per-task heartbeats, more writes, reimplements Streams).

## Keys

`q` = queue name. `{q}` is a Cluster hash tag: all keys of one queue share a slot.

| Key | Type | Purpose |
|---|---|---|
| `async_task:{q}` | list | main queue; producers `LPUSH` |
| `async_task_backup:{q}:<wid>` | list | this worker's in-flight payloads |
| `async_task_retry:{q}` | zset | retries, score = due unix time |
| `async_task_dead:{q}` | list | dead-letter queue (no TTL) |
| `async_task_hb:{q}:<wid>` | string | heartbeat, `SET … PX heartbeat_ttl` |
| `async_task_workers:{q}` | set | registry of worker ids |
| `async-q-task:<id>` | string | task status (unchanged name; single-key) |

Legacy (pre-0.3.0) keys: `async_task:<q>`, `async_task_backup:<q>`, `async_task_retry:<q>`, `async_task_dead:<q>`.

**Worker id (`wid`):** `<hostname>-<pid>-<6 random hex>`, generated per process start.

## Redis client

`RedisBuilder(..., cluster: bool = False)`. `cluster=True` builds `redis.RedisCluster` (sync) and `redis.asyncio.RedisCluster` (async) with the same kwargs; otherwise unchanged.

## Atomic operations

- **Fetch:** `BRPOPLPUSH async_task:{q} async_task_backup:{q}:<wid> timeout=1`.
- **Ack (success):** `LREM async_task_backup:{q}:<wid> 1 <payload>` on the worker's own list (a single atomic command), then status. Failure, reject and cancel acks use the `fail_ack` / `requeue` scripts below so a crash between steps never leaves the payload in no list; status is written only after the script, and only if it removed the payload.
- **In-flight copies:** if peers reclaimed this worker's backup during an outage and it re-fetches the same bytes, two in-flight copies share one backup entry. The worker counts in-flight copies per payload; every copy but the last skips its ack/retry/DLQ (logs a warning, frees its slot) and the last one acks normally.
- **Lua `reclaim`** — KEYS: hb, backup, main, registry; ARGV: wid. If `EXISTS hb` → return 0. Else `RPOPLPUSH backup main` until empty, `DEL backup`, `SREM registry wid`, return count moved. Idempotent; concurrent reapers are harmless.
- **Lua `promote_due`** — KEYS: retry zset, main; ARGV: now, limit (100). `ZRANGEBYSCORE retry -inf now LIMIT 0 limit`, `ZREM` each, `RPUSH main` each (the consuming end, so due retries run next), return count. No double promotion.
- **Lua `release`** — KEYS: backup, main, hb, registry; ARGV: wid. Move all of backup to main, `DEL backup`, `DEL hb`, `SREM registry wid`.
- **Lua `requeue`** — KEYS: backup, main; ARGV: payload. `LREM backup 1 payload`; only if removed, `RPUSH main payload` (front of the queue). Returns removed count. Used for cancelled tasks and for a payload stranded by a Redis error right after fetch; idempotent.
- **Lua `fail_ack`** — KEYS: backup, retry zset, dead; ARGV: original, new_payload, score, mode (`retry` | `dead`). `LREM backup 1 original`; only if removed: `retry` → `ZADD retry score new_payload`, `dead` → `LPUSH dead new_payload`. Returns removed count (0 = a peer reclaimed it; nothing written). Used for failed tasks (new_payload carries the bumped attempt) and rejected payloads (new_payload = original).

## Worker lifecycle

### Concurrency (replaces `AsyncQueue`, D3)

Delete `AsyncQueue` and the producer/consumer split. One fetch loop:

```
while not stop:
    await sem.acquire()                       # sem = Semaphore(concurrency)
    value = BRPOPLPUSH main → own backup (1s)
    if value is None: sem.release(); continue
    await _process_message(...)               # Batch 1: rejects to DLQ, releases slot
    task = create_task(run(item)); inflight.add(task)
    on done: schedule _task_done (held in _background_tasks), inflight.discard, sem.release()
```

A payload leaves main only when a slot is free, so a worker never hoards un-started work in its backup.

### Startup

1. Generate `wid`.
2. Legacy migration (below).
3. `SET hb PX heartbeat_ttl`, `SADD registry wid`.
4. Start loops: **fetch**; **heartbeat** (every `heartbeat_interval`: `SET hb PX heartbeat_ttl` + `SADD registry wid` — re-registers if wrongly reaped); **reaper** (every `heartbeat_interval`: `reclaim` for each registry member ≠ self, then legacy drain); **retry promoter** (every 1s: `promote_due`).

### Shutdown (SIGTERM, SIGINT, or `stop_event`)

1. Stop fetching: the in-progress 1s fetch is cancelled; a payload it already moved to the backup is not run and is requeued by `release` (step 6).
2. `asyncio.wait(inflight, timeout=shutdown_timeout)`.
3. Cancel remaining in-flight tasks; their done-handlers requeue them (existing `CancelledError` path).
4. Await all `_background_tasks`.
5. Cancel heartbeat/reaper/promoter loops.
6. `release(wid)`.
7. Close Redis.

**Hard kill:** no release; heartbeat expires within `heartbeat_ttl`; a peer reclaims within one more `heartbeat_interval` (≤ ~40s with defaults).

**Windows:** `loop.add_signal_handler` raises `NotImplementedError`; fall back to `signal.signal(SIGINT/SIGBREAK, …)` that calls `loop.call_soon_threadsafe(stop_event.set)` so Ctrl+C takes the graceful path.

## Legacy migration (one release)

- **On startup:** move each item of legacy `async_task_backup:<q>` → `async_task:{q}`; log a warning that this assumes no old-version workers are running.
- **On startup and every reaper tick:** move legacy `async_task:<q>` → `async_task:{q}` and legacy `async_task_retry:<q>` → `async_task_retry:{q}` (preserving scores); legacy DLQ → new DLQ on startup.
- Moves are per item (`RPOP`/`LPUSH`, or `ZRANGE`+`ZADD`+`ZREM`), not atomic across slots: a crash mid-move can lose at most the one item in transit. Accepted.
- Tracker gets an item to delete the legacy drain in the release after 0.3.0.

## Configuration

`AsyncTaskQueue(redis_builder, *, status_ttl=86400, heartbeat_interval=10, heartbeat_ttl=30, shutdown_timeout=30)`.

- `heartbeat_ttl < 2 * heartbeat_interval` → `ValueError`. All values must be > 0 → `ValueError`.
- CLI: `--shutdown-timeout SECONDS` overrides `shutdown_timeout`.

## Status TTL (B10)

Every write to `async-q-task:<id>` uses `ex=status_ttl` (submitted, pending, starting, finished, failed, retry-scheduled, requeued). DLQ list has no TTL.

## Error handling

- Heartbeat / reaper / promoter loops: catch, log, continue each iteration. Heartbeat failures log a warning noting peers may reclaim this worker's tasks after `heartbeat_ttl`.
- Fetch loop: on any `redis.RedisError` (connection, timeout, cluster-down, …), log and retry with backoff 1s → 2s → … capped at 30s; reset on success. Never exits the worker.
- Late ack after being reaped: `LREM` returns 0; log a warning (task may have run twice).

## Testing

- **Unit (existing fakes, always run):** key builders; config validation; fetch loop respects concurrency and releases slots on reject/success/failure; shutdown order (drain, cancel after timeout, background tasks awaited, release called); loops survive exceptions; fetch backoff on connection errors.
- **Integration (real Redis, skipped unless `ASYNC_Q_TEST_REDIS_URL` is set):** `reclaim` skips live / reclaims dead worker; `promote_due` promotes only due items and only once under concurrent callers; `release`; two workers on one queue — killing one (no release) gets its in-flight task re-run by the other, while a graceful stop of one does not disturb the other's in-flight tasks; legacy migration moves old keys; a task running longer than `heartbeat_ttl` on a live worker is not reclaimed.
- Cluster: unit-test that every per-queue key carries the same `{q}` hash tag (slot equality via `redis.crc.key_slot`). Real-Cluster integration test is out of scope (no local Cluster).
- Local run: `docker run -d -p 6379:6379 redis:7` + `ASYNC_Q_TEST_REDIS_URL=redis://localhost:6379/15`.

## Release

- Version `0.3.0`.
- README: "Running multiple workers" — Cluster (`RedisBuilder(cluster=True)`), at-least-once + idempotency, `--shutdown-timeout` for long tasks, upgrade order.
- `IMPROVEMENTS.md`: check B1, B5, B6, B10, D3; add "remove legacy key drain" item.

## Out of scope

Redis Streams, priority queues, F1–F6, D1, D2, D4, real-Cluster CI.
