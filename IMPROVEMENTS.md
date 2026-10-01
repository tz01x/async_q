# Improvements tracker

Findings from the 2026-10-01 code review. Check items off as PRs land; link the PR next to the item.

Plans live in `docs/superpowers/plans/`.

## Batch 1 — quick fixes

Plan: [`docs/superpowers/plans/2026-10-01-quick-fixes.md`](docs/superpowers/plans/2026-10-01-quick-fixes.md)

- [x] **B4 — Task module re-executed on every task.** `utils.get_function_ref` runs `exec_module` per call: slow, resets module state, leaks memory. Fix: `functools.lru_cache`. Note: workers now need a restart to pick up task code changes.
- [x] **B3 — Poison message kills the worker.** Bad payload or unloadable function raises out of the producer loop (`async_worker.listen_to_submitted_task`); item stays in backup and crashes again on restart. Also dead `isinstance` branch + `assert` that `-O` strips. Fix: reject to DLQ and continue.
- [x] **B8 — `async_worker` builds but never raises "not initiated" exception.** Fix: `raise RuntimeError`.
- [x] **B2 — Retries unreachable from public API.** `submit_task` has no `max_retries` / `backoff_*` params. Fix: add them.
- [x] **B7 — Fire-and-forget done-handler tasks may be GC'd.** `task_done_callback` drops the `create_task` reference. Fix: hold in a module-level set.
- [x] **B9 — CLI uses deprecated `asyncio.get_event_loop()`.** Fix: `main()` with `asyncio.new_event_loop()`, plus `async_q` console script.

Found in Batch 1 review (deferred):
- [ ] **F1 — Payload with wrong field types (e.g. int `id`) still crashes the producer.** Only from foreign/buggy producers. Fix: type-check `id`/`path`/`func_name` in the deserialize `try`.
- [ ] **F2 — Shared task module executed once per function.** Cache key is `(path, func_name)`; `task_a` and `task_b` in one file get separate module state. Fix: cache a path-keyed module loader.
- [ ] **F3 — README doesn't say workers must restart to pick up task code changes** (consequence of B4).
- [ ] **F4 — `main()` leaves a closed loop as current.** Add `asyncio.set_event_loop(None)` after `loop.close()`.
- [ ] **F5 — `backoff_*` params not validated** (negative values → immediate retry).
- [ ] **F6 — DLQ entries don't record why they failed.** Add an `error` field.

## Batch 2 — multi-worker correctness

- [x] **B1 — Multiple workers on one queue re-run each other's tasks.** Backup list is shared; startup (`listen_to_submitted_task`) and shutdown (`async_worker` finally) move the *whole* backup list back to main. Fix: per-worker backup key + heartbeat/reclaim of dead workers' backups.
- [x] **B5 — Retries starve under load; non-atomic retry promotion.** Retry ZSET only polled when `brpoplpush` times out. `zrangebyscore`+`zrem`+`lpush` races between workers. Fix: poll on a timer; only `lpush` if `zrem` returned 1 (or Lua script).
- [x] **B6 — Shutdown doesn't drain in-flight tasks.** Backup is requeued while tasks still run (duplicates); `r.close()` races done-callbacks. Fix: track in-flight tasks, `gather` with timeout, then requeue + close.
- [x] **B10 — Status keys leak.** Only `finished` gets a TTL; `starting`, `failed`, DLQ statuses never expire. Fix: TTL on every status write (configurable).
- [ ] **B11 — Remove legacy (pre-0.3.0) key drain** (`get_legacy_keys`, `Worker.migrate_legacy`) one release after 0.3.0.
- [ ] **B12 — Orphan window after a wrong reap.** A worker reaped during an outage that fetches before its next `beat()` and is then hard-killed leaves its backup unregistered forever. Fix: re-`SADD` in fetch error recovery, or have the reaper scan `async_task_backup:{q}:*` keys.
- [ ] **B13 — CLI exits 0 on fatal errors.** The process exits 0 when the fetch loop crashes or Redis is down at startup, so supervisors that restart only on failure won't restart it. Fix: non-zero exit code.

## Design / security

- [ ] **D1 — Payload carries an absolute file path that the worker `exec`s.** Redis write access = RCE on workers; also breaks when producer and worker paths differ (Docker). Fix: `module:qualname` + `importlib.import_module`, ideally a `@task` registry.
- [ ] **D2 — `task_id` forced into kwargs.** Tasks without `task_id` param / `**kwargs` fail with `TypeError`. Fix: pass only if signature accepts it, or document.
- [x] **D3 — `AsyncQueue` scans `asyncio.all_tasks()` with regex per `put` and pokes private `_putters`.** Fix: `asyncio.Semaphore(concurrency)`, delete class.
- [ ] **D4 — No async submit.** `submit_task` blocks async views. Fix: `asubmit_task` using the async client.

## Cleanup / DX

- [ ] **C1 — Custom `utils.to_thread` duplicates `asyncio.to_thread`.** Bump `requires-python` to `>=3.9` (3.8 is EOL) and delete it.
- [ ] **C2 — Test deps undeclared.** Add `[project.optional-dependencies] test = ["pytest", "pytest-asyncio"]`.
- [ ] **C3 — No CI.** GitHub Actions: ruff + pytest on 3.9–3.13.
- [ ] **C4 — `ROADMAP.md` stale.** Graceful shutdown listed twice; retries/DLQ done but unchecked.
- [ ] **C5 — Example `web_crawler/task.py` is empty.**
