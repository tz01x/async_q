## AsyncQ improvements and feature roadmap

This document outlines high‑impact cleanups, reliability hardening, and new features to bring `async_q` to a modern, maintainable state.

### Current state (quick take)
- Simple Redis‑backed async task runner using BRPOPLPUSH + backup queue
- Single process with asyncio concurrency; one queue name per worker
- Task payload stores function file path and name; functions resolved dynamically
- Minimal logging/observability; no retries, scheduling, or result backend beyond basic status keys

### High‑priority fixes (short term)
- [x] Avoid mutable default arguments in `submit_task` (`args=[]`, `kwargs={}`) → use `None` defaults and normalize inside.
- [x] Fix typos and inconsistent status values: "pandding" → "pending"; "Finalazing" → "Finalizing"; "Staring" → "Starting".
- [x] Make `msgpack.unpackb` explicit: `raw=False` to avoid bytes for strings across versions.
- [x] Guard logger handler duplication in `AsyncTaskQueue.config_logger` (avoid adding multiple handlers).
- [x] Use named args for Redis `set` TTL: `await redis.set(key, value, ex=3600)`.
- [x] Align README API: `submit_task` uses `queue_name`, not `kwargs['queue']`.
- [x] Handle undefined `p_task`/`c_task` in `async_worker` `finally` if startup fails.
- [x] Replace `redis.keys('pattern')` in example view with `SCAN` to avoid blocking Redis.
- [ ] Add type hints and docstrings across public APIs; enable `mypy` and `ruff`/`flake8`.
- [x] Pin compatible dependency ranges (e.g., `redis>=5,<6`, `msgpack>=1.0,<2`).

### Reliability and operations
- [ ] Graceful shutdown: handle signals (SIGINT/SIGTERM), cancel producer/consumer, drain queue, and update task statuses cleanly.
- [x] Graceful shutdown: handle signals (SIGINT/SIGTERM), cancel producer/consumer, drain queue, and update task statuses cleanly.
- [ ] Retries with exponential backoff and max attempts; send failed tasks to a dead‑letter queue.
- [ ] Per‑task timeout and cancellation support.
- [ ] Heartbeats + visibility timeout: periodically move stuck items from backup to main if worker is dead or exceeds visibility timeout.
- [ ] Observability: structured logs, metrics (Prometheus), and tracing hooks (OpenTelemetry).
- [ ] Backpressure via `asyncio.Semaphore` or bounded worker pool; avoid scanning `asyncio.all_tasks()` in `AsyncQueue.full`.
- [ ] Result backend abstraction (Redis hash or separate backend) to fetch result/exception, not only status.
- [ ] Idempotency keys/deduplication support at enqueue time.
- [ ] Task priority queues (e.g., `async_task:default:high|low` or a sorted set scheduler).
- [ ] Configurable per‑queue concurrency and the ability to consume from multiple queues in one worker.
- [ ] Lua scripts for atomic ack patterns and batch pops to reduce round trips.

### API ergonomics and safety
- [ ] Task registration decorator (e.g., `@async_q.task(name=...)`) with explicit registry instead of importing by file path; avoid arbitrary code execution from paths.
- [ ] Use dotted module paths (`package.module:function`) for portability across hosts instead of filesystem paths.
- [ ] Task options: `queue_name`, `retries`, `retry_backoff`, `timeout`, `eta`, `delay`, `priority`, `idempotency_key`.
- [ ] Synchronous task wrapper: transparently run sync callables in a thread pool.
- [ ] Middleware hooks: before/after task execution, success/failure handlers.

### Scheduling and orchestration
- [ ] Delayed tasks and simple scheduler (Redis ZSET of timestamps; mover process promotes ready tasks to the queue).
- [ ] Periodic/cron tasks with a lightweight scheduler process.
- [ ] Task chaining and groups (fan‑out, gather, chord‑like primitive).
- [ ] Cancellation API and cooperative cancellation propagation to tasks.

### Storage and protocol
- [ ] Versioned wire format for task payloads; include schema version.
- [ ] Optional compression for large payloads.
- [ ] Pluggable serializer interface (MsgPack default; JSON/pickle optional).

### Security
- [ ] Restrict task import to a registry and allowlist; disallow arbitrary file paths.
- [ ] Validate/size‑limit args/kwargs; optionally sign messages to prevent tampering.
- [ ] Auth/TLS options for Redis; support Redis Sentinel/Cluster.

### CLI and DX
- [ ] CLI: support multiple queues `-q q1,q2` and per‑queue concurrency `--q-concurrency q1=10,q2=2`.
- [ ] Add `--metrics-port` to expose Prometheus metrics.
- [ ] Improve error surfaces and human‑readable task IDs (prefix + short id).
- [ ] Add `async_q --version`, `--config` file support (TOML/YAML).

### Documentation
- [x] Update README with correct `submit_task` signature and multi‑queue examples.
- [ ] Add architecture docs (queue, backup, ack flow) and sequence diagrams.
- [ ] Usage guides for Django/FastAPI, deployment, and scaling patterns.
- [ ] Troubleshooting guide (Redis connectivity, stuck tasks, memory, timeouts).

### Example app fixes (django-example)
- [x] Replace `redis.keys('*')` in `get_current_submitted_task_element` with `SCAN`.
- [x] `requirements.txt`: use `async_q` (not `async-q`), and pin versions; add `redis`.
- [ ] The file writer appends raw JSON fragments; either write NDJSON or proper arrays.
- [ ] Ensure async views run under ASGI (Daphne/Uvicorn) and document setup.

### Potential breaking changes to plan
- Switch to registered tasks and dotted paths (deprecate file‑path import).
- Rename status values to a consistent set: `submitted → queued → running → finished|failed`.
- Change `submit_task(func, args, kwargs, queue_name=...)` to `submit_task(func, *, args=None, kwargs=None, queue_name='default', options=TaskOptions(...))`.

### Suggested implementation plan
1) Foundation cleanup: typing, defaults, typos, logger guards, README/API alignment, CI with lint + type check.
2) Reliability core: retries, timeouts, graceful shutdown, visibility timeout/heartbeat, SCAN usage, atomic ack Lua.
3) DX and API: task registry/decorator, dotted paths, middleware, sync wrapper.
4) Scheduling and orchestration: delays, periodic tasks, chaining/groups.
5) Observability: metrics endpoint, structured JSON logs, tracing hooks, minimal web dashboard for task state.

### File‑level notes (where to change)
- `src/async_q/async_q.py`
  - Change `submit_task` signature to avoid mutable defaults; add typing; honor `queue_name`; generate human‑friendly task id prefix; use `get_module` dotted path if adopting registry.
  - In `RedisBuilder`, expose connection kwargs; consider pools and health checks.
- `src/async_q/async_worker.py`
  - Replace `AsyncQueue.full()` heuristic with semaphore; fix status to `pending`; add retries/timeout; heartbeat + visibility timeout mover; robust finally with guards; named TTL args.
  - Use `SCAN` when listing keys (any admin/monitoring path); add metric counters and timings.
- `src/async_q/utils.py`
  - `deserialize`: `msgpack.unpackb(byte_value, raw=False)`; define `Enum` for statuses; add serializer interface.
  - Export helpers for `now_ms()`, backoff calculation, and ID generation.
- `src/async_q/__main__.py`
  - Spelling fixes; add `--queues` (multi) and `--metrics-port`; install signal handlers; run with `asyncio.run()`.

### New feature ideas (beyond MVP)
- Minimal web UI (Starlette/FastAPI) for queues/tasks with actions (retry/cancel/purge).
- Rate limiting per task/queue and global throttles.
- Task result streaming/progress updates via Pub/Sub or WebSocket.
- Pluggable backends (SQS/RabbitMQ) behind a broker interface.
- Plugin system for custom middlewares and metrics exporters.

### Tooling/CI
- GitHub Actions: test matrix for 3.8–3.12, Redis service, lint/type checks.
- Pre‑commit with `ruff`, `black`, `mypy`.
- Basic integration tests spinning a Redis container and exercising enqueue/execute/ack/failure paths.

---
If you want, I can start by implementing the short‑term fixes as a PR: mutable defaults, typos/status names, README/API alignment, logger guard, explicit msgpack config, and safer `finally` handling.


