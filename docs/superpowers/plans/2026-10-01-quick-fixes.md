# Batch 1 Quick Fixes Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fix six small reliability bugs in `async_q` (B4, B3, B8, B2, B7, B9 from `IMPROVEMENTS.md`) without changing the wire format or queue layout.

**Architecture:** Surgical edits to `src/async_q/{utils,async_worker,async_q,__main__}.py`. The producer's per-message logic is pulled into `_process_message` so it can be tested directly. Hand-rolled fake Redis (already in `tests/test_retry_and_dlq.py`) moves to `tests/fakes.py` for reuse.

**Tech Stack:** Python, asyncio, redis-py 5 (`redis.asyncio`), msgpack, pytest + pytest-asyncio.

**Spec:** `IMPROVEMENTS.md` (Batch 1 section).

## Global Constraints

- `requires-python = ">=3.8"` stays — no `X | Y` types, no `match`, no `asyncio.to_thread`.
- No new runtime dependencies. No new test dependencies (no fakeredis).
- Wire format (`TaskMetaData` fields, Redis key names) unchanged.
- Run tests with `python -m pytest -q` from repo root. Baseline: 12 passed.
- After each task, check its box in `IMPROVEMENTS.md`.
- Commit messages end with: `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`

## Review Focus

1. Payload from an older producer (no retry fields) → still deserializes via dataclass defaults. Covered: Task 2 happy-path test uses a payload built without retry fields.
2. Bytes that aren't msgpack, or msgpack that isn't a `TaskMetaData` dict → worker keeps running, payload lands in DLQ, removed from backup. Covered: Task 2.
3. Task file deleted / function renamed after submit → `FileNotFoundError` / `AttributeError` → DLQ with status `failed`, worker keeps running. Covered: Task 2.
4. Negative `max_retries` → `ValueError` at submit time, nothing pushed. Covered: Task 4.
5. Load failure must not be cached (fixing the file and resubmitting should work without restarting). `lru_cache` doesn't cache exceptions. Covered: Task 1.

---

### Task 0: Branch

- [ ] **Step 1:** `git checkout -b fix/batch1-quick-fixes`
- [ ] **Step 2:** `python -m pytest -q` → Expected: `12 passed`.
- [ ] **Step 3:** Commit tracker + plan:

```bash
git add IMPROVEMENTS.md docs/superpowers/plans/2026-10-01-quick-fixes.md
git commit -m "docs: add improvements tracker and batch 1 plan"
```

---

### Task 1 (B4): Cache resolved task functions

**Files:**
- Modify: `src/async_q/utils.py` (`get_function_ref`)
- Test: `tests/test_utils_misc.py` (append)

**Interfaces:**
- Produces: `get_function_ref(path: str, func_name: str)` — same signature, now memoized; `get_function_ref.cache_clear()` available.

- [ ] **Step 1: Write the failing tests** (append to `tests/test_utils_misc.py`):

```python
def test_get_function_ref_loads_module_once(tmp_path):
    from async_q.utils import get_function_ref
    get_function_ref.cache_clear()
    mod = tmp_path / "counted.py"
    mod.write_text(
        "import builtins\n"
        "builtins._async_q_loads = getattr(builtins, '_async_q_loads', 0) + 1\n"
        "async def job():\n    pass\n"
    )
    import builtins
    builtins._async_q_loads = 0
    f1 = get_function_ref(str(mod), "job")
    f2 = get_function_ref(str(mod), "job")
    assert f1 is f2
    assert builtins._async_q_loads == 1


def test_get_function_ref_does_not_cache_failures(tmp_path):
    import pytest
    from async_q.utils import get_function_ref
    get_function_ref.cache_clear()
    mod = tmp_path / "late.py"
    with pytest.raises(FileNotFoundError):
        get_function_ref(str(mod), "job")
    mod.write_text("async def job():\n    pass\n")
    assert get_function_ref(str(mod), "job").__name__ == "job"
```

- [ ] **Step 2:** `python -m pytest tests/test_utils_misc.py -q` → Expected: FAIL (`AttributeError: 'function' object has no attribute 'cache_clear'`).

- [ ] **Step 3: Implement** in `src/async_q/utils.py`:

```python
@functools.lru_cache(maxsize=None)  # ponytail: unbounded; one entry per task function, bounded by codebase size
def get_function_ref(path, func_name):
    module = get_module_ref(path)
    return getattr(module, func_name)
```

(`functools` is already imported. Leave `get_module_ref` uncached — `__main__.resolve_app` uses it directly.)

- [ ] **Step 4:** `python -m pytest -q` → Expected: all pass (14).
- [ ] **Step 5:** Check B4 in `IMPROVEMENTS.md`, commit:

```bash
git add src/async_q/utils.py tests/test_utils_misc.py IMPROVEMENTS.md
git commit -m "fix: cache task function lookup instead of re-executing module per task"
```

---

### Task 2 (B3): Reject poison messages to DLQ instead of crashing

**Files:**
- Create: `tests/fakes.py` (move `FakeAsyncRedis`, `FakeTask`, `NoopLogger` out of `tests/test_retry_and_dlq.py`)
- Modify: `tests/test_retry_and_dlq.py` (import from `fakes`, delete local copies; also delete the junk line `assert dlq_key in redis.llists if False else True  # placeholder to avoid linter`)
- Modify: `src/async_q/async_worker.py` (`listen_to_submitted_task`; add `_reject`, `_process_message`)
- Test: `tests/test_process_message.py` (create)

**Interfaces:**
- Produces: `async def _process_message(value: bytes, distribute_qname: str, redis, queue: asyncio.Queue, logger) -> None`
- Produces: `async def _reject(value: bytes, distribute_qname: str, redis, logger, reason: str, meta: Optional[TaskMetaData] = None) -> None`
- Produces (tests): `tests/fakes.py` exporting `FakeAsyncRedis`, `FakeTask`, `NoopLogger`. Tests import with `from fakes import ...` (pytest's default `prepend` import mode puts `tests/` on `sys.path` since it has no `__init__.py`).

- [ ] **Step 1: Move fakes.** Cut `FakeAsyncRedis`, `FakeTask`, `_NoopLogger` from `tests/test_retry_and_dlq.py` into `tests/fakes.py`, renaming `_NoopLogger` → `NoopLogger`. In `test_retry_and_dlq.py` add `from fakes import FakeAsyncRedis, FakeTask, NoopLogger` and replace `_NoopLogger()` → `NoopLogger()`. Run `python -m pytest -q` → Expected: still passes.

- [ ] **Step 2: Write the failing tests** (`tests/test_process_message.py`):

```python
import asyncio

import msgpack
import pytest

from fakes import FakeAsyncRedis, NoopLogger
from async_q.async_worker import _process_message
from async_q.utils import (
    TaskMetaData, deserialize, serialize, get_function_ref,
    get_redis_q_backup_key, get_dead_letter_key, get_task_key,
)

Q = "default"


async def _run(payload, redis=None):
    redis = redis or FakeAsyncRedis()
    await redis.lpush(get_redis_q_backup_key(Q), payload)  # as brpoplpush would
    queue = asyncio.Queue()
    await _process_message(payload, Q, redis, queue, NoopLogger())
    return redis, queue


def _meta(path, func_name="job"):
    # built WITHOUT retry fields: simulates payload from an older producer
    return TaskMetaData(id="t1", path=path, func_name=func_name,
                        args=[], kwargs={}, status="submitted")


@pytest.mark.asyncio
async def test_valid_message_is_queued(tmp_path):
    get_function_ref.cache_clear()
    mod = tmp_path / "ok.py"
    mod.write_text("async def job():\n    pass\n")
    payload = serialize(_meta(str(mod)))
    redis, queue = await _run(payload)
    item = queue.get_nowait()
    assert item.original_data == payload
    assert deserialize(redis.kv[get_task_key("t1")]).status == "pending"
    assert payload in redis.lists[get_redis_q_backup_key(Q)]


@pytest.mark.asyncio
@pytest.mark.parametrize("payload", [b"\xc1not-msgpack", msgpack.packb({"foo": 1})])
async def test_undecodable_payload_goes_to_dlq(payload):
    redis, queue = await _run(payload)
    assert queue.empty()
    assert redis.lists[get_dead_letter_key(Q)] == [payload]
    assert payload not in redis.lists[get_redis_q_backup_key(Q)]


@pytest.mark.asyncio
async def test_missing_file_goes_to_dlq_with_failed_status(tmp_path):
    payload = serialize(_meta(str(tmp_path / "gone.py")))
    redis, queue = await _run(payload)
    assert queue.empty()
    assert redis.lists[get_dead_letter_key(Q)] == [payload]
    assert payload not in redis.lists[get_redis_q_backup_key(Q)]
    assert deserialize(redis.kv[get_task_key("t1")]).status == "failed"


@pytest.mark.asyncio
async def test_missing_attribute_goes_to_dlq(tmp_path):
    get_function_ref.cache_clear()
    mod = tmp_path / "noattr.py"
    mod.write_text("x = 1\n")
    payload = serialize(_meta(str(mod), "renamed"))
    redis, queue = await _run(payload)
    assert queue.empty()
    assert redis.lists[get_dead_letter_key(Q)] == [payload]


@pytest.mark.asyncio
async def test_sync_function_goes_to_dlq(tmp_path):
    get_function_ref.cache_clear()
    mod = tmp_path / "sync.py"
    mod.write_text("def job():\n    pass\n")
    payload = serialize(_meta(str(mod)))
    redis, queue = await _run(payload)
    assert queue.empty()
    assert redis.lists[get_dead_letter_key(Q)] == [payload]
```

- [ ] **Step 3:** `python -m pytest tests/test_process_message.py -q` → Expected: FAIL (`ImportError: cannot import name '_process_message'`).

- [ ] **Step 4: Implement.** In `src/async_q/async_worker.py`, add above `listen_to_submitted_task`:

```python
async def _reject(value, distribute_qname: str, redis: aioredis.Redis, logger, reason: str, meta: Optional[TaskMetaData] = None):
    """Move an unprocessable payload from backup to the DLQ so it can't crash the worker again."""
    logger.error('rejecting task to DLQ: %s', reason)
    await redis.lrem(get_redis_q_backup_key(distribute_qname), count=1, value=value)
    await redis.lpush(get_dead_letter_key(distribute_qname), value)
    if meta is not None:
        meta.status = 'failed'
        await redis.set(get_task_key(meta.id), serialize(meta))


async def _process_message(value, distribute_qname: str, redis: aioredis.Redis, queue: asyncio.Queue, logger):
    try:
        data = deserialize(value)
    except Exception as e:
        await _reject(value, distribute_qname, redis, logger, f'undecodable payload: {e!r}')
        return

    logger.debug("deserialize received value : %s", data)

    try:
        fun = await to_thread(get_function_ref, data.path, data.func_name)
    except Exception as e:
        await _reject(value, distribute_qname, redis, logger, f'cannot load {data.path}:{data.func_name}: {e!r}', data)
        return

    if not inspect.iscoroutinefunction(fun):
        await _reject(value, distribute_qname, redis, logger, f'{data.func_name} is not a coroutine function', data)
        return

    data.status = 'pending'
    await redis.set(get_task_key(data.id), serialize(data))
    await queue.put(QueueItem(func_ref=fun, deserialize_data=data, original_data=value))
```

Then in `listen_to_submitted_task`, replace everything after the `if value is None: ... continue` block (from `data = deserialize(value)` to the end of the loop body, including the `isinstance`/`assert` branch) with:

```python
        await _process_message(value, distribute_qname, redis, queue, logger)
```

Behavior change to note in the commit: sync functions now go to DLQ (previously silently dropped).

- [ ] **Step 5:** `python -m pytest -q` → Expected: all pass.
- [ ] **Step 6:** Check B3 in `IMPROVEMENTS.md`, commit:

```bash
git add src/async_q/async_worker.py tests/fakes.py tests/test_retry_and_dlq.py tests/test_process_message.py IMPROVEMENTS.md
git commit -m "fix: send unprocessable payloads to DLQ instead of crashing worker

Undecodable payloads, unloadable functions and non-coroutine functions
are now removed from the backup queue and pushed to the DLQ. Previously
they raised out of the producer loop (stopping the worker) and were
replayed on every restart. Sync functions, previously dropped silently,
now land in the DLQ too."
```

---

### Task 3 (B8): Raise when worker starts without an app

**Files:**
- Modify: `src/async_q/async_worker.py` (`async_worker`, first lines)
- Test: `tests/test_async_worker_queue.py` (append)

- [ ] **Step 1: Failing test** (append):

```python
@pytest.mark.asyncio
async def test_async_worker_requires_initialized_app(reset_async_task_queue):
    from async_q.async_worker import async_worker
    with pytest.raises(RuntimeError, match="not initialized"):
        await async_worker()
```

- [ ] **Step 2:** `python -m pytest tests/test_async_worker_queue.py -q` → Expected: FAIL (`AttributeError: 'NoneType' object has no attribute 'redis_builder'`).
- [ ] **Step 3: Implement** — replace:

```python
    if not AsyncTaskQueue.get_instance():
        Exception('AsyncTaskQueue did not initiated')
```

with:

```python
    if not AsyncTaskQueue.get_instance():
        raise RuntimeError('AsyncTaskQueue is not initialized')
```

- [ ] **Step 4:** `python -m pytest -q` → all pass.
- [ ] **Step 5:** Check B8, commit `fix: raise when async_worker starts without an AsyncTaskQueue`.

---

### Task 4 (B2): Expose retry options on `submit_task`

**Files:**
- Modify: `src/async_q/async_q.py` (`submit_task`)
- Modify: `README.md` (retry comment in "Submitting Tasks", ~line 76)
- Test: `tests/test_async_q_core.py` (append)

**Interfaces:**
- Produces: `submit_task(func, args=None, kwargs=None, queue_name='default', max_retries=0, backoff_base=1.0, backoff_factor=2.0, backoff_max=60.0) -> None`. Existing positional order unchanged.

- [ ] **Step 1: Failing tests** (append; reuse `FakeRedis` in that file):

```python
def _app(monkeypatch):
    import async_q.async_q as aq
    monkeypatch.setattr(aq.redis, "Redis", FakeRedis)
    rb = RedisBuilder()
    AsyncTaskQueue(redis_builder=rb)
    return rb


def test_submit_task_sets_retry_options(monkeypatch, reset_async_task_queue):
    rb = _app(monkeypatch)

    async def t():
        pass

    submit_task(t, max_retries=3, backoff_base=0.5, backoff_factor=3.0, backoff_max=10.0)
    data = deserialize(rb.get_redis().lists[get_redis_q_key("default")][0])
    assert (data.max_retries, data.backoff_base, data.backoff_factor, data.backoff_max) == (3, 0.5, 3.0, 10.0)
    assert data.attempt == 0


def test_submit_task_rejects_negative_retries(monkeypatch, reset_async_task_queue):
    rb = _app(monkeypatch)

    async def t():
        pass

    with pytest.raises(ValueError):
        submit_task(t, max_retries=-1)
    assert rb.get_redis().lists == {}
```

- [ ] **Step 2:** `python -m pytest tests/test_async_q_core.py -q` → Expected: FAIL (`TypeError: submit_task() got an unexpected keyword argument 'max_retries'`).
- [ ] **Step 3: Implement** — new signature and body of `submit_task`:

```python
def submit_task(
    func: Callable,
    args: Optional[List[object]] = None,
    kwargs: Optional[Dict[str, object]] = None,
    queue_name: str = 'default',
    max_retries: int = 0,
    backoff_base: float = 1.0,
    backoff_factor: float = 2.0,
    backoff_max: float = 60.0,
) -> None:
    """Submit a coroutine function for asynchronous execution.

    Parameters:
        func: async callable to execute on workers
        args: positional arguments for the task
        kwargs: keyword arguments for the task
        queue_name: target queue name
        max_retries: retries after the first failure before moving to the DLQ
        backoff_base: delay in seconds before the first retry
        backoff_factor: multiplier applied to the delay on each further retry
        backoff_max: upper bound on the retry delay in seconds
    """
    if not AsyncTaskQueue._instance:
        raise Exception('AsyncTaskQueue did not initiated')

    if not inspect.iscoroutinefunction(func):
        raise TypeError('submit_task requires an async function (coroutine)')

    if max_retries < 0:
        raise ValueError('max_retries must be >= 0')

    r = AsyncTaskQueue._instance.redis_builder.get_redis()

    value = {
        'id': uuid.uuid4().hex,
        'path': inspect.getfile(func),
        'func_name': func.__name__,
        'args': [] if args is None else args,
        'kwargs': {} if kwargs is None else kwargs,
        'status': 'submitted',
        'max_retries': max_retries,
        'backoff_base': backoff_base,
        'backoff_factor': backoff_factor,
        'backoff_max': backoff_max,
    }
    byte_data: bytes = serialize(TaskMetaData(**value))
    r.lpush(get_redis_q_key(queue_name), byte_data)
```

- [ ] **Step 4: README** — replace the line
  `# You can control retries by setting defaults in your task payload later (max_retries, backoff_* fields)`
  with:

```python
         # Retry up to 3 times (delays 1s, 2s, 4s, capped at backoff_max) before moving to the DLQ
         submit_task(my_task, kwargs={'idx': i}, max_retries=3, backoff_base=1.0, backoff_factor=2.0, backoff_max=60.0)
```

- [ ] **Step 5:** `python -m pytest -q` → all pass.
- [ ] **Step 6:** Check B2, commit `feat: expose retry/backoff options on submit_task`.

---

### Task 5 (B7): Keep references to done-handler tasks

**Files:**
- Modify: `src/async_q/async_worker.py` (`task_done_callback`)
- Test: `tests/test_retry_and_dlq.py` (append)

**Interfaces:**
- Produces: module-level `_background_tasks: set` in `async_worker.py` (Batch 2 / B6 will await it at shutdown).

- [ ] **Step 1: Failing test** (append):

```python
@pytest.mark.asyncio
async def test_task_done_callback_holds_reference_until_done():
    from async_q import async_worker as aw
    redis = FakeAsyncRedis()
    meta = TaskMetaData(id='t3', path='/x.py', func_name='f', args=[], kwargs={}, status='starting')
    payload = serialize(meta)
    item = QueueItem(func_ref=None, deserialize_data=meta, original_data=payload)

    class DummyQ:
        def task_done(self):
            pass

    aw.task_done_callback(FakeTask(result=item), DummyQ(), 'default', redis, NoopLogger(), item)
    assert len(aw._background_tasks) == 1
    await asyncio.gather(*aw._background_tasks)
    await asyncio.sleep(0)  # let discard callback run
    assert len(aw._background_tasks) == 0
```

- [ ] **Step 2:** `python -m pytest tests/test_retry_and_dlq.py -q` → Expected: FAIL (`AttributeError: ... has no attribute '_background_tasks'`).
- [ ] **Step 3: Implement** — replace `task_done_callback` with:

```python
# Strong refs so the event loop can't GC done-handlers mid-flight
_background_tasks: set = set()


def task_done_callback(task, queue: AsyncQueue, distribute_qname: str, redis: aioredis.Redis, logger, item: QueueItem):
    t = asyncio.create_task(_task_done(task, queue, distribute_qname, redis, logger, item))
    _background_tasks.add(t)
    t.add_done_callback(_background_tasks.discard)
```

- [ ] **Step 4:** `python -m pytest -q` → all pass.
- [ ] **Step 5:** Check B7, commit `fix: hold references to task-done handler tasks`.

---

### Task 6 (B9): CLI `main()` on a fresh event loop + console script

**Files:**
- Modify: `src/async_q/__main__.py` (wrap `if __name__` body in `main(argv=None)`)
- Modify: `pyproject.toml` (add `[project.scripts]`)
- Modify: `README.md` (mention `async_q` command next to `python -m async_q` in "Initializing Worker Processes")
- Test: `tests/test_cli_app_resolution.py` (append)

**Interfaces:**
- Produces: `async_q.__main__.main(argv: Optional[List[str]] = None) -> None`; console script `async_q`.

- [ ] **Step 1: Failing tests** (append):

```python
import asyncio


def test_main_requires_app(monkeypatch):
    from async_q import __main__ as cli
    monkeypatch.delenv("ASYNC_Q_APP", raising=False)
    with pytest.raises(SystemExit):
        cli.main([])


def test_main_runs_worker_on_fresh_loop(tmp_path, monkeypatch, reset_async_task_queue):
    from async_q import __main__ as cli
    app_file = tmp_path / "app_main.py"
    app_file.write_text(
        "from async_q import AsyncTaskQueue, RedisBuilder\n"
        "app = AsyncTaskQueue(redis_builder=RedisBuilder())\n"
    )
    seen = {}

    async def fake_worker(stop_event=None):
        seen["stop_event"] = stop_event
        seen["loop"] = asyncio.get_running_loop()

    async def fake_close():
        pass

    monkeypatch.setattr(cli, "async_worker", fake_worker)
    monkeypatch.setattr(cli, "close", fake_close)
    cli.main(["-a", f"{app_file}:app", "-q", "mail", "-c", "3"])

    assert isinstance(seen["stop_event"], asyncio.Event)
    assert seen["loop"].is_closed()
    assert cli.AsyncTaskQueue.get_instance().distribute_qname == "mail"
```

- [ ] **Step 2:** `python -m pytest tests/test_cli_app_resolution.py -q` → Expected: FAIL (`AttributeError: module 'async_q.__main__' has no attribute 'main'`).
- [ ] **Step 3: Implement** — replace from `if __name__ == "__main__":` to end of `src/async_q/__main__.py` with:

```python
def main(argv: Optional[List[str]] = None) -> None:
    parser = argparse.ArgumentParser(description="Async task queue worker")
    parser.add_argument(
        "-a",
        "--app",
        required=False,
        help=(
            "Application spec as file path or dotted module path, e.g.:\n"
            "  - file:  -a app.py:async_q_app\n"
            "  - module: -a mypkg.app:async_q_app\n"
            "Can also be provided via ASYNC_Q_APP environment variable."
        ),
    )
    parser.add_argument('-c', '--concurrency', default=100, type=int,
                        required=False,
                        help='limit the number of concurrent execution, default is 100.')
    parser.add_argument('-q', '--queue', default='default', type=str)
    parser.add_argument('-ll', '--log_level', default='INFO', type=str,
                        required=False,
                        choices=list(logging._nameToLevel.keys()),
                        help='log level `-ll INFO`, default loglevel is INFO')

    parser.add_argument('-lf', '--log_filename', default=None, type=str,
                        required=False,
                        help='set log file path `-lf ./logs/async-q.log`')

    args = parser.parse_args(argv)

    app_spec = args.app or os.getenv('ASYNC_Q_APP')
    if not app_spec:
        raise SystemExit("--app is required (or set ASYNC_Q_APP)")

    async_task_queue = resolve_app(app_spec)

    async_task_queue.set_concurrency(args.concurrency)

    async_task_queue.config_logger(
        log_level=logging.getLevelName(args.log_level),
        log_filename=args.log_filename
    )
    async_task_queue.distribute_qname = args.queue

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        async_task_queue.logger.info(f'Starting workers with {args.concurrency} concurrency')

        stop_event = asyncio.Event()

        def _request_shutdown():
            if not stop_event.is_set():
                async_task_queue.logger.info('shutdown signal received; stopping...')
                stop_event.set()

        # Register signal handlers (Windows supports CTRL_C_EVENT → KeyboardInterrupt, but keep for POSIX)
        try:
            loop.add_signal_handler(signal.SIGINT, _request_shutdown)
            loop.add_signal_handler(signal.SIGTERM, _request_shutdown)
        except NotImplementedError:
            # Fallback for platforms without signal handler support in event loop
            pass

        loop.run_until_complete(
            async_worker(stop_event=stop_event)
        )
    except KeyboardInterrupt:
        async_task_queue.logger.info('program closing..')
    except Exception as e:
        async_task_queue.logger.error("__main__: %s", e)
    finally:
        loop.run_until_complete(close())
        loop.close()


if __name__ == "__main__":
    main()
```

And add to imports at top: `from typing import List, Optional`.

Note: `asyncio.Event()` is created after `set_event_loop`, so on 3.8/3.9 it binds to the right loop.

- [ ] **Step 4: pyproject** — add after `dependencies`:

```toml
[project.scripts]
async_q = "async_q.__main__:main"
```

- [ ] **Step 5: README** — under "#### 4. Initializing Worker Processes", after the first `python -m async_q ...` block, add:

```markdown
Installing the package also provides an `async_q` command, equivalent to `python -m async_q`:

```bash
async_q -a main_app.py:async_q_app -c 5
```
```

- [ ] **Step 6:** `python -m pytest -q` → all pass. Then `pip install -e . && async_q --help` → Expected: argparse help text.
- [ ] **Step 7:** Check B9, commit `fix: run CLI on a fresh event loop and add async_q console script`.

---

### Task 7: Wrap-up

- [ ] **Step 1:** `python -m pytest -q` → all pass; `git log --oneline main..` shows one commit per task.
- [ ] **Step 2:** Use superpowers:finishing-a-development-branch to decide merge/PR.
