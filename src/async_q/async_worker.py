import asyncio
import collections
import inspect
import copy
import time
from typing import Optional

from redis.exceptions import RedisError

from . import scripts
from .async_q import AsyncTaskQueue
from .utils import (
    QueueItem,
    TaskMetaData,
    deserialize,
    get_function_ref,
    serialize,
    get_redis_q_key,
    get_redis_q_backup_key,
    get_task_key,
    to_thread,
    get_retry_zset_key,
    get_dead_letter_key,
    get_heartbeat_key,
    get_workers_key,
    new_worker_id,
    get_legacy_keys,
)


def _next_backoff(current: float) -> float:
    return min(current * 2, 30)


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
        await (getattr(r, 'aclose', None) or r.close)()  # aclose: redis-py >=5.0.1


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
        # In-flight copies per payload. >1 means peers reclaimed our backup during an outage and we
        # fetched the same bytes again: one backup entry, so only the last copy to finish may ack it.
        self.copies: collections.Counter = collections.Counter()
        self.background: set = set()  # strong refs: done-handlers must not be GC'd mid-flight

    async def set_status(self, meta: TaskMetaData, status: str) -> None:
        meta.status = status
        await self.redis.set(get_task_key(meta.id), serialize(meta), ex=self.status_ttl)

    async def reject(self, value, reason: str, meta: Optional[TaskMetaData] = None) -> None:
        """Move an unprocessable payload from backup to the DLQ so it can't crash the worker again."""
        self.logger.error('rejecting task to DLQ: %s', reason)
        if await scripts.fail_ack(self.redis, self.q, self.wid, value, value, 'dead') and meta is not None:
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
            if self._release_copy(item.original_data):
                self.logger.warning('task %s: another in-flight copy of this payload (own task, reclaimed and '
                                    're-fetched) still owns the backup entry; skipping ack', meta.id)
                return
            try:
                task.result()
            except asyncio.CancelledError:
                # Front of the queue: it was already running, re-run it first.
                if await scripts.requeue(self.redis, self.q, self.wid, item.original_data):
                    await self.set_status(meta, 'submitted')
                    self.logger.info('task %s cancelled; re-queued', meta.id)
                else:
                    self._warn_reclaimed(meta)
                return
            except Exception as e:
                self.logger.error('task %s failed: %r', meta.id, e)
                self.logger.debug('traceback', exc_info=e)
                retry = meta.attempt < meta.max_retries
                if retry:
                    meta.attempt += 1
                    delay = min(meta.backoff_base * (meta.backoff_factor ** (meta.attempt - 1)), meta.backoff_max)
                    meta.next_retry_at = time.time() + delay
                meta.status = 'failed'
                # Re-serialize so the bumped attempt count travels with the payload
                if not await scripts.fail_ack(self.redis, self.q, self.wid, item.original_data, serialize(meta),
                                              'retry' if retry else 'dead', meta.next_retry_at if retry else 0):
                    self._warn_reclaimed(meta)
                    return
                await self.set_status(meta, 'failed')
                if retry:
                    self.logger.info('scheduled retry #%s in %.2fs for %s', meta.attempt, delay, meta.id)
                else:
                    self.logger.warning('moved task %s to DLQ after %s attempts', meta.id, meta.attempt)
                return
            if await self.redis.lrem(self.backup_key, 1, item.original_data):
                await self.set_status(meta, 'finished')
            else:
                self._warn_reclaimed(meta)
        finally:
            self.sem.release()

    def _release_copy(self, value) -> bool:
        """Drop one in-flight copy of value; True if another copy remains to ack the backup entry."""
        if self.copies[value] > 1:
            self.copies[value] -= 1
            return True
        self.copies.pop(value, None)
        return False

    def _warn_reclaimed(self, meta: TaskMetaData) -> None:
        self.logger.warning('task %s was reclaimed by another worker while running; it may run twice', meta.id)

    async def fetch_loop(self) -> None:
        """Pull work only when a concurrency slot is free. Runs until cancelled."""
        backoff = 1.0
        fetched = None  # payload moved to our backup but not yet turned into an item
        while True:
            await self.sem.acquire()
            try:
                fetched = await self.redis.brpoplpush(self.main_key, self.backup_key, timeout=1)
                item = await self.process_message(fetched) if fetched is not None else None
                fetched = None
            except RedisError as e:
                self.sem.release()
                self.logger.warning('redis unavailable (%r); retrying in %.0fs', e, backoff)
                await asyncio.sleep(backoff)
                backoff = _next_backoff(backoff)
                while fetched is not None:
                    # Best effort: hand the stranded payload back to main. If the reply was lost
                    # after the server already moved it, it stays in backup until release() at shutdown.
                    try:
                        await scripts.requeue(self.redis, self.q, self.wid, fetched)
                        fetched = None
                    except RedisError:
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
        self.copies[item.original_data] += 1
        task.add_done_callback(lambda t, _item=item: self._on_done(t, _item))

    def _on_done(self, task, item: QueueItem) -> None:
        self.inflight.discard(task)
        bg = asyncio.create_task(self.task_done(task, item))
        self.background.add(bg)
        bg.add_done_callback(self.background.discard)
        bg.add_done_callback(lambda t, _id=item.deserialize_data.id: self._log_ack_error(t, _id))

    def _log_ack_error(self, bg, task_id) -> None:
        if not bg.cancelled() and bg.exception() is not None:
            self.logger.error('finishing task %s failed: %r', task_id, bg.exception())

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
