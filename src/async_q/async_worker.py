import asyncio
import inspect
import logging
import re
import traceback
import copy
from typing import Any, Dict, Optional

import redis.asyncio as aioredis

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
)


class AsyncQueue(asyncio.Queue):

    def full(self) -> bool:
        isFull = super().full()
        async_q_task = [t.get_name() for t in asyncio.all_tasks(
        ) if re.match(get_task_key('.*'), t.get_name())]
        logging.debug(f'async_q_task count {len(async_q_task)}')
        logging.debug(
            f'AsyncQueue is Full  -> {isFull or len(async_q_task) >= self.maxsize}')
        return isFull or len(async_q_task) >= self.maxsize

    def task_done(self) -> None:
        super().task_done()
        self._wakeup_next(self._putters)

    def get_nowait(self):
        """Remove and return an item from the queue.

        Return an item if one is immediately available, else raise QueueEmpty.
        """
        if self.empty():
            raise asyncio.QueueEmpty
        item = self._get()
        return item

async def _task_done(task: asyncio.Task, queue: AsyncQueue, distribute_qname: str, redis: aioredis.Redis, logger, item: QueueItem):
    try:
        # Success path returns the same item
        returned_item: QueueItem = task.result()
        data = copy.deepcopy(returned_item.deserialize_data)
        logger.debug(
            f"removing {returned_item.original_data} from {get_redis_q_backup_key(distribute_qname)}")
        r = await redis.lrem(get_redis_q_backup_key(distribute_qname), count=0, value=returned_item.original_data)
        logger.debug(f"removed item from backup queue & status is : {r}")
        data.status = 'finished'
        await redis.set(get_task_key(data.id), serialize(data), ex=60 * 60)
    except asyncio.CancelledError:
        # Requeue the item for future processing
        logger.info("task cancelled, re-queueing item")
        meta = copy.deepcopy(item.deserialize_data)
        meta.status = 'submitted'
        await redis.set(get_task_key(meta.id), serialize(meta))
        # Move from backup to main (remove specific value then LPUSH to main)
        await redis.lrem(get_redis_q_backup_key(distribute_qname), count=1, value=item.original_data)
        await redis.lpush(get_redis_q_key(distribute_qname), item.original_data)
    except Exception as e:
        # Failure: compute retry or DLQ
        logger.error("task failed: %s", e)
        logger.debug('%s', traceback.format_exc())
        meta = copy.deepcopy(item.deserialize_data)
        meta.status = 'failed'
        # Remove from backup queue
        await redis.lrem(get_redis_q_backup_key(distribute_qname), count=1, value=item.original_data)

        if meta.attempt < meta.max_retries:
            meta.attempt += 1
            # Exponential backoff with jitter can be added; keep simple deterministic for now
            delay = min(meta.backoff_base * (meta.backoff_factor ** (meta.attempt - 1)), meta.backoff_max)
            import time
            score = time.time() + delay
            meta.next_retry_at = score
            await redis.set(get_task_key(meta.id), serialize(meta))
            await redis.zadd(get_retry_zset_key(distribute_qname), {item.original_data: score})
            logger.info('scheduled retry #%s in %.2fs for %s', meta.attempt, delay, meta.id)
        else:
            # Send to DLQ
            await redis.set(get_task_key(meta.id), serialize(meta))
            await redis.lpush(get_dead_letter_key(distribute_qname), item.original_data)
            logger.warning('moved task %s to DLQ after %s attempts', meta.id, meta.attempt)
    finally:
        queue.task_done()

def task_done_callback(task, queue: AsyncQueue, distribute_qname: str, redis: aioredis.Redis, logger, item: QueueItem):
    asyncio.create_task(_task_done(task, queue, distribute_qname, redis, logger, item))


async def _task_inner(tkey, func, item: QueueItem, redis, logger):
    meta_data = copy.deepcopy(item.deserialize_data)
    meta_data.status = 'starting'
    await redis.set(tkey, serialize(meta_data))
    logger.debug(f'starting a new task; task_id: {tkey}')
    coro = func(*meta_data.args, **{**meta_data.kwargs, 'task_id': tkey})
    await coro
    return item

async def create_async_task_from_queue(distribute_qname: str, redis: aioredis.Redis, queue: AsyncQueue, logger, stop_event: asyncio.Event):
    while True:
        # Try to get a unit of work with a short timeout so we can react to stop_event
        if stop_event.is_set() and queue.empty():
            break
        try:
            item: QueueItem = await asyncio.wait_for(queue.get(), timeout=0.5)
        except asyncio.TimeoutError:
            continue

        func_ref = item.func_ref
        task_key = get_task_key(item.deserialize_data.id)


        logger.info(f'creating a new task: {task_key}')
        task = asyncio.create_task(
            coro=_task_inner(task_key, func_ref, item,redis, logger), name=task_key)

        task.add_done_callback(lambda t, _item=item: task_done_callback(t, queue, distribute_qname, redis, logger, _item))


async def listen_to_submitted_task(distribute_qname: str, redis: aioredis.Redis, queue: AsyncQueue, logger, stop_event: asyncio.Event):

    # get the unfinished task form the backup queue
    while True:
        value = await redis.rpoplpush(get_redis_q_backup_key(distribute_qname), get_redis_q_key(distribute_qname))
        if not value:
            break

    while not stop_event.is_set():
        value = await redis.brpoplpush(
            get_redis_q_key(distribute_qname),
            get_redis_q_backup_key(distribute_qname),
            timeout=1,
        )

        if value is None:
            # Also check retry ZSET for due items
            import time
            now = time.time()
            try:
                # Fetch one due item
                due = await redis.zrangebyscore(get_retry_zset_key(distribute_qname), min=-1, max=now, start=0, num=1)
                if due:
                    payload = due[0]
                    # Move to backup and remove from ZSET
                    await redis.zrem(get_retry_zset_key(distribute_qname), payload)
                    await redis.lpush(get_redis_q_key(distribute_qname), payload)
                    # Continue loop to process as normal
            except Exception:
                pass
            continue

        data = deserialize(value)

        if not isinstance(data, TaskMetaData):
            assert 1 == await redis.lrem(get_redis_q_backup_key(distribute_qname), count=1, value=value), "can not remove invalid data from q backup"
            logging.error("submitted task is not dict type")

        logger.debug("deserialize received value : %s", data)

        fun = await to_thread(get_function_ref, data.path, data.func_name)
        logger.debug('get function ref %s', str(fun))

        if fun and inspect.iscoroutinefunction(fun):
            data.status = 'pending'
            await redis.set(get_task_key(data.id), serialize(data))
            await queue.put(QueueItem(func_ref=fun, deserialize_data=data, original_data=value))
        else:
            logger.info('submitted function is not coroutine')
            logger.debug('skipped from creating task')

            await redis.lrem(get_redis_q_backup_key(distribute_qname), count=1, value=value)


async def async_worker(stop_event: Optional[asyncio.Event] = None):
    if not AsyncTaskQueue.get_instance():
        Exception('AsyncTaskQueue did not initiated')

    async_task_q = AsyncTaskQueue.get_instance()
    r = async_task_q.redis_builder.get_redis_async()
    queue = AsyncQueue(maxsize=async_task_q.get_concurrency())
    if stop_event is None:
        stop_event = asyncio.Event()
    try:
        p_task = None
        c_task = None

        producer = listen_to_submitted_task(
            async_task_q.distribute_qname, r, queue, async_task_q.logger, stop_event)
        consumer = create_async_task_from_queue(
            async_task_q.distribute_qname, r, queue, async_task_q.logger, stop_event)

        p_task = asyncio.create_task(producer)
        c_task = asyncio.create_task(consumer)

        await asyncio.gather(p_task, c_task)

    except Exception as e:
        async_task_q.logger.error(e)
        async_task_q.logger.debug('%s', traceback.format_exc())
    finally:
        # Safely cancel tasks if they were created
        try:
            if p_task is not None:
                p_task.cancel()
            if c_task is not None:
                c_task.cancel()
        except Exception:
            pass

        # Requeue any remaining items in backup back to main queue and set status to submitted
        try:
            while True:
                moved = await r.rpoplpush(
                    get_redis_q_backup_key(async_task_q.distribute_qname),
                    get_redis_q_key(async_task_q.distribute_qname),
                )
                if not moved:
                    break
                try:
                    meta = deserialize(moved)
                    meta.status = 'submitted'
                    # When shutting down, retry attempts remain as-is
                    await r.set(get_task_key(meta.id), serialize(meta))
                except Exception:
                    # best effort
                    pass
        except Exception:
            pass

        await r.close()
