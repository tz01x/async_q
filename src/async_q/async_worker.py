import asyncio
import inspect
import logging
import re
import traceback
import copy
from typing import Any, Dict

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
    to_thread
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

async def _task_done(task:asyncio.Task, queue: AsyncQueue, distribute_qname: str, redis: aioredis.Redis, logger):
    item_:QueueItem = task.result()
    data = item_.deserialize_data
    data = copy.deepcopy(data)
    logger.debug(
        f"removing {item_.original_data} from {get_redis_q_backup_key(distribute_qname)}")
    r = await redis.lrem(get_redis_q_backup_key(distribute_qname), count=0, value=item_.original_data)
    logger.debug(f"removed item from backup queue & status is : {r}")
    data.status = 'finished'
    await redis.set(get_task_key(data.id), serialize(data), 60*60)
    # async_q_task = [t.get_name() for t in asyncio.all_tasks(
    # ) if re.match('async-q-task:.*', t.get_name())]
    # logger.debug(f' async_q_task count [task_done] {len(async_q_task)}')
    queue.task_done()

def task_done_callback(task, queue: AsyncQueue, distribute_qname: str, redis: aioredis.Redis, logger):
    asyncio.create_task(_task_done(task, queue, distribute_qname, redis, logger))


async def _task_inner(tkey, func, item:QueueItem, redis, logger):
    meta_data = copy.deepcopy(item.deserialize_data)
    meta_data.status = 'starting'
    await redis.set(tkey, serialize(meta_data))
    logger.debug(f'starting a new task; task_id: {tkey}')
    coro = func(*meta_data.args, **{**meta_data.kwargs, 'task_id': tkey})
    await coro
    return item

async def create_async_task_from_queue(distribute_qname: str, redis: aioredis.Redis, queue: AsyncQueue, logger):
    while True:
        # get a unit of work
        item: QueueItem = await queue.get()
        if not item:
            break

        func_ref = item.func_ref
        task_key = get_task_key(item.deserialize_data.id)


        logger.info(f'creating a new task: {task_key}')
        task = asyncio.create_task(
            coro=_task_inner(task_key, func_ref, item,redis, logger), name=task_key)

        task.add_done_callback(lambda t: task_done_callback(t, queue, distribute_qname, redis, logger))


async def listen_to_submitted_task(distribute_qname: str, redis: aioredis.Redis, queue: AsyncQueue, logger):

    # get the unfinished task form the backup queue
    while True:
        value = await redis.rpoplpush(get_redis_q_backup_key(distribute_qname), get_redis_q_key(distribute_qname))
        if not value:
            break

    while True:
        value = await redis.brpoplpush(get_redis_q_key(distribute_qname), get_redis_q_backup_key(distribute_qname))

        data = deserialize(value)

        if not isinstance(data, TaskMetaData):
            assert 1 == await redis.lrem(get_redis_q_backup_key(distribute_qname), count=1, value=value), "can not remove invalid data from q backup"
            logging.error("submitted task is not dict type")

        logger.debug("deserialize received value : %s", data)

        fun = await to_thread(get_function_ref, data.path, data.func_name)
        logger.debug('get function ref %s', str(fun))

        if fun and inspect.iscoroutinefunction(fun):
            data.status = 'pandding'
            await redis.set(get_task_key(data.id), serialize(data))
            await queue.put(QueueItem(func_ref=fun, deserialize_data=data, original_data=value))
        else:
            logger.info('submitted function is not coroutine')
            logger.debug('skipped from creating task')

            await redis.lrem(get_redis_q_backup_key(distribute_qname), count=1, value=value)


async def async_worker():
    if not AsyncTaskQueue.get_instance():
        raise Exception('AsyncTaskQueue did not initiated')

    async_task_q = AsyncTaskQueue.get_instance()
    r = async_task_q.redis_builder.get_redis_async()
    queue = AsyncQueue(maxsize=async_task_q.get_concurrency())
    try:
        producer = listen_to_submitted_task(
            async_task_q.distribute_qname, r, queue, async_task_q.logger)
        consumer = create_async_task_from_queue(
            async_task_q.distribute_qname, r, queue, async_task_q.logger)

        p_task = asyncio.create_task(producer)
        c_task = asyncio.create_task(consumer)

        await asyncio.gather(p_task, c_task)

    except Exception as e:
        async_task_q.logger.error(e)
        async_task_q.logger.debug('%s', traceback.format_exc())
    finally:
        p_task.cancel()
        c_task.cancel()

        await r.close()
