import asyncio
import inspect
import logging
import re
import traceback

import redis.asyncio as aioredis

from .async_q import AsyncTaskQueue
from .utils import deserialize, get_function_ref, serialize


class AsyncQueue(asyncio.Queue):

    def full(self) -> bool:
        isFull = super().full()
        async_q_task = [t.get_name() for t in asyncio.all_tasks(
        ) if re.match('async-q-task:.*', t.get_name())]
        logging.debug(f' async_q_task count [queue.full] {len(async_q_task)}')
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


def task_done_callback(task, queue: AsyncQueue, redis, item):
    data = item['deserialize_data']

    async def inner():
        await redis.lrem('asynctask_backup', count=1, value=item['original_data'])
        await redis.set('async-q-task:'+data['id'], serialize({**data, 'status': 'finished'}))
        async_q_task = [t.get_name() for t in asyncio.all_tasks(
        ) if re.match('async-q-task:.*', t.get_name())]
        logging.debug(f' async_q_task count [task_done] {len(async_q_task)}')
        queue.task_done()
    asyncio.create_task(inner())


async def create_async_task_from_queue(redis: aioredis.Redis, queue: AsyncQueue):
    while True:
        # get a unit of work
        item = await queue.get()
        if not item:
            break

        data = item['deserialize_data']
        fun = item['func_ref']
        task_id = 'async-q-task:'+data['id']

        # await redis.set('const_task_'+data['id'], value)
        await redis.set(task_id, serialize({**data, 'status': 'starting'}))

        async def inner():
            await redis.incr('current-task-worker')
            logging.debug(f'starting a new task; task_id: {task_id}')
            coro = fun(*data['args'], **{**data['kwargs'], 'task_id': task_id})
            await coro

        logging.info('creating a new task..')
        task = asyncio.create_task(
            coro=inner(), name=task_id)

        task.add_done_callback(lambda t: task_done_callback(
            t, queue, redis, item))


async def listen_to_submitted_task(redis: aioredis.Redis, queue: AsyncQueue):

    while True:
        value = await redis.brpoplpush('asynctask', 'asynctask_backup')

        data = deserialize(value)

        if not isinstance(data, dict):
            await redis.lrem('asynctask_backup', count=1, value=value)

        logging.debug("deserialize received value : %s", data)

        fun = await asyncio.to_thread(get_function_ref, data['path'], data['func_name'])
        logging.debug('get function ref %s', str(fun))

        if fun and inspect.iscoroutinefunction(fun):
            data = {**data, 'status': 'padding'}
            await redis.set('async-q-task:'+data['id'], serialize(data))
            await queue.put({'func_ref': fun, 'deserialize_data': data,
                             'original_data': value})
        else:
            logging.info('submitted function is not coroutine')
            logging.debug('skipped from creating task')

            await redis.lrem('asynctask_backup', count=1, value=value)


async def async_worker():
    if not AsyncTaskQueue.get_instance():
        Exception('AsyncTaskQueue did not initiated')

    async_task_q = AsyncTaskQueue.get_instance()
    r = async_task_q.redis_builder.get_redis_async()
    queue = AsyncQueue(maxsize=async_task_q.get_concurrency())
    try:
        producer = listen_to_submitted_task(r, queue)
        consumer = create_async_task_from_queue(r, queue)

        p_task = asyncio.create_task(producer)
        c_task = asyncio.create_task(consumer)

        await asyncio.gather(p_task, c_task)

    except Exception as e:
        logging.error(e)
        logging.debug('%s', traceback.format_exc())
    finally:
        p_task.cancel()
        c_task.cancel()

        await r.close()
