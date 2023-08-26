import asyncio
import importlib.util
import inspect
import logging
import pathlib
import traceback

import redis.asyncio as aioredis

from .async_q import AsyncTaskQueue
from .utils import deserialize

all_tasks = set()


def get_module(path):
    module_path = pathlib.Path(path)

    logging.debug('module spec : %s',  str(module_path))
    spec = importlib.util.spec_from_file_location(
        module_path.stem, module_path)
    # Create the module object from the spec
    module = importlib.util.module_from_spec(spec)

    # Load the module
    spec.loader.exec_module(module)
    return module


def get_function(path, func_name):
    module = get_module(path)
    return getattr(module, func_name)


async def get_submitted_task(redis: aioredis.Redis):

    while True:
        (key, value) = await redis.brpop('asynctask')
        data = deserialize(value)
        logging.debug("deserialize received value : %s", data)
        fun = await asyncio.to_thread(get_function, data['path'], data['func_name'])
        logging.debug('get function ref %s', str(fun))
        if fun:
            if inspect.iscoroutinefunction(fun):
                coro = fun(*data['args'], **data['kwargs'])
                logging.info('creating task')
                task = asyncio.create_task(coro=coro)
                all_tasks.add(task)
            else:
                logging.info('submitted function is not coroutine')
        else:
            logging.debug('skipped from creating task')
            await asyncio.sleep(10)


async def async_worker():
    if not AsyncTaskQueue.instance:
        Exception('AsyncTaskQueue did not initiated')

    r = AsyncTaskQueue.instance.redis_builder.get_redis_async()
    try:
        await get_submitted_task(r)
    except Exception as e:
        logging.error(e)
        logging.debug('traceback:', traceback.format_exc() )
    finally:
        await r.close()
