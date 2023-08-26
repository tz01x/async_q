import inspect
from typing import Self

import redis

from async_q.utils import serialize


class RedisBuilder:
    __redis_sync = None
    __redis_async = None

    def __init__(self, host: str = 'localhost', port: int = 6379, username: str = None, password: str = None, **kwargs):
        self.kwargs = {
            'host': host,
            'port': port,
            'password': password,
            'username': username,
            **kwargs
        }

    def get_redis(self) -> redis.Redis:
        if not self.__redis_sync:
            self.__redis_sync = redis.Redis(**self.kwargs)
        return self.__redis_sync

    def get_redis_async(self) -> redis.asyncio.Redis:
        if not self.__redis_async:
            self.__redis_async = redis.asyncio.Redis(**self.kwargs)
        return self.__redis_async


class AsyncTaskQueue:
    instance: Self = None

    def __init__(self, redis_builder: RedisBuilder):
        self.redis_builder = redis_builder

        if not AsyncTaskQueue.instance:
            AsyncTaskQueue.instance = self


def async_task(func, args: list = [], kwargs: dict = {}):
    # assert AsyncTaskQueue.instance is None, 'AsyncTaskQueue did not initiated'
    if not AsyncTaskQueue.instance:
        raise Exception('AsyncTaskQueue did not initiated')

    r = AsyncTaskQueue.instance.redis_builder.get_redis()

    value = {
        'path': str(inspect.getfile(func)),
        'func_name': func.__name__,
        'args': args,
        'kwargs': kwargs,
    }
    byte_data = serialize(value)
    r.lpush('asynctask', byte_data)
