import inspect
import uuid
from typing import Callable

import redis

from async_q.utils import serialize,get_redis_q_key

import logging
from typing import Union


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
    _instance: "AsyncTaskQueue" = None

    def __init__(self, redis_builder: RedisBuilder):
        self.distribute_qname = 'default'
        self.redis_builder = redis_builder
        self._concurrency = 100
        self.logger = logging.getLogger('async-q')

        if not AsyncTaskQueue._instance:
            AsyncTaskQueue._instance = self 

    @classmethod
    def get_instance(cls):
        return cls._instance

    def set_concurrency(self, c: int):
        self._concurrency = c

    def get_concurrency(self):
        return self._concurrency

    def config_logger(self, log_level: int = logging.INFO, log_filename: Union[str, None] = None):
        # configure logger
        self.logger.setLevel(log_level)

        if log_filename is None:
            self.handler = logging.StreamHandler()
        else:
            self.handler = logging.FileHandler(filename=log_filename)
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s (%(filename)s:%(lineno)d)')
        self.handler.setFormatter(formatter)

        # Add the handler to the logger
        self.logger.addHandler(self.handler)


def submit_task(func: Callable, args: list = [], kwargs: dict = {}, queue_name:str = 'default'):
    '''
    submit a task to async queue worker 
    '''
    if not AsyncTaskQueue._instance:
        raise Exception('AsyncTaskQueue did not initiated')

    r = AsyncTaskQueue._instance.redis_builder.get_redis()

    value = {
        'id': uuid.uuid4().hex,
        'path': inspect.getfile(func),
        'func_name': func.__name__,
        'args': args,
        'kwargs': kwargs,
        'status': 'submitted',
    }
    byte_data = serialize(value)
    r.lpush(get_redis_q_key(queue_name), byte_data)
