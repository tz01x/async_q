import inspect
import uuid
from typing import Callable, List, Dict, Optional, Union

import redis

from async_q.utils import TaskMetaData, serialize, get_redis_q_key

import logging
from typing import Union



class RedisBuilder:
    __redis_sync = None
    __redis_async = None

    def __init__(self, host: str = 'localhost', port: int = 6379, username: Optional[str] = None, password: Optional[str] = None, **kwargs):
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

    def set_concurrency(self, c: int) -> None:
        self._concurrency = c

    def get_concurrency(self) -> int:
        return self._concurrency

    def config_logger(self, log_level: int = logging.INFO, log_filename: Optional[str] = None) -> None:
        """Configure library logger with a single handler.

        Repeated calls will replace existing handlers to avoid duplicates.
        """
        self.logger.setLevel(log_level)
        # Remove existing handlers to prevent duplicates
        for h in list(self.logger.handlers):
            self.logger.removeHandler(h)

        handler: logging.Handler
        if log_filename is None:
            handler = logging.StreamHandler()
        else:
            handler = logging.FileHandler(filename=log_filename)

        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s (%(filename)s:%(lineno)d)'
        )
        handler.setFormatter(formatter)
        self.logger.addHandler(handler)
        # Do not propagate to root logger by default
        self.logger.propagate = False


def submit_task(
    func: Callable,
    args: Optional[List[object]] = None,
    kwargs: Optional[Dict[str, object]] = None,
    queue_name: str = 'default',
) -> None:
    """Submit a coroutine function for asynchronous execution.

    Parameters:
        func: async callable to execute on workers
        args: positional arguments for the task
        kwargs: keyword arguments for the task
        queue_name: target queue name
    """
    if not AsyncTaskQueue._instance:
        raise Exception('AsyncTaskQueue did not initiated')

    r = AsyncTaskQueue._instance.redis_builder.get_redis()

    value = {
        'id': uuid.uuid4().hex,
        'path': inspect.getfile(func),
        'func_name': func.__name__,
        'args': [] if args is None else args,
        'kwargs': {} if kwargs is None else kwargs,
        'status': 'submitted',
    }
    byte_data = serialize(TaskMetaData(**value))
    r.lpush(get_redis_q_key(queue_name), byte_data)
