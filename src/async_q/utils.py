import asyncio
import contextvars
import functools
import importlib
import logging
import os
import pathlib
import secrets
import socket
from typing import Any, Dict, Optional

import msgpack

import dataclasses

@dataclasses.dataclass
class TaskMetaData:
    id: str
    path: str
    func_name:str
    args: list
    kwargs: dict
    status: str
    attempt: int = 0
    max_retries: int = 0
    backoff_base: float = 1.0
    backoff_factor: float = 2.0
    backoff_max: float = 60.0
    next_retry_at: Optional[float] = None
    

@dataclasses.dataclass
class QueueItem:
    func_ref: Any
    deserialize_data: TaskMetaData
    original_data: Any

def get_module_ref(path):
    module_path = pathlib.Path(path)

    logging.debug('module spec : %s',  str(module_path))
    spec = importlib.util.spec_from_file_location(
        module_path.stem, module_path)
    # Create the module object from the spec
    module = importlib.util.module_from_spec(spec)

    # Load the module
    spec.loader.exec_module(module)
    return module


@functools.lru_cache(maxsize=None)  # ponytail: unbounded; one entry per task function, bounded by codebase size
def get_function_ref(path, func_name):
    module = get_module_ref(path)
    return getattr(module, func_name)


def serialize(value: TaskMetaData):
    val_dict = dataclasses.asdict(value)
    return msgpack.packb(val_dict, use_bin_type=True)


def deserialize(byte_value) -> TaskMetaData:
    val_dict = msgpack.unpackb(byte_value, raw=False)
    return TaskMetaData(**val_dict)

# Per-queue keys carry a {q} hash tag so every key of one queue lands in the
# same Redis Cluster slot (required for BRPOPLPUSH and the Lua scripts).
def get_redis_q_key(q: str = ''):
    return 'async_task:{' + q + '}'

def get_redis_q_backup_key(q: str = '', wid: str = ''):
    return 'async_task_backup:{' + q + '}:' + wid

def get_task_key(extra: str = ''):
    return 'async-q-task:' + extra

def get_retry_zset_key(q: str = ''):
    return 'async_task_retry:{' + q + '}'

def get_dead_letter_key(q: str = ''):
    return 'async_task_dead:{' + q + '}'

def get_heartbeat_key(q: str, wid: str):
    return 'async_task_hb:{' + q + '}:' + wid

def get_workers_key(q: str):
    return 'async_task_workers:{' + q + '}'

def get_legacy_keys(q: str) -> Dict[str, str]:
    """Pre-0.3.0 key names. # ponytail: drain support for one release, then delete"""
    return {
        'main': 'async_task:' + q,
        'backup': 'async_task_backup:' + q,
        'retry': 'async_task_retry:' + q,
        'dead': 'async_task_dead:' + q,
    }

def new_worker_id() -> str:
    return f'{socket.gethostname()}-{os.getpid()}-{secrets.token_hex(3)}'

async def to_thread(func, /, *args, **kwargs):
    """Asynchronously run function *func* in a separate thread.
    Any *args and **kwargs supplied for this function are directly passed
    to *func*. Also, the current :class:`contextvars.Context` is propogated,
    allowing context variables from the main thread to be accessed in the
    separate thread.
    Return a coroutine that can be awaited to get the eventual result of *func*.
    """
    loop = asyncio.get_running_loop()
    ctx = contextvars.copy_context()
    func_call = functools.partial(ctx.run, func, *args, **kwargs)
    return await loop.run_in_executor(None, func_call)