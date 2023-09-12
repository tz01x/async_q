import asyncio
import contextvars
import functools
import importlib
import logging
import pathlib
from typing import Any

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


def get_function_ref(path, func_name):
    module = get_module_ref(path)
    return getattr(module, func_name)


def serialize(value:TaskMetaData):
    val_dict = dataclasses.asdict(value)
    return msgpack.packb(val_dict)


def deserialize(byte_value) -> TaskMetaData:
    val_dict =  msgpack.unpackb(byte_value)
    return TaskMetaData(**val_dict)

def get_redis_q_key(extra:str=''):
    return 'async_task:'+extra

def get_redis_q_backup_key(extra:str=''):
    return 'async_task_backup:'+extra

def get_task_key(extra:str=''):
    return 'async-q-task:'+extra

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