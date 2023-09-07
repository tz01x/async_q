import importlib
import logging
import pathlib

import msgpack


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


def serialize(value):
    return msgpack.packb(value)


def deserialize(byte_value):
    return msgpack.unpackb(byte_value)

def get_redis_q_key(extra:str=''):
    return 'async_task:'+extra

def get_redis_q_backup_key(extra:str=''):
    return 'async_task_backup:'+extra

def get_task_key(extra:str=''):
    return 'async-q-task:'+extra