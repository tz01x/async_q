import inspect
import textwrap

import pytest

from async_q.utils import (
    TaskMetaData,
    serialize,
    deserialize,
    get_function_ref,
    get_module_ref,
)


def test_serialize_deserialize_roundtrip():
    meta = TaskMetaData(
        id="abc123",
        path="/tmp/module.py",
        func_name="do_work",
        args=[1, 2, 3],
        kwargs={"x": 10},
        status="submitted",
    )
    packed = serialize(meta)
    unpacked = deserialize(packed)

    assert isinstance(packed, (bytes, bytearray))
    assert unpacked.id == meta.id
    assert unpacked.path == meta.path
    assert unpacked.func_name == meta.func_name
    assert unpacked.args == meta.args
    assert unpacked.kwargs == meta.kwargs
    assert unpacked.status == meta.status


def test_get_module_and_function_ref(tmp_path):
    module_path = tmp_path / "mymod.py"
    module_path.write_text(
        textwrap.dedent(
            """
            import asyncio

            async def sample(arg1, *, kw=0):
                await asyncio.sleep(0)
                return arg1 + kw
            """
        )
    )

    module = get_module_ref(str(module_path))
    func = get_function_ref(str(module_path), "sample")

    assert inspect.ismodule(module)
    assert callable(func)
    assert inspect.iscoroutinefunction(func)


