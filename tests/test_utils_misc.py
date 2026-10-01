import asyncio

import pytest

from async_q.utils import (
    get_redis_q_key,
    get_redis_q_backup_key,
    get_task_key,
    to_thread,
)


def test_key_builders():
    assert get_redis_q_key("default") == "async_task:{default}"
    assert get_redis_q_backup_key("default", "w") == "async_task_backup:{default}:w"
    assert get_task_key("123") == "async-q-task:123"


@pytest.mark.asyncio
async def test_to_thread_executes_in_executor():
    def blocking_add(a, b):
        import time
        time.sleep(0.01)
        return a + b

    result = await to_thread(blocking_add, 2, 3)
    assert result == 5




def test_get_function_ref_loads_module_once(tmp_path):
    from async_q.utils import get_function_ref
    get_function_ref.cache_clear()
    mod = tmp_path / "counted.py"
    mod.write_text(
        "import builtins\n"
        "builtins._async_q_loads = getattr(builtins, '_async_q_loads', 0) + 1\n"
        "async def job():\n    pass\n"
    )
    import builtins
    builtins._async_q_loads = 0
    f1 = get_function_ref(str(mod), "job")
    f2 = get_function_ref(str(mod), "job")
    assert f1 is f2
    assert builtins._async_q_loads == 1


def test_get_function_ref_does_not_cache_failures(tmp_path):
    from async_q.utils import get_function_ref
    get_function_ref.cache_clear()
    mod = tmp_path / "late.py"
    with pytest.raises(FileNotFoundError):
        get_function_ref(str(mod), "job")
    mod.write_text("async def job():\n    pass\n")
    assert get_function_ref(str(mod), "job").__name__ == "job"
