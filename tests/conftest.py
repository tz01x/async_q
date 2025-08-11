import os
import sys
import asyncio

import pytest

# Ensure `src` is on sys.path before tests import modules
_project_root = os.path.dirname(os.path.dirname(__file__))
_src_path = os.path.join(_project_root, "src")
if _src_path not in sys.path:
    sys.path.insert(0, _src_path)


@pytest.fixture
def reset_async_task_queue():
    # Ensure AsyncTaskQueue singleton is reset between tests
    import async_q.async_q as aq
    aq.AsyncTaskQueue._instance = None
    yield
    aq.AsyncTaskQueue._instance = None


@pytest.fixture(scope="session")
def event_loop():
    # Create a fresh event loop for the test session
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()


