import os
import sys
import asyncio

import pytest
import pytest_asyncio

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


@pytest.fixture
def fake_ack_scripts(monkeypatch):
    """Swap the ack scripts for FakeAsyncRedis-compatible stand-ins."""
    from async_q import scripts
    from fakes import fake_fail_ack, fake_requeue
    monkeypatch.setattr(scripts, "requeue", fake_requeue)
    monkeypatch.setattr(scripts, "fail_ack", fake_fail_ack)


@pytest.fixture(scope="session")
def event_loop():
    # Create a fresh event loop for the test session
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()


@pytest_asyncio.fixture
async def real_redis():
    url = os.getenv("ASYNC_Q_TEST_REDIS_URL")
    if not url:
        pytest.skip("set ASYNC_Q_TEST_REDIS_URL to run Redis integration tests")
    import redis.asyncio as aioredis
    r = aioredis.Redis.from_url(url)
    await r.flushdb()
    yield r
    await r.flushdb()
    await (getattr(r, "aclose", None) or r.close)()
