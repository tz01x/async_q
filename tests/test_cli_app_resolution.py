import textwrap

import pytest

from async_q.__main__ import resolve_app
from async_q.async_q import AsyncTaskQueue, RedisBuilder


@pytest.fixture
def tmp_module_pkg(tmp_path, monkeypatch):
    # Create a temporary package with an app inside
    pkg = tmp_path / "mypkg"
    pkg.mkdir()
    (pkg / "__init__.py").write_text("")
    mod = pkg / "app.py"
    mod.write_text(
        textwrap.dedent(
            """
            from async_q import AsyncTaskQueue, RedisBuilder
            async_q_app = AsyncTaskQueue(redis_builder=RedisBuilder())
            """
        )
    )

    # Put temp package on sys.path
    monkeypatch.syspath_prepend(str(tmp_path))
    return mod


def test_resolve_app_from_dotted_module(tmp_module_pkg):
    app = resolve_app("mypkg.app:async_q_app")
    assert isinstance(app, AsyncTaskQueue)


def test_resolve_app_from_file_path(tmp_module_pkg):
    app = resolve_app(f"{tmp_module_pkg}:async_q_app")
    assert isinstance(app, AsyncTaskQueue)




import asyncio


def test_main_requires_app(monkeypatch):
    from async_q import __main__ as cli
    monkeypatch.delenv("ASYNC_Q_APP", raising=False)
    with pytest.raises(SystemExit):
        cli.main([])


def test_main_runs_worker_on_fresh_loop(tmp_path, monkeypatch, reset_async_task_queue):
    from async_q import __main__ as cli
    app_file = tmp_path / "app_main.py"
    app_file.write_text(
        "from async_q import AsyncTaskQueue, RedisBuilder\n"
        "app = AsyncTaskQueue(redis_builder=RedisBuilder())\n"
    )
    seen = {}

    async def fake_worker(stop_event=None):
        seen["stop_event"] = stop_event
        seen["loop"] = asyncio.get_running_loop()

    async def fake_close():
        pass

    monkeypatch.setattr(cli, "async_worker", fake_worker)
    monkeypatch.setattr(cli, "close", fake_close)
    cli.main(["-a", f"{app_file}:app", "-q", "mail", "-c", "3"])

    assert isinstance(seen["stop_event"], asyncio.Event)
    assert seen["loop"].is_closed()
    assert cli.AsyncTaskQueue.get_instance().distribute_qname == "mail"


def _app_file(tmp_path):
    app_file = tmp_path / "app_cli2.py"
    app_file.write_text(
        "from async_q import AsyncTaskQueue, RedisBuilder\n"
        "app = AsyncTaskQueue(redis_builder=RedisBuilder())\n"
    )
    return app_file


def test_main_shutdown_timeout_flag(tmp_path, monkeypatch, reset_async_task_queue):
    from async_q import __main__ as cli

    async def fake_worker(stop_event=None):
        pass

    async def fake_close():
        pass

    monkeypatch.setattr(cli, "async_worker", fake_worker)
    monkeypatch.setattr(cli, "close", fake_close)
    cli.main(["-a", f"{_app_file(tmp_path)}:app", "--shutdown-timeout", "600"])
    assert cli.AsyncTaskQueue.get_instance().shutdown_timeout == 600


@pytest.mark.parametrize("value", ["0", "-5", "nan", "abc"])
def test_main_rejects_non_positive_shutdown_timeout(tmp_path, monkeypatch, reset_async_task_queue, value):
    from async_q import __main__ as cli

    async def fake_worker(stop_event=None):
        pass

    async def fake_close():
        pass

    monkeypatch.setattr(cli, "async_worker", fake_worker)
    monkeypatch.setattr(cli, "close", fake_close)
    with pytest.raises(SystemExit) as exc:
        cli.main(["-a", f"{_app_file(tmp_path)}:app", "--shutdown-timeout", value])
    assert exc.value.code == 2  # argparse usage error


def test_main_falls_back_to_signal_signal_when_loop_lacks_handlers(tmp_path, monkeypatch, reset_async_task_queue):
    from async_q import __main__ as cli
    installed = {}
    real_new_loop = asyncio.new_event_loop

    def loop_without_signal_support():
        loop = real_new_loop()

        def nope(*a, **k):
            raise NotImplementedError

        loop.add_signal_handler = nope
        return loop

    def fake_signal(sig, handler):
        installed[sig] = handler

    seen = {}

    async def fake_worker(stop_event=None):
        installed[cli.signal.SIGINT](cli.signal.SIGINT, None)   # simulate Ctrl+C
        await asyncio.wait_for(stop_event.wait(), 1)
        seen["stopped"] = True

    async def fake_close():
        pass

    monkeypatch.setattr(cli.asyncio, "new_event_loop", loop_without_signal_support)
    monkeypatch.setattr(cli.signal, "signal", fake_signal)
    monkeypatch.setattr(cli, "async_worker", fake_worker)
    monkeypatch.setattr(cli, "close", fake_close)
    cli.main(["-a", f"{_app_file(tmp_path)}:app"])
    assert seen == {"stopped": True}
