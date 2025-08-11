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


