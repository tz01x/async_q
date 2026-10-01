import argparse
import asyncio
import logging
import signal
import os
import importlib
import pathlib
from typing import List, Optional

from .utils import get_module_ref
from .async_worker import async_worker
from .async_q import AsyncTaskQueue


async def close():
    logging.info('Finalizing...')
    await asyncio.sleep(1)


def resolve_app(app_spec: str) -> AsyncTaskQueue:
    """Resolve an app spec (file path or dotted module path) to an AsyncTaskQueue instance."""
    try:
        # Use rsplit to handle Windows drive letters like C:\path\file.py:attr
        module_part, app_attr = str(app_spec).rsplit(':', 1)
    except ValueError:
        raise SystemExit("--app must be in the form module_or_file:attribute")

    # Decide how to import: by file path or dotted module path
    module: object
    path = pathlib.Path(module_part)
    if path.exists() and path.suffix == '.py':
        module = get_module_ref(str(path))
    else:
        module = importlib.import_module(module_part)

    async_task_queue = getattr(module, app_attr, None)
    if not isinstance(async_task_queue, AsyncTaskQueue):
        raise Exception(f'{app_attr} should be instance of AsyncTaskQueue')
    return async_task_queue


def _positive_seconds(text: str) -> float:
    try:
        value = float(text)
    except ValueError:
        raise argparse.ArgumentTypeError(f'{text!r} is not a number')
    if not (value > 0):  # also rejects nan
        raise argparse.ArgumentTypeError(f'must be > 0, got {text!r}')
    return value


def main(argv: Optional[List[str]] = None) -> None:
    parser = argparse.ArgumentParser(description="Async task queue worker")
    parser.add_argument(
        "-a",
        "--app",
        required=False,
        help=(
            "Application spec as file path or dotted module path, e.g.:\n"
            "  - file:  -a app.py:async_q_app\n"
            "  - module: -a mypkg.app:async_q_app\n"
            "Can also be provided via ASYNC_Q_APP environment variable."
        ),
    )
    parser.add_argument('-c', '--concurrency', default=100, type=int,
                        required=False,
                        help='limit the number of concurrent execution, default is 100.')
    parser.add_argument('-q','--queue',default='default', type=str)
    parser.add_argument('-ll', '--log_level', default='INFO', type=str,
                        required=False,
                        choices=list(logging._nameToLevel.keys()),
                        help='log level `-ll INFO`, default loglevel is INFO')

    parser.add_argument('-lf', '--log_filename', default=None, type=str,
                        required=False,
                        help='set log file path `-lf ./logs/async-q.log`')
    parser.add_argument('--shutdown-timeout', default=None, type=_positive_seconds,
                        help='seconds to let running tasks finish on shutdown before re-queueing them (default 30)')

    args = parser.parse_args(argv)

    app_spec = args.app or os.getenv('ASYNC_Q_APP')
    if not app_spec:
        raise SystemExit("--app is required (or set ASYNC_Q_APP)")

    async_task_queue = resolve_app(app_spec)

    async_task_queue.set_concurrency(args.concurrency)

    async_task_queue.config_logger(
        log_level=logging.getLevelName(args.log_level),
        log_filename=args.log_filename
    )
    async_task_queue.distribute_qname = args.queue

    if args.shutdown_timeout is not None:
        async_task_queue.shutdown_timeout = args.shutdown_timeout

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        async_task_queue.logger.info(f'Starting workers with {args.concurrency} concurrency')

        stop_event = asyncio.Event()

        def _request_shutdown():
            if not stop_event.is_set():
                async_task_queue.logger.info('shutdown signal received; stopping...')
                stop_event.set()

        try:
            loop.add_signal_handler(signal.SIGINT, _request_shutdown)
            loop.add_signal_handler(signal.SIGTERM, _request_shutdown)
        except NotImplementedError:
            # Windows: no loop signal handlers. Route Ctrl+C / Ctrl+Break to the same graceful path.
            for sig in (signal.SIGINT, getattr(signal, 'SIGBREAK', None)):
                if sig is not None:
                    signal.signal(sig, lambda *_: loop.call_soon_threadsafe(_request_shutdown))

        loop.run_until_complete(
            async_worker(stop_event=stop_event)
        )
    except KeyboardInterrupt:
        async_task_queue.logger.info('program closing..')
    except Exception as e:
        async_task_queue.logger.error("__main__: %s", e)
    finally:
        loop.run_until_complete(close())
        loop.close()


if __name__ == "__main__":
    main()
