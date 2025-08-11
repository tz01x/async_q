import argparse
import asyncio
import logging
import signal

from .utils import get_module_ref
from .async_worker import async_worker
from .async_q import AsyncTaskQueue


async def close():
    logging.info('Finalizing...')
    await asyncio.sleep(1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Async task queue worker")
    parser.add_argument("-a", "--app", required=True,
                        help="application path ex: `async_q -a app.py:async_task_queue_app`")
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

    args = parser.parse_args()

    [filepath, app_attr] = str(args.app).split(':')

    module = get_module_ref(filepath)

    async_task_queue = getattr(module, app_attr, None)

    if not isinstance(async_task_queue, AsyncTaskQueue):

        raise Exception(f'{app_attr} should be instance of AsyncTaskQueue')

    async_task_queue.set_concurrency(args.concurrency)

    async_task_queue.config_logger(
        log_level=logging.getLevelName(args.log_level),
        log_filename=args.log_filename
    )
    async_task_queue.distribute_qname = args.queue

    try:
        async_task_queue.logger.info(f'Starting workers with {args.concurrency} concurrency')
        loop = asyncio.get_event_loop()

        stop_event = asyncio.Event()

        def _request_shutdown():
            if not stop_event.is_set():
                async_task_queue.logger.info('shutdown signal received; stopping...')
                stop_event.set()

        # Register signal handlers (Windows supports CTRL_C_EVENT → KeyboardInterrupt, but keep for POSIX)
        try:
            loop.add_signal_handler(signal.SIGINT, _request_shutdown)
            loop.add_signal_handler(signal.SIGTERM, _request_shutdown)
        except NotImplementedError:
            # Fallback for platforms without signal handler support in event loop
            pass

        loop.run_until_complete(
            async_worker(stop_event=stop_event)
        )
    except KeyboardInterrupt:
        async_task_queue.logger.info('program closing..')
    except Exception as e:
        async_task_queue.logger.error("__main__: %s", e)
    finally:
        loop.run_until_complete(close())
