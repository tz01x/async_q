import argparse
import asyncio

from .utils import get_module_ref
from .async_worker import async_worker, AsyncTaskQueue
import logging


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s (%(filename)s:%(lineno)d)",
    filename='async-q-logs.log'
)


async def close():
    logging.info('Finalazing...')
    await asyncio.sleep(1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Async task queue worker")
    parser.add_argument("-a", "--app", required=True,
                        help="application path ex: `async_q -a app.py:async_task_queue_app`")

    args = parser.parse_args()

    [filepath, app_attr] = str(args.app).split(':')

    module = get_module_ref(filepath)

    if not isinstance(getattr(module, app_attr,None), AsyncTaskQueue):

        raise Exception(f'{app_attr} should be instance of AsyncTaskQueue')

    try:
        loop = asyncio.get_event_loop()
        loop.run_until_complete(
            async_worker()
        )
    except KeyboardInterrupt:
        logging.info('program closing..')
    except Exception as e:
        logging.error("__main__: %s", e)
    finally:
        loop.run_until_complete(close())
