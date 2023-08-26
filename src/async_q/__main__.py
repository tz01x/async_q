import argparse
import asyncio
from .async_worker import async_worker, get_module, AsyncTaskQueue
import logging


logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s (%(filename)s:%(lineno)d)",
)


async def close():
    print('Finalazing...')
    await asyncio.sleep(1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Async task queue worker")
    parser.add_argument("-a", "--app", required=True,
                        help="application path ex: `async_task_queue -a app.py:async_task_queue_app`")

    args = parser.parse_args()

    [filepath, app_attr] = str(args.app).split(':')

    module = get_module(filepath)

    if not isinstance(getattr(module, app_attr,None), AsyncTaskQueue):

        raise Exception(f'{app_attr} should be instance of AsyncTaskQueue')

    try:
        # print(filepath,app_attr)
        loop = asyncio.get_event_loop()
        loop.set_debug(True)
        loop.run_until_complete(
            async_worker()
        )
    except KeyboardInterrupt:
        logging.info('program closing..')
    except Exception as e:
        logging.error("__main__: %s", e)
    finally:
        loop.run_until_complete(close())
