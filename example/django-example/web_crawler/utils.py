import logging
import traceback
from aiohttp import ClientSession
import inspect

async def web_crawler(url,strategy, headers = None):
    try:
        async with ClientSession() as session:
            response = await session.get(url,headers=headers)
            if response.ok:
                text = await response.text()
                if inspect.iscoroutinefunction(strategy):
                    await strategy(text)
                else:
                    strategy(text)
    except Exception as e:
        logging.error(e)
        logging.error('traceback-> %s',traceback.format_exc())
