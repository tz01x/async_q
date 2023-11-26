import json
from bs4 import BeautifulSoup
from web_crawler.utils import web_crawler
from web_crawler.models import AmazonProductItem
import logging
from async_q.async_q import AsyncTaskQueue
from async_q.utils import to_thread
from core.settings import BASE_DIR

logger = AsyncTaskQueue.get_instance().logger

def write_to_file(data):
    try:
        with open(BASE_DIR/'productItem.json','a') as f:
            f.write(data)
        logger.info('file written....')
    except Exception as e:
        logger.error('can not write to file because of: %s', str(e))

async def parse_amazon_search(html_text):
    logger.info('start parsing.......')
    soup = BeautifulSoup(html_text, 'html.parser')
    foundItems = []
    foundItemRaw = []

    # Extract information for each item
    items = soup.select('div[data-asin]:not(.AdHolder)')
    for item in items:
        asin = item['data-asin']
        price = item.select_one('.a-price[data-a-size=xl] .a-offscreen')
        if not price:
            continue

        price = price.text.strip()

        # Extract header
        header = item.select_one('h2')
        if not header:
            continue
        header = header.text.strip()

        # Extract price

        # Extract image URL
        image = item.select_one('.s-image')['src']

        link = item.select_one('h2 a[href]')['href']
        itemDict = {
            'asin': asin,
            'title': header,
            'price': price,
            'img_src': image,
            'url': link,
            'type':'gpu'
        }
        foundItemRaw.append(itemDict)
        foundItems.append(AmazonProductItem(**itemDict))
    items = await AmazonProductItem.objects.abulk_create(
        foundItems,
        update_conflicts=True,
        update_fields=[
            "title",
            "price",
            "img_src",
            "url",
            'type'
        ],
        unique_fields=['asin'],
    )


async def crawl_amazon_for_gpu(page=1, *args, **kwargs):
    logger.info('crawl_amazon_for_gpu with page %s .......',page)

    url = f'https://www.amazon.com/s?k=gpu&page={page}'
    headers = {
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
        'accept-language': 'en-GB,en;q=0.9',
    }
    logger.info('calling web_crawler with %s ', url)
    await web_crawler(url=url, strategy=parse_amazon_search, headers=headers)
