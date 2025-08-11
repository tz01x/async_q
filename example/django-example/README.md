## Django X async_q
steps to setup async_q with django 
#### 1# async_q application

```python
# core/async.q
from async_q import AsyncTaskQueue, RedisBuilder
...
# Define Async Task Queue App
async_q_app = AsyncTaskQueue(
    redis_builder=RedisBuilder(
        host='redis',
        port='6379',
    )
)
```

#### 2# submit async task

```python

# django-example\web_crawler\views.py
from async_q import submit_task
from web_crawler.service import crawl_amazon_for_gpu

...

async def submit_amazon_web_crawl_task_view(request):

    page = int(request.GET.get('page', 1))
    
    # submitting background task to crawl amazon to get item related to gpu

    submit_task(crawl_amazon_for_gpu, args=[page])
    submit_task(crawl_amazon_for_gpu, args=[page+1])
    submit_task(crawl_amazon_for_gpu, args=[page+2])
    submit_task(crawl_amazon_for_gpu, args=[page+3])

```

#### 3# run async_q workers
```bash
python -m async_q -a core/asgi.py:async_q_app -c 5 -ll DEBUG
```


## Run this example project
Make sure you have Docker installed on your system before running this command. This command is using Docker Compose to start the project defined in the `docker-compose.yml` file in detached mode (`-d`).
```bash
docker compose -f docker-compose.yml up -d
```

