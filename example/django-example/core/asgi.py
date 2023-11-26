"""
ASGI config for djangoexample project.

It exposes the ASGI callable as a module-level variable named ``application``.

For more information on this file, see
https://docs.djangoproject.com/en/4.2/howto/deployment/asgi/
"""

import os
from async_q import AsyncTaskQueue, RedisBuilder
from django.core.asgi import get_asgi_application

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'core.settings')

application = get_asgi_application()


# Define Async Task Queue App
async_q_app = AsyncTaskQueue(
    redis_builder=RedisBuilder(
        host='redis',
        port='6379',
    )
)