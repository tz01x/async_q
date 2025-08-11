from django.shortcuts import render
from async_q.utils import get_task_key
from web_crawler.service import crawl_amazon_for_gpu
from async_q import submit_task
from django.http import JsonResponse
from django.db.models import Case, When, BooleanField
from django.utils import timezone
from .models import AmazonProductItem
from django.core.paginator import Paginator, EmptyPage, PageNotAnInteger
from async_q import AsyncTaskQueue



PAGE_LIMIT = 10



def index_view(request):
    context = {
        'page': request.GET.get('page',1),
    }
    return render(request, template_name='web_crawler/index.html', context=context)


def list_amazon_product_item(request):
    page = int(request.GET.get('page', '1'))

    queryset = AmazonProductItem.objects.filter(type='gpu').annotate(
        is_new=Case(
            When(updated_at__gte=timezone.now() -
                 timezone.timedelta(hours=1), then=True),
            default=False,
            output_field=BooleanField()
        )

    ).order_by('-updated_at')
    #           offset  PAGE_LIMIT X (PAGE_NUM -1 ),  limit PAGE_LIMIT x PAGE_NUM
    # page 1 -> offset 15*0 , limit 15
    # page 2 -> offset 15 limit 30
    paginator = Paginator(queryset, PAGE_LIMIT)
    try:
        qs = paginator.page(page)
    except PageNotAnInteger:
        qs = paginator.page(1)
    except EmptyPage:
        qs = paginator.page(paginator.num_pages)

    pages = [None if i==Paginator.ELLIPSIS else i for i in paginator.get_elided_page_range()]
 
    context = {
        'product_items': qs,
        'pages':pages,
        
    }
    return render(request, template_name='web_crawler/product-item.html', context=context)

# apis


async def submit_amazon_web_crawl_task_view(request):

    page = int(request.GET.get('page', 1))
    
    # submitting background task to crawl amazon to get item related to gpu

    submit_task(crawl_amazon_for_gpu, args=[page])
    submit_task(crawl_amazon_for_gpu, args=[page+1])
    submit_task(crawl_amazon_for_gpu, args=[page+2])
    submit_task(crawl_amazon_for_gpu, args=[page+3])

    return JsonResponse(data={'detail': 'task submitted'})

from async_q.utils import deserialize

# task statuses are
#         pending
#         submitted
#         starting
#         finished
        

async def get_current_submitted_task_element(request):

    redis = AsyncTaskQueue.get_instance().redis_builder.get_redis_async()
    items = []

    # Use SCAN to avoid blocking Redis on large keyspaces
    async for key in redis.scan_iter(match=get_task_key('*')):
        byte = await redis.get(key)
        if not byte:
            continue
        item = deserialize(byte_value=byte)
        if item.status != 'finished':
            items.append(item)
    
    context = {
        'queued_items':items,
    }
    return render(request, 'web_crawler/queued_items_list.html', context=context)