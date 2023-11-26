from django.urls import path
from .views import get_current_submitted_task_element, index_view, list_amazon_product_item, submit_amazon_web_crawl_task_view


urlpatterns = [
    path('', index_view, name='index-view'),
    path('gpu-list',list_amazon_product_item),
    path('get-running-tasks', get_current_submitted_task_element),
    
    path('api/submit-task', submit_amazon_web_crawl_task_view),
    
]
