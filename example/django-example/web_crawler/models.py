from django.db import models

# Create your models here.
class AmazonProductItem(models.Model):
    asin = models.CharField(max_length=50,unique=True,db_index=True)
    title = models.TextField()
    price = models.CharField(max_length=50)
    img_src = models.CharField(max_length=1000)
    url = models.URLField()
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    type = models.CharField(max_length=100)
    def __str__(self) -> str:
        return self.asin + self.title