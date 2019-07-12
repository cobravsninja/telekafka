from django.db import models
from uuid import uuid4
from django.utils import timezone

# Create your models here.

class ActiveGoogleSearch(models.Model):
  id          = models.UUIDField(primary_key=True,default=uuid4,editable=False)
  query       = models.CharField(max_length=100)
  status      = models.BooleanField(default=True)
  date_added  = models.DateTimeField(default=timezone.now())
  chat_id     = models.CharField(max_length=100)

  def __str__(self):
    return '{}-{}'.format(self.pk,self.query)

class ActiveInstagramSearch(ActiveGoogleSearch):
  pass

class Url(models.Model):
  url           = models.CharField(max_length=1000)
  status        = models.BooleanField(default=True)
  requested     = models.DateTimeField(blank=True,null=True)

  class Meta:
    abstract = True

  def __str__(self):
    return '{} - {}'.format(self.status,self.requested)

class GoogleUrl(Url):
  active        = models.ForeignKey(ActiveGoogleSearch,on_delete=models.CASCADE)

class InstagramUrl(Url):
  active        = models.ForeignKey(ActiveInstagramSearch,on_delete=models.CASCADE)
