from django.contrib import admin

# Register your models here.

from .models import (
  ActiveGoogleSearch,
  ActiveInstagramSearch,
  GoogleUrl,
  InstagramUrl
)

admin.site.register(ActiveGoogleSearch)
admin.site.register(ActiveInstagramSearch)
admin.site.register(GoogleUrl)
admin.site.register(InstagramUrl)
