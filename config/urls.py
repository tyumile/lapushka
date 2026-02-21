from django.contrib import admin
from django.urls import include, path
from django.views.generic import RedirectView

urlpatterns = [
    path("", RedirectView.as_view(url="/start/", permanent=False)),
    path("admin/", admin.site.urls),
    path("", include("core_v02.urls")),
]
