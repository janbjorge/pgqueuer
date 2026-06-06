from django.contrib import admin
from django.urls import path

from myapp import views

urlpatterns = [
    path("admin/", admin.site.urls),
    path("", views.index, name="index"),
    path("enqueue/", views.enqueue_email, name="enqueue_email"),
    path("status/", views.job_status, name="job_status"),
    path("api/stats/", views.queue_stats, name="queue_stats"),
]
