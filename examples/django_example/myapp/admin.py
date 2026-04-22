from django.contrib import admin

from .models import EmailJob


@admin.register(EmailJob)
class EmailJobAdmin(admin.ModelAdmin):
    list_display = ("recipient", "subject", "status", "pgqueuer_job_id", "created_at")
    list_filter = ("status", "created_at")
    search_fields = ("recipient", "subject")
    readonly_fields = ("pgqueuer_job_id", "created_at", "updated_at")
