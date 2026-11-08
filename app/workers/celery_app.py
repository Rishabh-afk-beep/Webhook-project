"""Celery application configuration."""
from celery import Celery

from app.core.config import get_settings

settings = get_settings()

celery_app = Celery(
    "webhook_workers",
    broker=settings.celery_broker_url,
    include=["app.workers.tasks"],
)

celery_app.conf.update(
    # Acknowledge tasks after they complete, not when received.
    # This means if a worker crashes mid-task, the task returns to the queue.
    task_acks_late=True,
    # Don't prefetch more than 1 task at a time per worker.
    worker_prefetch_multiplier=1,
    # Serialize using JSON
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    # UTC timestamps
    timezone="UTC",
    enable_utc=True,
    # For testing: run tasks synchronously when CELERY_TASK_ALWAYS_EAGER=true
    task_always_eager=settings.celery_task_always_eager,
    # Reject tasks on worker shutdown so they return to the queue
    worker_cancel_long_running_tasks_on_connection_loss=True,
)
