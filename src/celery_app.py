import os
from celery import Celery, Task
from kombu import Exchange, Queue
from .config import CELERY_CONFIG, SYSTEM_CONFIG

print(f"🚀 Initializing Celery with {SYSTEM_CONFIG['cpu_cores']} CPU cores")

# Create Celery app
celery_app = Celery(
    'cpu_worker',
    broker=CELERY_CONFIG['broker_url'],
    backend=CELERY_CONFIG['result_backend'],
    include=['src.tasks']  # Include tasks module
)

# Configure queues with priorities
celery_app.conf.update(
    # Queue definitions
    task_queues=[
        Queue(
            'cpu_high',
            Exchange('cpu_high'),
            routing_key='cpu.high',
            queue_arguments={
                'x-max-priority': 10,  # Enable priority (0-10, 10 is highest)
                'x-queue-mode': 'lazy'  # Lazy mode for large queues
            }
        ),
        Queue(
            'cpu_normal',
            Exchange('cpu_normal'),
            routing_key='cpu.normal',
            queue_arguments={
                'x-max-priority': 5,
                'x-queue-mode': 'lazy'
            }
        ),
    ],

    # Task routing
    task_routes={
        'tasks.cpu_heavy_task1': {'queue': 'cpu_normal'},
        'tasks.cpu_heavy_task2': {'queue': 'cpu_normal'},
        'tasks.cpu_priority_task': {'queue': 'cpu_high'},
    },

    # Worker settings
    worker_concurrency=CELERY_CONFIG['worker_concurrency'],
    worker_prefetch_multiplier=CELERY_CONFIG['worker_prefetch_multiplier'],
    worker_max_tasks_per_child=CELERY_CONFIG['worker_max_tasks_per_child'],
    worker_disable_rate_limits=True,

    # Task settings
    task_serializer=CELERY_CONFIG['task_serializer'],
    accept_content=CELERY_CONFIG['accept_content'],
    result_serializer=CELERY_CONFIG['result_serializer'],
    task_acks_late=CELERY_CONFIG['task_acks_late'],
    task_reject_on_worker_lost=CELERY_CONFIG['task_reject_on_worker_lost'],
    task_track_started=CELERY_CONFIG['task_track_started'],
    task_time_limit=CELERY_CONFIG['task_time_limit'],
    task_soft_time_limit=CELERY_CONFIG['task_soft_time_limit'],
    result_expires=3600,  # Results expire after 1 hour

    # Priority settings
    task_default_priority=5,

    # Broker settings
    broker_connection_retry_on_startup=True,
    broker_pool_limit=100,
    broker_connection_max_retries=None,
)


# Custom task base class for priority tasks
class PriorityTask(Task):
    """Base class for priority tasks"""
    priority = 5  # Default priority

    def apply_async(self, args=None, kwargs=None, **options):
        # Set priority if not specified
        if 'priority' not in options:
            options['priority'] = self.priority
        return super().apply_async(args=args, kwargs=kwargs, **options)


# Export celery app instance
celery = celery_app