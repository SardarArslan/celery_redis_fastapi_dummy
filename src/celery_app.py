import os
from celery import Celery, Task
from kombu import Exchange, Queue
from .config import CELERY_CONFIG, SYSTEM_CONFIG

print(f"Initializing Celery with {SYSTEM_CONFIG['cpu_cores']} CPU cores")


celery_app = Celery(
    'cpu_worker',
    broker=CELERY_CONFIG['broker_url'],
    backend=CELERY_CONFIG['result_backend'],
    include=['src.tasks']
)
celery_app.config_from_object(CELERY_CONFIG)


celery_app.conf.update(
    task_queues=[
        Queue('cpu_high'),
        Queue('cpu_normal'),
    ],
    # Task routing
    task_routes={
        'tasks.cpu_heavy_task1': {'queue': 'cpu_normal'},
        'tasks.cpu_heavy_task2': {'queue': 'cpu_normal'},
        'tasks.cpu_priority_task': {'queue': 'cpu_high'},
    },

)

# Custom task base class for priority tasks
class PriorityTask(Task):
    """Base class for priority tasks"""
    priority = 5

    def apply_async(self, args=None, kwargs=None, **options):
        # Set priority if not specified
        if 'priority' not in options:
            options['priority'] = self.priority
        return super().apply_async(args=args, kwargs=kwargs, **options)


celery = celery_app