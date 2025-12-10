import os
from typing import Dict, Any

# System Configuration
SYSTEM_CONFIG = {
    "cpu_cores": os.cpu_count() or 12,
    "redis_host": "127.0.0.1",
    "redis_port": 6379,
    "redis_db_celery": 0,
    "redis_db_results": 1,
}

# Celery Configuration for CPU tasks
CELERY_CONFIG = {
    "broker_url": f"redis://{SYSTEM_CONFIG['redis_host']}:{SYSTEM_CONFIG['redis_port']}/{SYSTEM_CONFIG['redis_db_celery']}",
    "result_backend": f"redis://{SYSTEM_CONFIG['redis_host']}:{SYSTEM_CONFIG['redis_port']}/{SYSTEM_CONFIG['redis_db_results']}",


    # Worker configuration
    "worker_concurrency": SYSTEM_CONFIG["cpu_cores"],  # 1 per core for CPU tasks
    "worker_prefetch_multiplier": 1,
    "worker_max_tasks_per_child": 100,
    "worker_disable_rate_limits": True,
    # Task settings
    "task_serializer": "json",
    "accept_content": ["json"],
    "result_serializer": "json",
    "task_acks_late": True,
    "task_reject_on_worker_lost": True,
    "task_track_started": True,
    "task_time_limit": 300,  # 5 minutes
    "task_soft_time_limit": 280,
    "result_expires": 3600,  # Results expire after 1 hour
    "task_default_priority": 5,  # Default priority for tasks
    # Broker Settings
    "broker_connection_retry_on_startup": True,
    "broker_pool_limit": 100,
    "broker_connection_max_retries": None,
    # -----------------------

    # Priority setup for Redis
    "broker_transport_options": {
        'priority_steps': [0,5,9],
        'queue_order_strategy': 'priority',
    }
}

# FastAPI Configuration
FASTAPI_CONFIG = {
    "host": "0.0.0.0",
    "port": 8000,
    "workers": 4,  # For handling concurrent I/O requests
    "max_concurrent_io_tasks": 100,  # Max async I/O tasks
}

# Task-specific configurations
TASK_CONFIG = {
    "cpu_task_timeout": 60,  # seconds
    "io_task_timeout": 30,  # seconds
    "priority_task_timeout": 10,  # seconds (faster for priority)
    "max_retries": 3,
    "retry_delay": 5,
}