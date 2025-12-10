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

    # Queue configuration with priorities
    "task_queues": {
        "cpu_normal": {"exchange": "cpu", "routing_key": "cpu.normal"},
        "cpu_high": {"exchange": "cpu", "routing_key": "cpu.high"},
    },

    # Task routes
    "task_routes": {
        "tasks.cpu_heavy_task1": {"queue": "cpu_normal"},
        "tasks.cpu_heavy_task2": {"queue": "cpu_normal"},
        "tasks.cpu_priority_task": {"queue": "cpu_high"},
    },

    # Worker configuration
    "worker_concurrency": SYSTEM_CONFIG["cpu_cores"],  # 1 per core for CPU tasks
    "worker_prefetch_multiplier": 1,
    "worker_max_tasks_per_child": 100,

    # Task settings
    "task_serializer": "json",
    "accept_content": ["json"],
    "result_serializer": "json",
    "task_acks_late": True,
    "task_reject_on_worker_lost": True,
    "task_track_started": True,
    "task_time_limit": 300,  # 5 minutes
    "task_soft_time_limit": 280,
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