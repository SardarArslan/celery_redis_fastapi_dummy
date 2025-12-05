import time
from celery import Celery

celery = Celery(
    "worker",
    broker="redis://127.0.0.1:6379/0",
    backend="redis://127.0.0.1:6379/1"
)

@celery.task
def write_log_celery(message: str):
    time.sleep(5)
    with open("log_celery.txt", "a") as f:
        f.write(f"{message}\n")
    return f"Task completed: {message}"