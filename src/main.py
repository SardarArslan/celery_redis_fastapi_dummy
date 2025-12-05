from fastapi import FastAPI

from celery.result import AsyncResult
from .celery_worker import write_log_celery,celery

app = FastAPI()

@app.post("/notify/")
async def notify(email: str):
    task = write_log_celery.delay(f"Notification sent to {email}")
    return {"message":f"email will sent to {email}","task_id":task.id}

@app.get("/task_status/{task_id}")
async def get_task_status(task_id: str):
    task_result = AsyncResult(task_id)
    if task_result.ready():
        return {"task_id":task_id,"status":"completed","result":task_result.result}
    if task_result.failed():
        return {"task_id":task_id, "status":"failed"}
    else:
        return {"task_id":task_id, "status":"in progress"}
