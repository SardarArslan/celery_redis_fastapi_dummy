from fastapi import FastAPI, HTTPException, BackgroundTasks, Query, Body
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
import asyncio
from typing import List, Dict, Any, Optional
import uuid
import time
import redis
from datetime import datetime
from celery.result import AsyncResult

# Import our modules
from .config import SYSTEM_CONFIG, FASTAPI_CONFIG, TASK_CONFIG
from .tasks import (
    cpu_heavy_task1,
    cpu_heavy_task2,
    cpu_priority_task,
    async_io_task1,
    async_io_task2
)
from .celery_app import celery

# Initialize FastAPI app
app = FastAPI(
    title="Hybrid Task Processing System",
    description="Combines FastAPI Async I/O with Celery CPU processing",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc"
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Initialize Redis client for monitoring
redis_client = redis.Redis(
    host=SYSTEM_CONFIG['redis_host'],
    port=SYSTEM_CONFIG['redis_port'],
    db=SYSTEM_CONFIG['redis_db_celery'],
    decode_responses=True
)

# Store async task statuses (in-memory for demo, use Redis in production)
async_task_status = {}


# ==================== HEALTH & MONITORING ENDPOINTS ====================

@app.get("/")
async def root():
    """Root endpoint with system info"""
    return {
        "service": "Hybrid Task Processor",
        "version": "1.0.0",
        "architecture": "FastAPI Async I/O + Celery CPU",
        "system_cores": SYSTEM_CONFIG['cpu_cores'],
        "endpoints": {
            "cpu_tasks": "/cpu/*",
            "io_tasks": "/io/*",
            "monitoring": "/system/*",
            "task_status": "/task/{task_id}"
        }
    }


@app.get("/health")
async def health_check():
    """Health check endpoint"""
    # Check Redis
    redis_ok = redis_client.ping()

    # Check Celery (try to ping worker)
    celery_ok = False
    try:
        inspector = celery.control.inspect()
        stats = inspector.stats()
        celery_ok = bool(stats)
    except:
        celery_ok = False

    return {
        "status": "healthy" if redis_ok and celery_ok else "degraded",
        "timestamp": datetime.now().isoformat(),
        "components": {
            "redis": "ok" if redis_ok else "unavailable",
            "celery": "ok" if celery_ok else "unavailable",
            "fastapi": "ok"
        }
    }


@app.get("/system/stats")
async def system_stats():
    """Get system statistics and queue status"""
    try:
        # Redis queue lengths
        queue_lengths = {
            "cpu_normal": redis_client.llen("cpu_normal"),
            "cpu_high": redis_client.llen("cpu_high"),
            "celery": redis_client.llen("celery")  # Default queue
        }

        # Celery worker stats
        inspector = celery.control.inspect()
        active_tasks = inspector.active() or {}
        reserved_tasks = inspector.reserved() or {}
        scheduled_tasks = inspector.scheduled() or {}

        # Count workers and tasks
        total_workers = len(active_tasks)
        total_active = sum(len(tasks) for tasks in active_tasks.values())
        total_reserved = sum(len(tasks) for tasks in reserved_tasks.values())
        total_scheduled = sum(len(tasks) for tasks in scheduled_tasks.values())

        # Async task stats
        async_stats = {
            "pending": len([t for t in async_task_status.values() if t['status'] == 'pending']),
            "running": len([t for t in async_task_status.values() if t['status'] == 'running']),
            "completed": len([t for t in async_task_status.values() if t['status'] == 'completed']),
            "failed": len([t for t in async_task_status.values() if t['status'] == 'failed']),
        }

        return {
            "timestamp": datetime.now().isoformat(),
            "queues": queue_lengths,
            "celery": {
                "workers": total_workers,
                "active_tasks": total_active,
                "reserved_tasks": total_reserved,
                "scheduled_tasks": total_scheduled
            },
            "async_tasks": async_stats,
            "system": {
                "cpu_cores": SYSTEM_CONFIG['cpu_cores'],
                "max_concurrent_io": FASTAPI_CONFIG['max_concurrent_io_tasks']
            }
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ==================== CPU TASK ENDPOINTS (Celery) ====================

@app.post("/cpu/analyze-data")
async def analyze_data(data: Dict[str, Any] = Body(...)):
    """
    Endpoint for CPU-intensive data analysis
    Routes to celery worker (cpu_normal queue)
    """
    task_id = str(uuid.uuid4())

    try:
        # Submit to Celery
        task = cpu_heavy_task1.apply_async(
            args=[data],
            task_id=task_id,
            priority=5  # Normal priority
        )

        return {
            "message": "CPU analysis task submitted",
            "task_id": task_id,
            "queue": "cpu_normal",
            "status_endpoint": f"/task/{task_id}"
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/cpu/process-text")
async def process_text(
        text: str = Body(..., embed=True),
        iterations: int = Body(1000, embed=True)
):
    """
    Endpoint for CPU-intensive text processing
    Routes to celery worker (cpu_normal queue)
    """
    task_id = str(uuid.uuid4())

    try:
        # Submit to Celery
        task = cpu_heavy_task2.apply_async(
            args=[text, iterations],
            task_id=task_id,
            priority=0  # Normal priority
        )

        return {
            "message": "Text processing task submitted",
            "task_id": task_id,
            "queue": "cpu_normal",
            "status_endpoint": f"/task/{task_id}"
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/cpu/priority-process")
async def priority_process(data: Dict[str, Any] = Body(...)):
    """
    Endpoint for high-priority CPU tasks
    Routes to celery worker (cpu_high queue) with high priority
    """
    task_id = str(uuid.uuid4())

    try:
        # Ensure required fields for priority
        if 'priority' not in data:
            data['priority'] = 'high'

        # Submit to Celery with high priority
        task = cpu_priority_task.apply_async(
            args=[data],
            task_id=task_id,
            priority=0,  # Highest priority
            queue='cpu_high'
        )

        return {
            "message": "High-priority CPU task submitted",
            "task_id": task_id,
            "queue": "cpu_high",
            "priority": 9,
            "status_endpoint": f"/task/{task_id}"
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ==================== ASYNC I/O TASK ENDPOINTS (FastAPI) ====================

@app.post("/io/fetch-urls")
async def fetch_urls(
        urls: List[str] = Body(...),
        timeout: int = Body(10, embed=True),
        background_tasks: BackgroundTasks = None
):
    """
    Endpoint for async I/O URL fetching
    Handled by FastAPI async (not Celery)
    """
    task_id = str(uuid.uuid4())

    # Store initial status
    async_task_status[task_id] = {
        "task_id": task_id,
        "type": "async_io_fetch_urls",
        "status": "pending",
        "created_at": datetime.now().isoformat(),
        "urls_count": len(urls)
    }

    # Define async function to run
    async def run_fetch_task():
        try:
            async_task_status[task_id]["status"] = "running"
            async_task_status[task_id]["started_at"] = datetime.now().isoformat()

            # Run the async I/O task
            result = await async_io_task1(urls, timeout)

            async_task_status[task_id].update({
                "status": "completed",
                "completed_at": datetime.now().isoformat(),
                "result": result,
                "success": True
            })

        except Exception as e:
            async_task_status[task_id].update({
                "status": "failed",
                "completed_at": datetime.now().isoformat(),
                "error": str(e),
                "success": False
            })

    # Run in background if background_tasks is provided
    if background_tasks:
        background_tasks.add_task(run_fetch_task)
        return {
            "message": "Async URL fetch task started in background",
            "task_id": task_id,
            "execution": "background",
            "status_endpoint": f"/task/async/{task_id}"
        }
    else:
        # Run immediately
        return await run_fetch_task_wrapper(task_id, run_fetch_task)


@app.post("/io/process-files")
async def process_files(
        file_paths: List[str] = Body(...),
        operation: str = Body("read", embed=True),
        background_tasks: BackgroundTasks = None
):
    """
    Endpoint for async I/O file operations
    Handled by FastAPI async (not Celery)
    """
    task_id = str(uuid.uuid4())

    # Store initial status
    async_task_status[task_id] = {
        "task_id": task_id,
        "type": f"async_io_file_{operation}",
        "status": "pending",
        "created_at": datetime.now().isoformat(),
        "files_count": len(file_paths),
        "operation": operation
    }

    # Define async function to run
    async def run_file_task():
        try:
            async_task_status[task_id]["status"] = "running"
            async_task_status[task_id]["started_at"] = datetime.now().isoformat()

            # Run the async I/O task
            result = await async_io_task2(file_paths, operation)

            async_task_status[task_id].update({
                "status": "completed",
                "completed_at": datetime.now().isoformat(),
                "result": result,
                "success": True
            })

        except Exception as e:
            async_task_status[task_id].update({
                "status": "failed",
                "completed_at": datetime.now().isoformat(),
                "error": str(e),
                "success": False
            })

    # Run in background if background_tasks is provided
    if background_tasks:
        background_tasks.add_task(run_file_task)
        return {
            "message": "Async file processing task started in background",
            "task_id": task_id,
            "execution": "background",
            "status_endpoint": f"/task/async/{task_id}"
        }
    else:
        # Run immediately
        return await run_fetch_task_wrapper(task_id, run_file_task)


async def run_fetch_task_wrapper(task_id: str, task_coro):
    """Wrapper to run async task and return result"""
    try:
        await task_coro()
        status = async_task_status[task_id]

        if status["status"] == "completed":
            return {
                "task_id": task_id,
                "status": "completed",
                "result": status["result"]
            }
        else:
            raise HTTPException(
                status_code=500,
                detail=status.get("error", "Task failed")
            )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ==================== TASK STATUS ENDPOINTS ====================

@app.get("/task/{task_id}")
async def get_task_status(task_id: str):
    """Get status of a Celery task"""
    try:
        task_result = AsyncResult(task_id, app=celery)

        response = {
            "task_id": task_id,
            "status": task_result.status,
            "ready": task_result.ready(),
            "successful": task_result.successful() if task_result.ready() else None,
        }

        if task_result.ready():
            if task_result.successful():
                response["result"] = task_result.result
            else:
                response["error"] = str(task_result.result)
                response["traceback"] = task_result.traceback

        # Add queue info if available
        try:
            task_info = task_result._get_task_meta()
            response["queue"] = task_info.get("queue", "unknown")
            response["worker"] = task_info.get("worker", "unknown")
        except:
            pass

        return response

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/task/async/{task_id}")
async def get_async_task_status(task_id: str):
    """Get status of an async I/O task"""
    if task_id not in async_task_status:
        raise HTTPException(status_code=404, detail="Task not found")

    return async_task_status[task_id]


@app.get("/tasks")
async def list_tasks(
        task_type: Optional[str] = Query(None, description="Filter by task type"),
        status: Optional[str] = Query(None, description="Filter by status"),
        limit: int = Query(50, description="Maximum tasks to return")
):
    """List all tasks (both Celery and Async)"""
    try:
        # Get Celery tasks
        inspector = celery.control.inspect()

        all_tasks = []

        # Active tasks
        active = inspector.active() or {}
        for worker, tasks in active.items():
            for task in tasks:
                all_tasks.append({
                    "type": "celery",
                    "status": "active",
                    "worker": worker,
                    "task_id": task.get("id"),
                    "name": task.get("name"),
                    "args": task.get("args"),
                    "queue": task.get("delivery_info", {}).get("routing_key", "unknown"),
                    "time_start": task.get("time_start"),
                })

        # Reserved tasks
        reserved = inspector.reserved() or {}
        for worker, tasks in reserved.items():
            for task in tasks:
                all_tasks.append({
                    "type": "celery",
                    "status": "reserved",
                    "worker": worker,
                    "task_id": task.get("id"),
                    "name": task.get("name"),
                    "queue": task.get("delivery_info", {}).get("routing_key", "unknown"),
                })

        # Add async tasks
        for task_id, task_info in list(async_task_status.items())[:limit // 2]:
            all_tasks.append({
                "type": "async",
                "status": task_info["status"],
                "task_id": task_id,
                "task_type": task_info["type"],
                "created_at": task_info.get("created_at"),
                "completed_at": task_info.get("completed_at"),
            })

        # Apply filters
        if task_type:
            all_tasks = [t for t in all_tasks if t.get("type") == task_type]

        if status:
            all_tasks = [t for t in all_tasks if t.get("status") == status]

        return {
            "total": len(all_tasks),
            "tasks": all_tasks[:limit]
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ==================== BATCH PROCESSING ====================

@app.post("/batch/mixed")
async def batch_mixed_tasks(
        cpu_data: Dict[str, Any] = Body(None),
        io_urls: List[str] = Body(None),
        background_tasks: BackgroundTasks = None
):
    """
    Process a mix of CPU and I/O tasks concurrently
    Demonstrates the hybrid architecture
    """
    batch_id = str(uuid.uuid4())
    results = {
        "batch_id": batch_id,
        "timestamp": datetime.now().isoformat(),
        "cpu_tasks": [],
        "io_tasks": [],
        "completed": False
    }

    # Submit CPU task if data provided
    if cpu_data:
        cpu_task = cpu_heavy_task1.apply_async(
            args=[cpu_data],
            task_id=f"{batch_id}_cpu"
        )
        results["cpu_tasks"].append({
            "task_id": cpu_task.id,
            "status": "submitted",
            "queue": "cpu_normal"
        })

    # Submit async I/O task if URLs provided
    if io_urls:
        io_task_id = f"{batch_id}_io"

        async def run_io_batch():
            try:
                io_result = await async_io_task1(io_urls)
                results["io_tasks"].append({
                    "task_id": io_task_id,
                    "status": "completed",
                    "result": io_result
                })
            except Exception as e:
                results["io_tasks"].append({
                    "task_id": io_task_id,
                    "status": "failed",
                    "error": str(e)
                })

            # Check if all tasks are done
            all_done = all(
                t["status"] in ["completed", "failed"]
                for t in results["cpu_tasks"] + results["io_tasks"]
            )
            results["completed"] = all_done

        if background_tasks:
            background_tasks.add_task(run_io_batch)
        else:
            await run_io_batch()

    return results


# ==================== STARTUP/SHUTDOWN ====================

@app.on_event("startup")
async def startup_event():
    """Initialize on startup"""
    print(f"🚀 Hybrid System Starting")
    print(f"   CPU Cores: {SYSTEM_CONFIG['cpu_cores']}")
    print(f"   Redis: {SYSTEM_CONFIG['redis_host']}:{SYSTEM_CONFIG['redis_port']}")
    print(f"   FastAPI: http://localhost:{FASTAPI_CONFIG['port']}")
    print(f"   Docs: http://localhost:{FASTAPI_CONFIG['port']}/docs")


@app.on_event("shutdown")
async def shutdown_event():
    """Cleanup on shutdown"""
    print("🛑 Hybrid System Shutting Down")
    redis_client.close()