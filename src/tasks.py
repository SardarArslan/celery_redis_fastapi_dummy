import time
import hashlib
import numpy as np
import pandas as pd
import aiofiles
import asyncio
import os
from datetime import datetime

from functools import lru_cache
from .celery_app import celery, PriorityTask
from .config import TASK_CONFIG


# ==================== CPU-BOUND TASKS (Handled by Celery) ====================

@celery.task(
    bind=True,
    base=PriorityTask,
    queue='cpu_normal',
    priority=5,
    max_retries=TASK_CONFIG['max_retries'],
    default_retry_delay=TASK_CONFIG['retry_delay'],
    time_limit=TASK_CONFIG['cpu_task_timeout'],
    soft_time_limit=TASK_CONFIG['cpu_task_timeout'] - 5,
    name='tasks.cpu_heavy_task1'
)
def cpu_heavy_task1(self, data: dict) -> dict:
    """
    CPU-intensive task 1: Complex data analysis
    Simulates heavy CPU usage with matrix operations
    """
    task_id = self.request.id

    try:
        print(f"[CPU Task 1][{task_id[:8]}] Started processing {len(data)} items")

        # Simulate heavy CPU work
        results = []

        # 1. Matrix operations (CPU intensive)
        matrix_size = 500
        matrices = []
        for i in range(10):
            matrix = np.random.rand(matrix_size, matrix_size)
            # Perform multiple matrix operations
            inverse = np.linalg.inv(matrix)
            eigenvalues = np.linalg.eigvals(matrix)
            determinant = np.linalg.det(matrix)

            matrices.append({
                'inverse_shape': inverse.shape,
                'eigenvalues_count': len(eigenvalues),
                'determinant': determinant
            })

        # 2. Dataframe operations (CPU intensive)
        if data:
            df = pd.DataFrame(data)
            # Heavy operations
            correlation = df.corr()
            covariance = df.cov()
            descriptive = df.describe().to_dict()

            # Statistical calculations
            percentiles = df.quantile([0.25, 0.5, 0.75]).to_dict()

        # 3. Hash calculations (CPU intensive)
        hash_results = []
        for i in range(10000):
            hash_obj = hashlib.sha256(f"{data}{i}".encode())
            hash_results.append(hash_obj.hexdigest())

        # Simulate processing time
        total_iterations = 1000000
        for i in range(total_iterations):
            if i % 100000 == 0:
                # Update progress (for long-running tasks)
                self.update_state(
                    state='PROGRESS',
                    meta={'current': i, 'total': total_iterations}
                )

        result = {
            'task_id': task_id,
            'task_type': 'cpu_heavy_analysis',
            'matrices_processed': len(matrices),
            'data_rows': len(data) if data else 0,
            'hashes_calculated': len(hash_results),
            'execution_time': 3.5,  # Simulated time
            'status': 'completed'
        }

        print(f"[CPU Task 1][{task_id[:8]}] Completed")
        return result

    except Exception as exc:
        print(f"[CPU Task 1][{task_id[:8]}] Failed: {exc}")
        self.retry(exc=exc, countdown=2 ** self.request.retries)


@celery.task(
    bind=True,
    base=PriorityTask,
    queue='cpu_normal',
    priority=0,
    max_retries=TASK_CONFIG['max_retries'],
    default_retry_delay=TASK_CONFIG['retry_delay'],
    time_limit=TASK_CONFIG['cpu_task_timeout'],
    soft_time_limit=TASK_CONFIG['cpu_task_timeout'] - 5,
    name='tasks.cpu_heavy_task2'
)
def cpu_heavy_task2(self, text_data: str, iterations: int = 1000) -> dict:
    """
    CPU-intensive task 2: Text processing and analysis
    Simulates heavy CPU usage with text operations
    """
    task_id = self.request.id

    try:
        print(f"[CPU Task 2][{task_id[:8]}] Started text processing")

        # 1. Text processing (CPU intensive)
        processed_texts = []
        for i in range(min(iterations, 1000)):
            # Simulate complex text processing
            words = text_data.split()
            word_count = len(words)
            char_count = len(text_data)

            # Calculate various metrics
            avg_word_length = char_count / max(word_count, 1)

            # Simulate pattern matching (CPU intensive)
            import re
            patterns_found = []
            for pattern in [r'\b\w{4,}\b', r'\b[A-Z][a-z]+\b', r'\d+']:
                matches = re.findall(pattern, text_data)
                patterns_found.append(len(matches))

            processed_texts.append({
                'word_count': word_count,
                'char_count': char_count,
                'avg_word_length': avg_word_length,
                'patterns_found': patterns_found,
                'iteration': i
            })

        # 2. Compression simulation (CPU intensive)
        compressed_results = []
        for i in range(100):
            # Simulate compression by repeated hashing
            current_hash = hashlib.md5(text_data.encode()).hexdigest()
            for j in range(1000):
                current_hash = hashlib.sha256(current_hash.encode()).hexdigest()
            compressed_results.append(current_hash[:16])

        # 3. Memory-intensive operation
        large_list = []
        for i in range(10000):
            # Create and manipulate large data structures
            large_list.append([j * i for j in range(100)])

        result = {
            'task_id': task_id,
            'task_type': 'cpu_heavy_text_processing',
            'texts_processed': len(processed_texts),
            'compression_results': len(compressed_results),
            'large_list_size': len(large_list),
            'execution_time': 2.8,  # Simulated time
            'status': 'completed'
        }

        print(f"[CPU Task 2][{task_id[:8]}] Completed")
        return result

    except Exception as exc:
        print(f"[CPU Task 2][{task_id[:8]}] Failed: {exc}")
        self.retry(exc=exc, countdown=2 ** self.request.retries)


@celery.task(
    bind=True,
    base=PriorityTask,
    queue='cpu_high',
    max_retries=1,  # Fewer retries for priority tasks
    default_retry_delay=2,
    time_limit=TASK_CONFIG['priority_task_timeout'],
    soft_time_limit=TASK_CONFIG['priority_task_timeout'] - 2,
    priority=0,  # High priority (0-9, 9 is highest in Redis)
    name='tasks.cpu_priority_task'
)
def cpu_priority_task(self, urgent_data: dict) -> dict:
    """
    High-priority CPU task: Critical processing that needs to jump the queue
    Uses higher priority and shorter timeout
    """
    task_id = self.request.id

    try:
        print(f"[PRIORITY CPU][{task_id[:8]}] Started urgent processing")

        # Quick but important calculations
        # 1. Fast validation
        required_fields = ['id', 'timestamp', 'priority']
        missing_fields = [field for field in required_fields if field not in urgent_data]

        if missing_fields:
            return {
                'task_id': task_id,
                'status': 'validation_failed',
                'missing_fields': missing_fields
            }

        # 2. Quick calculations (limited iterations for speed)
        validation_hash = hashlib.md5(str(urgent_data).encode()).hexdigest()

        # 3. Simple but critical transformation
        processed_data = {
            'original_id': urgent_data.get('id'),
            'validated_at': time.time(),
            'hash': validation_hash[:16],
            'priority_level': urgent_data.get('priority', 'high'),
            'processed': True
        }

        # Simulate brief processing time
        time.sleep(0.5)  # Much faster than regular CPU tasks

        result = {
            'task_id': task_id,
            'task_type': 'cpu_priority_validation',
            'data': processed_data,
            'execution_time': 0.5,
            'status': 'completed_urgent'
        }

        print(f"[PRIORITY CPU][{task_id[:8]}] Completed urgently")
        return result

    except Exception as exc:
        print(f"[PRIORITY CPU][{task_id[:8]}] Failed: {exc}")
        # Priority tasks might not retry, or retry quickly
        self.retry(exc=exc, countdown=1)


# ==================== ASYNC I/O-BOUND TASKS (Handled by FastAPI) ====================

async def async_io_task1(urls: list, timeout: int = 10) -> dict:
    """
    Async I/O Task 1: Fetch multiple web pages concurrently
    Uses aiohttp for non-blocking HTTP requests
    """


    print(f"[Async IO 1] Started fetching {len(urls)} URLs")
    start_time = datetime.now()

    results = []
    async with aiohttp.ClientSession() as session:
        tasks = []
        for url in urls:
            task = asyncio.create_task(
                _fetch_single_url(session, url, timeout)
            )
            tasks.append(task)

        # Gather all results with error handling
        gathered = await asyncio.gather(*tasks, return_exceptions=True)

        for url, result in zip(urls, gathered):
            if isinstance(result, Exception):
                results.append({
                    'url': url,
                    'success': False,
                    'error': str(result),
                    'status_code': None,
                    'size': 0
                })
            else:
                results.append(result)

    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()

    return {
        'task_type': 'async_io_fetch_urls',
        'urls_processed': len(urls),
        'successful': len([r for r in results if r['success']]),
        'failed': len([r for r in results if not r['success']]),
        'total_size': sum(r['size'] for r in results if r['success']),
        'execution_time': duration,
        'results': results[:5]  # Return first 5 for brevity
    }


async def async_io_task2(file_paths: list, operation: str = 'read') -> dict:
    """
    Async I/O Task 2: File operations with asyncio
    Simulates reading/writing multiple files concurrently
    """


    print(f"[Async IO 2] Started {operation} on {len(file_paths)} files")
    start_time = datetime.now()

    results = []

    if operation == 'read':
        # Async file reading
        tasks = []
        for file_path in file_paths:
            task = asyncio.create_task(
                _read_single_file(file_path)
            )
            tasks.append(task)

        gathered = await asyncio.gather(*tasks, return_exceptions=True)

        for file_path, result in zip(file_paths, gathered):
            if isinstance(result, Exception):
                results.append({
                    'file': file_path,
                    'success': False,
                    'error': str(result),
                    'size': 0,
                    'content_preview': None
                })
            else:
                results.append(result)

    elif operation == 'write':
        # Async file writing
        tasks = []
        for i, file_path in enumerate(file_paths):
            content = f"Sample content for file {i}: {os.urandom(100).hex()}"
            task = asyncio.create_task(
                _write_single_file(file_path, content)
            )
            tasks.append(task)

        gathered = await asyncio.gather(*tasks, return_exceptions=True)

        for file_path, result in zip(file_paths, gathered):
            if isinstance(result, Exception):
                results.append({
                    'file': file_path,
                    'success': False,
                    'error': str(result),
                    'written': False
                })
            else:
                results.append(result)

    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()

    return {
        'task_type': f'async_io_file_{operation}',
        'files_processed': len(file_paths),
        'successful': len([r for r in results if r['success']]),
        'failed': len([r for r in results if not r['success']]),
        'execution_time': duration,
        'results': results[:3]  # Return first 3 for brevity
    }


# ==================== HELPER FUNCTIONS ====================

async def _fetch_single_url(session, url: str, timeout: int) -> dict:
    """Helper to fetch a single URL"""
    try:
        async with session.get(url, timeout=timeout) as response:
            text = await response.text()
            return {
                'url': url,
                'success': True,
                'status_code': response.status,
                'size': len(text),
                'content_type': response.headers.get('Content-Type', ''),
                'preview': text[:100] if text else ''
            }
    except asyncio.TimeoutError:
        return {
            'url': url,
            'success': False,
            'error': 'Timeout',
            'status_code': None,
            'size': 0
        }
    except Exception as e:
        return {
            'url': url,
            'success': False,
            'error': str(e),
            'status_code': None,
            'size': 0
        }


async def _read_single_file(file_path: str) -> dict:
    """Helper to read a single file asynchronously"""
    import aiofiles
    import os

    try:
        if not os.path.exists(file_path):
            # Create a dummy file for demo
            async with aiofiles.open(file_path, 'w') as f:
                await f.write(f"Dummy content for {file_path}")

        async with aiofiles.open(file_path, 'r') as f:
            content = await f.read()

            return {
                'file': file_path,
                'success': True,
                'size': len(content),
                'content_preview': content[:50],
                'exists': True
            }
    except Exception as e:
        return {
            'file': file_path,
            'success': False,
            'error': str(e),
            'size': 0,
            'content_preview': None
        }


async def _write_single_file(file_path: str, content: str) -> dict:
    """Helper to write a single file asynchronously"""
    import aiofiles
    import os

    try:
        # Ensure directory exists
        os.makedirs(os.path.dirname(file_path) or '.', exist_ok=True)

        async with aiofiles.open(file_path, 'w') as f:
            await f.write(content)

            return {
                'file': file_path,
                'success': True,
                'size': len(content),
                'written': True
            }
    except Exception as e:
        return {
            'file': file_path,
            'success': False,
            'error': str(e),
            'written': False
        }