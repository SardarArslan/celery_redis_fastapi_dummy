# Celery Redis FastAPI App
### The purpose of this app was to build an app using celery for background tasks and redis as a broker (task queue) and backend (result store)

## Setup
- Install uv using "curl -LsSf https://astral.sh/uv/install.sh | sh" command
- Clone the repo using "git clone https://github.com/SardarArslan/celery_redis_fastapi.git" and run "cd celery_redis_fastapi" in terminal
- Run uv sync command in terminal
- Run "brew install redis" in terminal (macOS)
- Start the server using "brew services start redis" inm terminal
- Test redis server by running "redis-cli ping" in terminal, it should return PONG
- Run Celery server using "celery -A src.celery_worker.celery worker" command in terminal
- Start FastAPI server by running "uv run uvicorn src.main:app" in terminal
- Test the app on Swagger at http://127.0.0.1:8000/docs