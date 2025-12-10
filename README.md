# Celery Redis FastAPI App
### The purpose of this app was to build an app using celery for background tasks and redis as a broker (task queue) and backend (result store)

## Setup
- Install uv using "curl -LsSf https://astral.sh/uv/install.sh | sh" command
- Clone the repo using "git clone https://github.com/SardarArslan/celery_redis_fastapi.git" and run "cd celery_redis_fastapi" in terminal
- Run uv sync command in terminal
- Run "brew install redis" in terminal (macOS)
- Start the server using "brew services start redis" in terminal
- Test redis server by running "redis-cli ping" in terminal, it should return PONG
- Run "chmod +x run.sh" then "chmod +x stop.sh" in terminal 
- Run "./run.sh" in terminal to start services and "./stop.sh" to stop them