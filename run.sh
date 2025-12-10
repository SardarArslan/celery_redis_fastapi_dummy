#!/bin/bash

echo "=========================================="
echo "Starting Hybrid System (Fixed)"
echo "=========================================="

# Kill existing processes
echo "Stopping existing processes..."
pkill -f uvicorn 2>/dev/null
pkill -f celery 2>/dev/null
pkill -f flower 2>/dev/null
sleep 2

# Check if Redis is running
if ! redis-cli ping > /dev/null 2>&1; then
    echo "Starting Redis..."
    redis-server --daemonize yes
    sleep 2
fi

# Create logs directory
mkdir -p logs

# Start Celery Workers
echo "Starting Celery Workers..."

echo "1. Normal CPU Worker..."
celery -A src.celery_app.celery worker \
  --queues=cpu_normal \
  --concurrency=12 \
  --loglevel=info \
  --hostname=worker_normal@%h \
  --logfile=logs/celery_normal.log \
  --pidfile=logs/celery_normal.pid \
  --detach

sleep 1

echo "2. High Priority CPU Worker..."
celery -A src.celery_app.celery worker \
  --queues=cpu_high \
  --concurrency=4 \
  --loglevel=info \
  --hostname=worker_high@%h \
  --logfile=logs/celery_high.log \
  --pidfile=logs/celery_high.pid \
  --detach

sleep 2

echo "3. Starting Flower Monitoring (port 5555) in background..."
celery -A src.celery_app.celery flower \
  --port=5555 \
  --address=0.0.0.0 \
  --logfile=logs/flower.log \
  --pidfile=logs/flower.pid \
  --detach 2>/dev/null &

sleep 2

echo "4. Starting FastAPI Server (port 8000)..."
# Start FastAPI in background but redirect output to log file
uvicorn src.main:app \
  --host 0.0.0.0 \
  --port 8000 \
  --reload \
  --log-level info \
  > logs/fastapi.log 2>&1 &

sleep 3

echo ""
echo "✅ Services Started!"
echo ""
echo "📊 Flower: http://localhost:5555"
echo "🌐 FastAPI: http://localhost:8000"
echo "📚 Docs: http://localhost:8000/docs"
echo "📈 Stats: http://localhost:8000/system/stats"
echo ""
echo "Checking if services are running..."
echo ""

# Check if FastAPI is running
if curl -s --max-time 3 http://localhost:8000/health > /dev/null; then
    echo "✅ FastAPI is responding"
else
    echo "❌ FastAPI may not be running"
    echo "   Check logs: tail -f logs/fastapi.log"
fi

# Check if Flower is running
if curl -s --max-time 3 http://localhost:5555 > /dev/null; then
    echo "✅ Flower is responding"
else
    echo "⚠️  Flower may not be running (workers might need time to connect)"
fi

echo ""
echo "To stop all services: pkill -f 'uvicorn|celery|flower'"
echo "To view logs: tail -f logs/*.log"
echo "To test: curl http://localhost:8000/health"