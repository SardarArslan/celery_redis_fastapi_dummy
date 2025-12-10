#!/bin/bash

echo "=========================================="
echo "Stopping Hybrid System"
echo "=========================================="

echo "1. Stopping FastAPI server..."
pkill -f "uvicorn src.main:app" 2>/dev/null
echo "   ✅ FastAPI stopped"

echo "2. Stopping Flower monitoring..."
pkill -f "celery.*flower" 2>/dev/null
echo "   ✅ Flower stopped"

echo "3. Stopping Celery workers..."
pkill -f "celery.*worker" 2>/dev/null
echo "   ✅ Celery workers stopped"

echo "4. Optional: Stop Redis (if started by script)..."
redis-cli shutdown 2>/dev/null && echo "   ✅ Redis stopped" || echo "   ⏭️  Redis not stopped (might be running elsewhere)"

echo ""
echo "✅ All services stopped!"
echo ""

# Check if anything is still running
echo "Checking for remaining processes..."
ps aux | grep -E "(celery|uvicorn|flower)" | grep -v grep

if [ $? -eq 0 ]; then
    echo "⚠️  Some processes still running. Force killing..."
    pkill -9 -f "celery" 2>/dev/null
    pkill -9 -f "uvicorn" 2>/dev/null
    pkill -9 -f "flower" 2>/dev/null
fi

echo ""
echo "Done! 🎉"