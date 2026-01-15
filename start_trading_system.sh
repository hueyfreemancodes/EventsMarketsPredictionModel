#!/bin/bash

# Define log files
COLLECTOR_LOG="collector.log"
BOT_LOG="bot.log"

echo "=== Starting Event Markets Trading System ==="

# 0. Update Fundamentals (Blocking)
echo "0. Updating NBA Fundamentals..."
python3 scripts/fetch_nba_fundamentals.py
echo "   Fundamentals Updated."

# 1. Start Collector (Source)
echo "1. Launching Data Collector..."
python3 scripts/run_targeted_collector.py > "$COLLECTOR_LOG" 2>&1 &
COLLECTOR_PID=$!
echo "   PID: $COLLECTOR_PID (Logs: $COLLECTOR_LOG)"

# Wait for Collector to initialize WSS
echo "   Waiting 5s for WSS connection..."
sleep 5

# 2. Start Paper Bot (Consumer)
echo "2. Launching Paper Trading Bot..."
python3 scripts/run_paper_bot.py > "$BOT_LOG" 2>&1 &
BOT_PID=$!
echo "   PID: $BOT_PID (Logs: $BOT_LOG)"

# Function to kill processes on exit
cleanup() {
    echo ""
    echo "=== Shutting Down System ==="
    echo "Killing Collector (PID $COLLECTOR_PID)..."
    kill $COLLECTOR_PID
    echo "Killing Bot (PID $BOT_PID)..."
    kill $BOT_PID
    echo "Done."
}

# Trap Ctrl+C (SIGINT) and Exit
trap cleanup EXIT

# 3. Start Monitor (Foreground)
echo "3. Starting Dashboard..."
echo "   (Press Ctrl+C to stop everything)"
sleep 2
python3 scripts/monitor_trades.py
