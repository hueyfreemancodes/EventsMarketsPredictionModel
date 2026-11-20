#!/bin/bash

# Define log files
COLLECTOR_LOG="collector.log"
BOT_LOG="bot.log"

echo "=== Starting Event Markets Trading System ==="

# 0. Update Fundamentals (Blocking)
echo "0. Updating NBA Fundamentals... (Skipped for fast restart)"
# python3 scripts/fetch_nba_fundamentals.py
echo "   Fundamentals Check Skipped."

# 1. Start Collector (Source)
echo "1. Launching Data Collector..."
python3 scripts/run_targeted_collector.py > "$COLLECTOR_LOG" 2>&1 &
COLLECTOR_PID=$!
echo "   PID: $COLLECTOR_PID (Logs: $COLLECTOR_LOG)"

# Wait for Collector to initialize WSS
echo "   Waiting 5s for WSS connection..."
sleep 5

# 2. Start Kalshi Collector (Source)
echo "2. Launching Kalshi Collector..."
KALSHI_LOG="kalshi.log"
python3 scripts/run_kalshi_collector.py > "$KALSHI_LOG" 2>&1 &
KALSHI_PID=$!
echo "   PID: $KALSHI_PID (Logs: $KALSHI_LOG)"


# 3. Start Paper Bot (Consumer)
echo "3. Launching Paper Trading Bot..."
python3 scripts/run_paper_bot.py > "$BOT_LOG" 2>&1 &
BOT_PID=$!
echo "   PID: $BOT_PID (Logs: $BOT_LOG)"

# Function to kill processes on exit
cleanup() {
    echo ""
    echo "=== Shutting Down System ==="
    echo "Killing Collector (PID $COLLECTOR_PID)..."
    echo "Killing Kalshi Collector (PID $KALSHI_PID)..."
    echo "Killing Bot (PID $BOT_PID)..."
    kill $COLLECTOR_PID
    kill $KALSHI_PID
    kill $BOT_PID
    echo "Done."
}

# Trap Ctrl+C (SIGINT) and Exit
trap cleanup EXIT

# 4. Monitor Process (Keep Alive)
echo "4. Application Running."
echo "   (Background processes active. Send SIGINT to stop.)"
wait
