#!/bin/bash

echo "🚨 EMERGENCY RESET: Killing All Trading Processes..."

# 1. Kill everything ruthlessley
pkill -f "scripts/run_"
pkill -f "start_trading_system.sh"
pkill -f "streamlit"
pkill -f "monitor_trades.py"

echo "   All processes killed."
sleep 2

# 2. Check for leftovers
LEFTOVERS=$(ps aux | grep -E "python.*scripts/" | grep -v grep | awk '{print $2}')
if [ ! -z "$LEFTOVERS" ]; then
    echo "Foce killing PIDs: $LEFTOVERS"
    kill -9 $LEFTOVERS
fi

echo "🧹 Cleaning up logs..."
rm -f logs/*.log
rm -f bot.log collector.log kalshi.log

echo "🚀 Relaunching System (Fast Mode)..."
nohup ./start_trading_system.sh > logs/system.log 2>&1 &
nohup python3 -m streamlit run scripts/dashboard.py --server.headless true > logs/dashboard.log 2>&1 &

echo "✅ DONE. System Restarted."
echo "   Monitor Dashboard: http://localhost:8501"
echo "   Monitor Logs: tail -f logs/system.log"
