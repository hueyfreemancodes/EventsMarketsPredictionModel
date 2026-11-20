
import json
import time
import os
import sys
import subprocess
from datetime import datetime

REPORT_FILE = 'data/shadow_report.json'
SHADOW_SCRIPT = 'run_shadow_mode.py'

def clear_screen():
    os.system('cls' if os.name == 'nt' else 'clear')

def check_process_alive():
    try:
        # pgrep is cleaner than parsing ps aux
        result = subprocess.run(['pgrep', '-f', SHADOW_SCRIPT], stdout=subprocess.PIPE)
        return bool(result.stdout.strip())
    except:
        return False

def load_report():
    if not os.path.exists(REPORT_FILE):
        return None
    try:
        with open(REPORT_FILE, 'r') as f:
            return json.load(f)
    except:
        return None

def main():
    while True:
        alive = check_process_alive()
        status_color = "\033[92mONLINE\033[0m" if alive else "\033[91mOFFLINE\033[0m"
        
        data = load_report()
        
        clear_screen()
        print(f"==========================================")
        print(f"   SHADOW MODE COMMAND CENTER   ")
        print(f"==========================================")
        print(f"Time: {datetime.now().strftime('%H:%M:%S')}")
        print(f"Engine Status: {status_color}")
        print(f"==========================================\n")
        
        if not data:
            print("No data available yet...")
        else:
            # Header
            print(f"{'STRATEGY NAME':<30} | {'CLOSED':<8} | {'ACTIVE':<8} | {'PnL ($)':<10} | {'VOLUME'}")
            print("-" * 80)
            
            total_trades = sum(d['trades'] for d in data.values())
            
            for name, stats in data.items():
                trades = stats['trades']
                active = stats.get('active', 0)
                pnl = stats.get('pnl', 0.0)
                
                # Colorize PnL
                pnl_str = f"${pnl:>.2f}"
                if pnl > 0:
                    pnl_display = f"\033[92m{pnl_str}\033[0m" # Green
                elif pnl < 0:
                    pnl_display = f"\033[91m{pnl_str}\033[0m" # Red
                else:
                    pnl_display = pnl_str
                
                # Calculate simple visual bar
                pct = (trades / total_trades) if total_trades > 0 else 0
                bar_len = int(pct * 20)
                bar = "█" * bar_len
                
                print(f"{name:<30} | {trades:<8} | {active:<8} | {pnl_display:<10} | {bar} ({pct:.1%})")
                
        print("\nPress Ctrl+C to exit.")
        
        # Alert if offline
        if not alive:
            print("\n⚠️  WARNING: Shadow Runner is NOT running!")
            print("Restarting in 5 seconds...")
            # Auto-restart logic?
            # subprocess.Popen(['python3', 'scripts/run_shadow_mode.py'])
            
        time.sleep(2)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nExiting...")
        sys.exit(0)
