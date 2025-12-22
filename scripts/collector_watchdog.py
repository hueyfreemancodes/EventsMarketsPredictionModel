#!/usr/bin/env python3
"""
Collector Watchdog
------------------
Monitors Polymarket and Kalshi data collectors.
Restarts them if they crash or stop writing to logs.
"""
import os
import sys
import time
import signal
import subprocess
from pathlib import Path
from datetime import datetime

# Configuration
THRESHOLD_SECONDS = 15 * 60  # 15 minutes
PROJECT_ROOT = Path(__file__).resolve().parent.parent
LOGS_DIR = PROJECT_ROOT / "logs"
PIDS_FILE = PROJECT_ROOT / "collector_pids.txt"

# Ensure directories exist
LOGS_DIR.mkdir(exist_ok=True)

def get_venv_python():
    """Returns the path to the venv python if it exists, else system python."""
    venv_python = PROJECT_ROOT / "venv" / "bin" / "python"
    return str(venv_python) if venv_python.exists() else sys.executable

PYTHON_EXE = get_venv_python()

COLLECTORS = [
    {
        "name": "Polymarket",
        "log_file": LOGS_DIR / "polymarket.log",
        "cmd": [PYTHON_EXE, "scripts/run_targeted_collector.py"],
        "env": {"COLLECTION_DURATION_SECONDS": "-1"},  # Continuous mode
        "cwd": PROJECT_ROOT
    },
    {
        "name": "Kalshi",
        "log_file": LOGS_DIR / "kalshi.log",
        "cmd": [PYTHON_EXE, "scripts/run_kalshi_collector.py"],
        "env": {},
        "cwd": PROJECT_ROOT
    }
]

def _log(msg):
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {msg}")

def is_running(pid):
    """Check if a process with the given PID is currently running."""
    try:
        os.kill(int(pid), 0)
        return True
    except (OSError, ValueError):
        return False

def kill_process(pid):
    """Aggressively kills a process."""
    if not pid: return
    
    pid = int(pid)
    try:
        os.kill(pid, signal.SIGTERM)
        time.sleep(1)
        if is_running(pid):
            _log(f"Force killing PID {pid}...")
            os.kill(pid, signal.SIGKILL)
    except OSError:
        pass  # Process likely already gone

def start_collector(config):
    """Starts a collector process and returns its PID."""
    _log(f"🔄 Starting {config['name']}...")
    
    # Prepare environment
    env = os.environ.copy()
    env.update(config.get("env", {}))
    
    try:
        with open(config['log_file'], "a") as log_sc:
            process = subprocess.Popen(
                config['cmd'],
                cwd=str(config['cwd']),
                stdout=log_sc,
                stderr=log_sc,
                env=env,
                start_new_session=True  # Detach from parent
            )
            return process.pid
    except Exception as e:
        _log(f"❌ Failed to start {config['name']}: {e}")
        return None

def check_and_recover():
    """Main watchdog loop."""
    _log("🐶 Watchdog active.")
    
    # Load existing PIDs
    current_pids = []
    if PIDS_FILE.exists():
        try:
            current_pids = [x.strip() for x in PIDS_FILE.read_text().splitlines()]
        except Exception:
            _log("Could not read PIDs file. Assuming clean state.")

    # Pad pid list if too short
    while len(current_pids) < len(COLLECTORS):
        current_pids.append(None)

    new_pids = []
    
    for idx, col in enumerate(COLLECTORS):
        pid = current_pids[idx]
        needs_restart = False
        reason = ""

        # 1. Process Liveness Check
        if not pid or not is_running(pid):
            needs_restart = True
            reason = "Process dead or PID missing"
        
        # 2. Staleness Check (Log file activity)
        elif col['log_file'].exists():
            last_mod = col['log_file'].stat().st_mtime
            age = time.time() - last_mod
            if age > THRESHOLD_SECONDS:
                needs_restart = True
                reason = f"Log stale ({int(age/60)}m silence)"
        
        else:
            # PID is running but log file is missing? Weird, but restart to be safe.
            needs_restart = True
            reason = "Log file missing"

        # Action
        if needs_restart:
            _log(f"⚠️  {col['name']} ISSUE: {reason}")
            if pid: kill_process(pid)
            
            new_pid = start_collector(col)
            new_pids.append(str(new_pid) if new_pid else "0")
            
            if new_pid:
                _log(f"✅ Restarted {col['name']} (PID: {new_pid})")
        else:
            _log(f"✅ {col['name']} Healthy (PID: {pid})")
            new_pids.append(str(pid))

    # Save state
    PIDS_FILE.write_text("\n".join(new_pids) + "\n")

if __name__ == "__main__":
    check_and_recover()
