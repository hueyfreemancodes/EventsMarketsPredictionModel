import time
import subprocess
import logging
import os
from datetime import datetime

# Setup Logging
LOG_FILE = "logs/watchdog.log"
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format='%(asctime)s - WATCHDOG - %(message)s'
)
console = logging.StreamHandler()
console.setLevel(logging.INFO)
logging.getLogger('').addHandler(console)

def run_cmd(cmd):
    try:
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
        return result.stdout.strip()
    except Exception as e:
        logging.error(f"Command failed: {cmd} | Error: {e}")
        return ""

def check_docker_container(container_name):
    """Returns True if container is running."""
    output = run_cmd(f"docker ps --filter name={container_name} --format '{{{{.Names}}}}'")
    return container_name in output

def check_python_process(script_name):
    """Returns True if python script is running."""
    # pgrep -f matches full command line
    output = run_cmd(f"pgrep -f {script_name}")
    return bool(output)

def restart_docker(container_name):
    logging.warning(f"⚠️  Container {container_name} is DOWN. Restarting...")
    run_cmd(f"docker restart {container_name}")
    time.sleep(10) # Wait for startup
    if check_docker_container(container_name):
        logging.info(f"✅ Container {container_name} successfully restarted.")
    else:
        logging.error(f"❌ Failed to restart {container_name}.")

def restart_shadow_mode():
    script = "scripts/run_shadow_mode.py"
    logging.warning(f"⚠️  Script {script} is DOWN. Restarting...")
    
    # Run with nohup, unbuffered
    cmd = f"nohup python3 -u {script} >> logs/shadow_watchdog_restart.log 2>&1 &"
    subprocess.Popen(cmd, shell=True)
    
    time.sleep(5)
    if check_python_process(script):
        logging.info(f"✅ Script {script} successfully restarted.")
    else:
        logging.error(f"❌ Failed to restart {script}.")

def monitor_loop():
    logging.info("🐶 System Watchdog Started. Checking every 60 minutes...")
    
    while True:
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        logging.info(f"--- Health Check: {timestamp} ---")
        
        # 1. Check Live Bot (Docker)
        if not check_docker_container("nba_bot"):
            restart_docker("nba_bot")
        else:
            logging.info("✅ Live Bot (Docker) is RUNNING.")

        # 2. Check Data Collector (Docker)
        if not check_docker_container("nba_collector"):
            restart_docker("nba_collector")
        else:
            logging.info("✅ Data Collector (Docker) is RUNNING.")

        # 3. Check Shadow Bot (Python)
        if not check_python_process("run_shadow_mode.py"):
            restart_shadow_mode()
        else:
            logging.info("✅ Shadow Bot (Python) is RUNNING.")

        # Sleep for 1 hour
        logging.info("Sleeping for 60 minutes...")
        time.sleep(3600)

if __name__ == "__main__":
    # Ensure logs dir exists
    os.makedirs("logs", exist_ok=True)
    monitor_loop()
