
import time
import traceback
import logging
from typing import Callable, Any

logger = logging.getLogger("Supervisor")

class ServiceSupervisor:
    """
    Wraps a main execution loop with error handling and automatic restarts.
    Prevents the container from exiting on unhandled exceptions.
    """
    def __init__(self, service_name: str, sleep_on_error: int = 5):
        self.service_name = service_name
        self.sleep_on_error = sleep_on_error

    def run(self, func: Callable[[], Any]):
        """
        Run the provided function in an infinite loop.
        If it crashes, log the error, sleep, and restart.
        """
        logger.info(f"🛡️ Starting Supervisor for {self.service_name}")
        
        while True:
            try:
                func()
            except KeyboardInterrupt:
                logger.info(f"🛑 {self.service_name} stopped by user.")
                break
            except Exception as e:
                logger.error(f"💥 {self.service_name} Crashed! Restarting in {self.sleep_on_error}s...")
                logger.error(f"Error: {str(e)}")
                logger.error(traceback.format_exc())
                time.sleep(self.sleep_on_error)
                logger.info(f"🔄 Restarting {self.service_name}...")
