import logging
import asyncio
import threading
from tools.logger import init_log
from tools.config import init_config
from common.health_checker import HealthChecker
from common.heartbeat import Heartbeat
from dotenv import load_dotenv

load_dotenv()

def main():
    config = init_config()
    logging_level = config["LOGGING_LEVEL"].upper()
    heartbeat_port = int(config["HEARTBEAT_PORT"])
    ping_interval = int(config["PING_INTERVAL"])
    services = config["SERVICES"]
    max_concurrent_health_checks = int(config["MAX_CONCURRENT_HEALTH_CHECKS"])
    grace_period = int(config["GRACE_PERIOD"])
    max_retries = int(config["MAX_RETRIES"])
    skip_grace_period = config["SKIP_GRACE_PERIOD"]
    
    init_log(logging_level)

    logging.info("Starting health checker")

    health_checker = HealthChecker(ping_interval, services, max_concurrent_health_checks, grace_period, max_retries, skip_grace_period)
    heartbeat_server = Heartbeat(heartbeat_port)
    heartbeat_thread = threading.Thread(
        target=heartbeat_server.run)
    
    try:
        heartbeat_thread.start()
        asyncio.run(health_checker.run())
    except Exception as e:
        logging.critical(f"Failed health checker: {e}")
    finally:
        heartbeat_server.stop()
        heartbeat_thread.join()

if __name__ == "__main__":
    main()