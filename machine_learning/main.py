from dotenv import load_dotenv
import threading
from common.heartbeat import Heartbeat
from os import getenv
from common.clientHandler import MachineLearning, MachineLearningConfig
from common.worker import ExchangeSpec
from tools.config import load_config, config_string
from tools.logger import init_log
import logging

log = logging.getLogger(__name__)


def main():
    load_dotenv()
    message_broker = getenv("CLI_WORKER_BROKER", "default_broker")
    log_level = getenv("CLI_LOG_LEVEL", "INFO").upper()
    input_routing_key = getenv("ROUTINGKEYS_INPUT")
    output_routing_keys = getenv("ROUTINGKEYS_OUTPUT_GROUP-BY-OVERVIEW-AVERAGE").split(
        ",")

    routing_keys_output = {
        "overview_average": output_routing_keys,
    }

    init_log(log_level)

    log.info(f"Machine Learning worker config:\n\
        message_broker: {message_broker}\n\
        input_routing_key: {input_routing_key}\n\
        output_routing_keys: {output_routing_keys}\n\
        log_level: {log_level}\n\n")

    heartbeat_port = int(getenv("CLI_HEARTBEAT_PORT", "0"))
    if heartbeat_port <= 0:
        log.error("Invalid heartbeat port specified. Exiting.")
        return

    heartbeat_server = Heartbeat(heartbeat_port)
    heartbeat_thread = threading.Thread(
        target=heartbeat_server.run)
    exchange = ExchangeSpec(
        
        input_routing_key=input_routing_key,
        output_routing_keys=routing_keys_output,
    )

    ml_config = MachineLearningConfig(
        exchange=exchange,
        message_broker=message_broker,
    )

    worker = MachineLearning(ml_config)

    try:
        heartbeat_thread.start()
        worker.run_worker()
    except Exception as e:
        log.error(f"Exception when running worker: {e}")
    finally:
        heartbeat_server.stop()
        heartbeat_thread.join()


if __name__ == "__main__":
    main()
