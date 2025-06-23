from common.clientHandler import MachineLearning, MachineLearningConfig
from common.worker import ExchangeSpec
from tools.config import load_config, config_string
from tools.logger import init_log
import logging
from common.heartbeat import heartbeat
import threading

log = logging.getLogger(__name__)


def main():
    config = load_config()
    log_level = config["log.level"]
    init_log(log_level)

    log.info(f"Machine Learning worker config: {config_string(config)}")

    input_routing_keys = config["worker.exchange.input.routingkeys"]
    output_routing_keys = config["worker.exchange.output.routingkeys"]
    queue_name = "machine_learning_queue"
    port = int(config["heartbeat.port"])

    input_exchange_spec = ExchangeSpec(
        routing_keys=input_routing_keys,
        queue_name=queue_name
    )
    output_exchange_spec = ExchangeSpec(
        routing_keys=output_routing_keys,
        queue_name=queue_name
    )
    message_broker = config["worker.broker"]

    filter_config = MachineLearningConfig(
        input_exchange=input_exchange_spec,
        output_exchange=output_exchange_spec,
        message_broker=message_broker
    )

    worker = MachineLearning(filter_config, output_routing_keys)
    ctx = threading.Event()
    heartbeat_thread = threading.Thread(target=heartbeat, args=(port, ctx))

    try:
        heartbeat_thread.start()
        worker.run_worker()
    except Exception as e:
        log.error(f"Exception when running worker: {e}")
    finally:
        ctx.set()
        heartbeat_thread.join()


if __name__ == "__main__":
    main()
