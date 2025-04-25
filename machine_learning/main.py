from common.clientHandler import MachineLearning, MachineLearningConfig
from common.worker import ExchangeSpec
from tools.config import load_config, config_string
from tools.logger import init_log
import logging

log = logging.getLogger(__name__)


def main():
    config = load_config()
    log_level = config["log.level"]
    init_log(log_level)

    log.info(f"Machine Learning worker config: {config_string(config)}")

    input_routing_keys = config["worker.exchange.input.routingkeys"]
    output_routing_keys = config["worker.exchange.output.routingkeys"]

    queue_name = config["worker.queue.name"]

    input_exchange_spec = ExchangeSpec(
        name=config["worker.exchange.input.name"],
        routing_keys=input_routing_keys,
        queue_name=queue_name
    )
    output_exchange_spec = ExchangeSpec(
        name=config["worker.exchange.output.name"],
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

    try:
        worker.run_worker()
    except Exception as e:
        log.error(f"Exception when running worker: {e}")
    # finally:
    #     worker.worker.close_worker()


if __name__ == "__main__":
    main()
