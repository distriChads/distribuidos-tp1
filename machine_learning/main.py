from common.clientHandler import MachineLearning, MachineLearningConfig
from common.worker import ExchangeSpec
from tools.config import load_config


def main():
    config = load_config()
    log_level = config["log.level"]

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
        print("Triste")
    # finally:
        # worker.worker.close_worker()


if __name__ == "__main__":
    main()
