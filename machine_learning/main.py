from common.clientHandler import MachineLearning, MachineLearningConfig
from common.worker import ExchangeSpec
from tools.config import load_config


def main():
    
    config = load_config()
    log_level = config["log.level"]
    input_exchange_spec = ExchangeSpec(
        name=config["worker.exchange.input.name"],
        routing_keys=[config["worker.exchange.input.routingKeys"]],
        queue_name="machine_learning_queue"
    )
    output_exchange_spec = ExchangeSpec(
        name=config["worker.exchange.output.name"],
        routing_keys=[config["worker.exchange.output.routingKeys"]],
        queue_name="machine_learning_queue"
    )
    message_broker = config["worker.broker"]

    filter_config = MachineLearningConfig(
        input_exchange=input_exchange_spec,
        output_exchange=output_exchange_spec,
        message_broker=message_broker
    )

    worker = MachineLearning(filter_config)

    try:
        worker.run_worker()
    except Exception as e:
        print("Triste")
    finally:
        worker.worker.close_worker()

if __name__ == "__main__":
    main()