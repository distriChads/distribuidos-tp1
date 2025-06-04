import logging
from common.hasher import Hasher, HasherConfig
from common.worker import ExchangeSpec
from tools.logger import init_log
from tools.config import init_config


def main():
    config = init_config()
    logging_level = config["logging_level"].upper()
    init_log(logging_level)

    input_exchange = config["CLI_WORKER_EXCHANGE_INPUT_NAME"]
    output_exchange = config["CLI_WORKER_EXCHANGE1_OUTPUT_NAME"]

    input_routing_keys = config["CLI_WORKER_EXCHANGE_INPUT_ROUTINGKEYS"].split(
        ",")
    output_credits_routing_key = config["CLI_WORKER_EXCHANGE1_OUTPUT_ROUTINGKEYS"].split(
        ",")
    output_ratings_routing_key = config["CLI_WORKER_EXCHANGE2_OUTPUT_ROUTINGKEYS"].split(
        ",")

    message_broker = config["CLI_WORKER_BROKER"]

    movies_routing_key = input_routing_keys[0]
    credits_routing_key = input_routing_keys[1]
    ratings_routing_key = input_routing_keys[2]

    logging.info(
        f"logging_level: {logging_level}\n"
        f"rabbitmq_host: {config['CLI_WORKER_BROKER']}\n\n"

        f"input_exchange: {input_exchange}\n"
        f"input_routing_keys: {input_routing_keys}\n\n"

        f"output_exchange: {output_exchange}\n"
        f"output_routing_keys: {output_credits_routing_key}\n"
        f"output_routing_keys: {output_ratings_routing_key}\n\n"
    )

    input_exchange = ExchangeSpec(
        name=input_exchange,
        routing_keys=input_routing_keys,
        queue_name="hasher_queue"
    )

    output_exchange = ExchangeSpec(
        name=output_exchange,
        routing_keys=[output_credits_routing_key, output_ratings_routing_key],
        queue_name="hasher_queue"
    )

    hasher_config = HasherConfig(
        input_exchange=input_exchange,
        output_exchange1=output_exchange,
        message_broker=message_broker
    )

    worker = Hasher(hasher_config=hasher_config,)

    try:
        worker.run()
    except Exception as e:
        logging.critical(f"Failed client handler: {e}")


if __name__ == "__main__":
    main()
