import logging
from common.clientHandler import ClientHandler, ClientHandlerConfig
from common.worker import ExchangeSpec
from tools.logger import init_log
from tools.config import init_config


def main():
    config = init_config()
    logging_level = config["logging_level"].upper()
    port = config["port"]
    listen_backlog = config["listen_backlog"]

    init_log(logging_level)

    logging.info(
        f"action: config\nport: {port}\n"
        f"listen_backlog: {listen_backlog}\n"
        f"logging_level: {logging_level}\n"
        f"rabbitmq_host: {config['CLI_WORKER_BROKER']}\n"
        f"input_exchange: {config['CLI_WORKER_EXCHANGE_INPUT_NAME']}\n"
        f"input_routing_keys: {config['CLI_WORKER_EXCHANGE_INPUT_ROUTINGKEYS']}\n"
    )

    input_routing_keys =   config["CLI_WORKER_EXCHANGE_INPUT_ROUTINGKEYS"].split(",")
    output_routing_keys1 = config["CLI_WORKER_EXCHANGE1_OUTPUT_ROUTINGKEYS"].split(",")
    output_routing_keys2 = config["CLI_WORKER_EXCHANGE2_OUTPUT_ROUTINGKEYS"].split(",")
    output_routing_keys3 = config["CLI_WORKER_EXCHANGE3_OUTPUT_ROUTINGKEYS"].split(",")

    logging.info(f"exchange1: {config["CLI_WORKER_EXCHANGE1_OUTPUT_NAME"]}")
    logging.info(f"exchange2: {config["CLI_WORKER_EXCHANGE2_OUTPUT_NAME"]}")
    logging.info(f"exchange3: {config["CLI_WORKER_EXCHANGE3_OUTPUT_NAME"]}")

    input_exchange_spec = ExchangeSpec(
        name=config["CLI_WORKER_EXCHANGE_INPUT_NAME"],
        routing_keys=input_routing_keys,
        queue_name="client_handler_queue"
    )
    output_exchange_spec1 = ExchangeSpec(
        name=config["CLI_WORKER_EXCHANGE1_OUTPUT_NAME"],
        routing_keys=output_routing_keys1,
        queue_name="client_handler_queue"
    )
    output_exchange_spec2 = ExchangeSpec(
        name=config["CLI_WORKER_EXCHANGE2_OUTPUT_NAME"],
        routing_keys=output_routing_keys2,
        queue_name="client_handler_queue"
    )
    output_exchange_spec3 = ExchangeSpec(
        name=config["CLI_WORKER_EXCHANGE3_OUTPUT_NAME"],
        routing_keys=output_routing_keys3,
        queue_name="client_handler_queue"
    )
    message_broker = config["CLI_WORKER_BROKER"]

    client_handler_config = ClientHandlerConfig(
        input_exchange=input_exchange_spec,
        output_exchange1=output_exchange_spec1,
        output_exchange2=output_exchange_spec2,
        output_exchange3=output_exchange_spec3,
        message_broker=message_broker
    )    

    worker = ClientHandler(
        port=config["port"],
        client_handler_config=client_handler_config,
    )

    try:
        worker.run()
    except Exception as e:
        logging.critical(f"Failed server: {e}")


if __name__ == "__main__":
    main()
