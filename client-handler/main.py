import logging
import threading
from common.clientHandler import ClientHandler, ClientHandlerConfig
from common.worker import ExchangeSpec
from common.heartbeat import heartbeat
from tools.logger import init_log
from tools.config import init_config


def main():
    config = init_config()
    logging_level = config["logging_level"].upper()
    port = config["port"]
    listen_backlog = config["listen_backlog"]
    heartbeat_port = config["heartbeat_port"]

    eof_expected = config["eof_expected"]

    routing_keys_input = config["INPUT_ROUTINGKEY"].split(",")
    routing_keys_output = {
        "filter_arg": config["filter_arg"],
        "filter_one_country": config["filter_one_country"],
        "join_movies_rating": config["join_movies_rating"],
        "join_movies_credits": config["join_movies_credits"],
        "machine_learning": config["machine_learning"],
    }

    init_log(logging_level)

    logging.info(
        f"action: config\nport: {port}\n"
        f"listen_backlog: {listen_backlog}\n"
        f"logging_level: {logging_level}\n"
        f"rabbitmq_host: {config['CLI_WORKER_BROKER']}\n"
        f"input_routing_keys: {routing_keys_input}\n"
        f"output_routing_keys: {routing_keys_output}\n\n"
    )

    exchange = ExchangeSpec(
        input_routing_keys=routing_keys_input,
        output_routing_keys=routing_keys_output,
        queue_name="client_handler_queue"
    )
    message_broker = config["CLI_WORKER_BROKER"]

    client_handler_config = ClientHandlerConfig(
        exchange=exchange,
        message_broker=message_broker
    )

    worker = ClientHandler(
        port=config["port"],
        listen_backlog=listen_backlog,
        client_handler_config=client_handler_config,
        eof_expected=eof_expected
    )

    ctx = threading.Event()
    heartbeat_thread = threading.Thread(
        target=heartbeat, args=(heartbeat_port, ctx))

    try:
        heartbeat_thread.start()
        worker.run()
    except Exception as e:
        logging.critical(f"Failed client handler: {e}")
    finally:
        ctx.set()
        heartbeat_thread.join()


if __name__ == "__main__":
    main()
