import logging
from pika.exceptions import StreamLostError
import threading
import traceback
from common.clientHandler import ClientHandler, ClientHandlerConfig
from common.worker import ExchangeSpec
from common.heartbeat import Heartbeat
from tools.logger import init_log
from tools.config import init_config


def main():
    config = init_config()
    logging_level = config["logging_level"].upper()
    port = config["port"]
    listen_backlog = config["listen_backlog"]
    heartbeat_port = config["heartbeat_port"]
    state_file_path = config["state_file_path"]
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
    logging.getLogger("pika").setLevel(logging.CRITICAL)

    logging.info("Client Handler started. Config:")
    logging.info(f"port: {port}")
    logging.info(f"listen_backlog: {listen_backlog}")
    logging.info(f"logging_level: {logging_level}")
    logging.info(f"rabbitmq_host: {config['CLI_WORKER_BROKER']}")
    logging.info(f"input_routing_keys: {routing_keys_input}")
    logging.info(f"output_routing_keys: {routing_keys_output}")
    logging.info(f"state_file_path: {state_file_path}")
    
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

    try:
        client_handler = ClientHandler(
            port=config["port"],
            listen_backlog=listen_backlog,
            client_handler_config=client_handler_config,
            eof_expected=eof_expected,
            state_file_path=state_file_path
        )
    except Exception as e:
        logging.critical(f"Failed to initialize client handler: {e}\nTraceback: {traceback.format_exc()}")
        raise e

    heartbeat_server = Heartbeat(heartbeat_port)
    heartbeat_thread = threading.Thread(
        target=heartbeat_server.run)

    try:
        heartbeat_thread.start()
        client_handler.run()
    except StreamLostError:
        pass # This is expected when the client handler is closed
    except Exception as e:
        logging.critical(f"Failed client handler: {e}\nTraceback: {traceback.format_exc()}")
    finally:
        heartbeat_server.stop()
        heartbeat_thread.join()


if __name__ == "__main__":
    main()
    logging.info("Client Handler finished")
