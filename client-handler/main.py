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
    input_routing_keys = config["CLI_WORKER_EXCHANGE_INPUT_ROUTINGKEYS"].split(
        ",")
    output_routing_keys = config["CLI_WORKER_EXCHANGE_OUTPUT_ROUTINGKEYS"].split(
        ",")

    init_log(logging_level)

    logging.info(
        f"action: config\nport: {port}\n"
        f"listen_backlog: {listen_backlog}\n"
        f"logging_level: {logging_level}\n"
        f"rabbitmq_host: {config['CLI_WORKER_BROKER']}\n"
        f"input_routing_keys: {input_routing_keys}\n"
        f"output_routing_keys: {output_routing_keys}\n"
    )

    exchange = ExchangeSpec(
        input_routing_keys=input_routing_keys,
        output_routing_keys=output_routing_keys,
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
    )

    ctx = threading.Event()
    heartbeat_thread = threading.Thread(target=heartbeat, args=(heartbeat_port, ctx))

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
