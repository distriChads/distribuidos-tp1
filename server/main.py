import logging
from common.clientHandler import ClientHandler
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
        f"rabbitmq_host: {config['WORKER_BROKER']}\n"
        f"exchange_name: {config['EXCHANGE_NAME']}"
    )

    ch = ClientHandler(
        port=config["port"],
        rabbitmq_host=config["WORKER_BROKER"],
        exchange_name=config["EXCHANGE_NAME"]
    )

    try:
        ch.run()
    except Exception as e:
        logging.critical(f"Failed server: {e}")


if __name__ == "__main__":
    main()
