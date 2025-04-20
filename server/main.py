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
        f"action: config\nport: {port}\n"f"listen_backlog: {listen_backlog}\nlogging_level: {logging_level}\nrabbitmq_host: {config['WORKER_BROKER']}")

    ch = ClientHandler(
        port=config["port"],
        rabbitmq_host=config["WORKER_BROKER"],
    )

    try:
        ch.run()
    except Exception as e:
        logging.critical(f"Failed server: {e}")


if __name__ == "__main__":
    main()
