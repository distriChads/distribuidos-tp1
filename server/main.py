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

    logging.debug(
        f"action: config | port: {port} | "f"listen_backlog: {listen_backlog} | logging_level: {logging_level}")

    ch = ClientHandler(config["port"])

    try:
        ch.run()
    except Exception as e:
        logging.critical(f"Failed server: {e}")
        return
    finally:
        ch.stop()


if __name__ == "__main__":
    main()
