import logging
from tools.logger import init_log
from tools.config import init_config

from client import Client


def main():
    config = init_config()
    logging_level = config["logging_level"].upper()
    port = config["client_handler_port"]

    init_log(logging_level)

    logging.debug(
        f"action: config | client_handler_port: {port} | logging_level: {logging_level}")

    client = Client(config["client_handler_address"],
                    config["client_handler_port"], config["storage_path"])

    try:
        client.run()
    except Exception as e:
        logging.critical(f"Failed client handler: {e}")
        return


if __name__ == "__main__":
    main()
