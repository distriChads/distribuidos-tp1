import logging
from tools.logger import init_log
from tools.config import init_config

from client import Client


def main():
    config = init_config()
    logging_level = config["logging_level"].upper()
    port = config["server_port"]

    init_log(logging_level)

    logging.debug(
        f"action: config | server_port: {port} | logging_level: {logging_level}")

    client = Client(config["server_address"], config["server_port"], config["storage_path"])

    try:
        client.run()
    except Exception as e:
        logging.critical(f"Failed server: {e}")
        return


if __name__ == "__main__":
    main()
