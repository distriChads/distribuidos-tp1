import logging
from tools.logger import init_log

from client import Client


def main():
    logging_level = "INFO"
    port = 3000

    init_log(logging_level)

    logging.debug(
        f"action: config | server_port: {port} | logging_level: {logging_level}")

    client = Client("localhost", 3000, "../datasets")

    try:
        client.run()
    except Exception as e:
        logging.critical(f"Failed server: {e}")
        return


if __name__ == "__main__":
    main()
