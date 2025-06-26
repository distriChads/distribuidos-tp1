import logging
import os
from tools.logger import init_log
from tools.config import init_config

from client import Client


def main():
    config = init_config()
    logging_level = config["logging_level"].upper()
    address = config["client_handler_address"]
    port = config["client_handler_port"]
    dataset_path = config["dataset_path"]
    max_retries = config["max_retries"]
    retry_delay = config["retry_delay"]
    backoff_factor = config["backoff_factor"]
    results_path = config["results_path"]

    if not os.path.exists(dataset_path):
        logging.critical(f"Dataset path {dataset_path} does not exist")
        return
    if not os.path.exists(results_path):
        os.makedirs(results_path)
    init_log(logging_level)

    logging.debug(
        f"action: config | client_handler_port: {port} | logging_level: {logging_level}"
    )

    client = Client(
        address,
        port,
        dataset_path,
        results_path,
        max_retries,
        retry_delay,
        backoff_factor,
    )

    try:
        client.run()
    except Exception as e:
        logging.critical(f"Failed client handler: {e}")
        return


if __name__ == "__main__":
    main()
