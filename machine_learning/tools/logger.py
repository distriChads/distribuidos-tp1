import logging


def init_log(logging_level: str = "DEBUG") -> None:
    """
    Python custom logging initialization

    Current timestamp is added to be able to identify in docker
    compose logs the date when the log has arrived
    """
    logging.basicConfig(
        format="%(asctime)s %(levelname)-8s %(message)s",
        level=logging_level.upper(),
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    logging.getLogger("pika").setLevel(logging.WARNING)  # Suppress pika logs
