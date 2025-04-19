from configparser import ConfigParser
import os

from dotenv import load_dotenv


def init_config() -> dict[str, str | int]:
    load_dotenv()
    config = ConfigParser()
    config.read('./tools/config.ini')

    config_params: dict[str, str | int] = {}
    try:
        config_params["port"] = int(
            os.getenv('SERVER_PORT') or config["DEFAULT"]["SERVER_PORT"])
        config_params["listen_backlog"] = int(
            os.getenv('SERVER_LISTEN_BACKLOG') or config["DEFAULT"]["SERVER_LISTEN_BACKLOG"])
        config_params["logging_level"] = os.getenv(
            'LOGGING_LEVEL') or config["DEFAULT"]["LOGGING_LEVEL"]
    except KeyError as e:
        raise KeyError(
            "Key was not found. Error: {} .Aborting server".format(e))
    except ValueError as e:
        raise ValueError(
            "Key could not be parsed. Error: {}. Aborting server".format(e))

    return config_params
