from configparser import ConfigParser
import os

from dotenv import load_dotenv


def init_config() -> dict[str, str | int]:
    load_dotenv()
    config = ConfigParser()
    config.read('./tools/config.ini')

    config_params: dict[str, str | int] = {}
    try:
        config_params["client_handler_port"] = int(
            os.getenv('CLIENT_HANDLER_PORT') or config["DEFAULT"]["CLIENT_HANDLER_PORT"])
        config_params["logging_level"] = os.getenv(
            'LOGGING_LEVEL') or config["DEFAULT"]["LOGGING_LEVEL"]
        config_params["client_handler_address"] = os.getenv(
            'CLIENT_HANDLER_ADDRESS') or config["DEFAULT"]["CLIENT_HANDLER_ADDRESS"]
        config_params["storage_path"] = os.getenv(
            'STORAGE_PATH') or config["DEFAULT"]["STORAGE_PATH"]
    except KeyError as e:
        raise KeyError(
            "Key was not found. Error: {} .Aborting client_handler".format(e))
    except ValueError as e:
        raise ValueError(
            "Key could not be parsed. Error: {}. Aborting client_handler".format(e))

    return config_params
