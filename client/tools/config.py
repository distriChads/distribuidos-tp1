from configparser import ConfigParser
import os

from dotenv import load_dotenv


def init_config() -> dict[str, str | int]:
    load_dotenv()
    config = ConfigParser()
    config.read('./tools/config.ini')

    config_params: dict[str, str | int] = {}
    try:
        config_params["server_port"] = int(
            os.getenv('SERVER_PORT') or config["DEFAULT"]["SERVER_PORT"])
        config_params["logging_level"] = os.getenv(
            'LOGGING_LEVEL') or config["DEFAULT"]["LOGGING_LEVEL"]
        config_params["server_address"] = os.getenv(
            'SERVER_ADDRESS') or config["DEFAULT"]["SERVER_ADDRESS"]
        config_params["storage_path"] = os.getenv(
            'STORAGE_PATH') or config["DEFAULT"]["STORAGE_PATH"]
    except KeyError as e:
        raise KeyError(
            "Key was not found. Error: {} .Aborting server".format(e))
    except ValueError as e:
        raise ValueError(
            "Key could not be parsed. Error: {}. Aborting server".format(e))

    return config_params
