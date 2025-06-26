from configparser import ConfigParser
import os

from dotenv import load_dotenv


def init_config() -> dict[str, str | int]:
    """
    Loads the configuration from the .env file and the config.ini file.
    Raises ValueError if a critical parameter is missing.
    """
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
        config_params["dataset_path"] = os.getenv(
            'DATASET_PATH') or config["DEFAULT"]["DATASET_PATH"]
        config_params["results_path"] = os.getenv(
            'RESULTS_PATH') or config["DEFAULT"]["RESULTS_PATH"]
        config_params["max_retries"] = int(os.getenv(
            'MAX_RETRIES') or config["DEFAULT"]["MAX_RETRIES"])
        config_params["retry_delay"] = int(os.getenv(
            'RETRY_DELAY') or config["DEFAULT"]["RETRY_DELAY"])
        config_params["backoff_factor"] = int(os.getenv(
            'BACKOFF_FACTOR') or config["DEFAULT"]["BACKOFF_FACTOR"])
    except KeyError as e:
        raise KeyError(
            "Key was not found. Error: {} .Aborting client_handler".format(e))
    except ValueError as e:
        raise ValueError(
            "Key could not be parsed. Error: {}. Aborting client_handler".format(e))

    return config_params
