from configparser import ConfigParser
import os

from dotenv import load_dotenv


def init_config() -> dict[str, str | int]:
    load_dotenv()
    config = ConfigParser()
    config.read(['./tools/config.ini', './server/tools/config.ini'])

    config_params: dict[str, str | int] = {}
    load_environmental_variables(config, config_params)

    return config_params


def load_environmental_variables(config: ConfigParser, config_params: dict[str, str | int]):
    try:
        config_params["port"] = int(
            os.getenv('SERVER_PORT') or config["DEFAULT"]["SERVER_PORT"])
        config_params["listen_backlog"] = int(
            os.getenv('SERVER_LISTEN_BACKLOG') or config["DEFAULT"]["SERVER_LISTEN_BACKLOG"])
        config_params["logging_level"] = os.getenv(
            'LOGGING_LEVEL') or config["DEFAULT"]["LOGGING_LEVEL"]
        config_params["CLI_WORKER_BROKER"] = os.getenv(
            'CLI_WORKER_BROKER') or config["DEFAULT"]["CLI_WORKER_BROKER"]
        config_params["CLI_WORKER_EXCHANGE_INPUT_NAME"] = os.getenv(
            'CLI_WORKER_EXCHANGE_INPUT_NAME') or config["DEFAULT"]["CLI_WORKER_EXCHANGE_INPUT_NAME"]
        config_params["CLI_WORKER_EXCHANGE_INPUT_ROUTINGKEYS"] = os.getenv(
            'CLI_WORKER_EXCHANGE_INPUT_ROUTINGKEYS') or config["DEFAULT"]["CLI_WORKER_EXCHANGE_INPUT_ROUTINGKEYS"]
        config_params["CLI_WORKER_EXCHANGE_OUTPUT_NAME"] = os.getenv(
            'CLI_WORKER_EXCHANGE_OUTPUT_NAME') or config["DEFAULT"]["CLI_WORKER_EXCHANGE_OUTPUT_NAME"]
        config_params["CLI_WORKER_EXCHANGE_OUTPUT_ROUTINGKEYS"] = os.getenv(
            'CLI_WORKER_EXCHANGE_OUTPUT_ROUTINGKEYS') or config["DEFAULT"]["CLI_WORKER_EXCHANGE_OUTPUT_ROUTINGKEYS"]
    except KeyError as e:
        raise KeyError(
            "Key was not found. Error: {} .Aborting server".format(e))
    except ValueError as e:
        raise ValueError(
            "Key could not be parsed. Error: {}. Aborting server".format(e))
