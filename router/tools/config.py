from configparser import ConfigParser
import os

from dotenv import load_dotenv


def init_config() -> dict[str, str | int]:
    load_dotenv()
    config = ConfigParser()
    config.read(['./tools/config.ini', './client_handler/tools/config.ini'])

    config_params: dict[str, str | int] = {}
    load_environmental_variables(config, config_params)

    return config_params


def load_environmental_variables(config: ConfigParser, config_params: dict[str, str | int]):
    try:
        setup_client_handler_config_esentials(config, config_params)
        load_input_exchange_config(config, config_params)
        load_output_exchange_config(config, config_params)
    except KeyError as e:
        raise KeyError(
            "Key was not found. Error: {} .Aborting client_handler".format(e))
    except ValueError as e:
        raise ValueError(
            "Key could not be parsed. Error: {}. Aborting client_handler".format(e))


def load_output_exchange_config(config, config_params):
    config_params["CLI_WORKER_EXCHANGE1_OUTPUT_NAME"] = os.getenv(
        'CLI_WORKER_EXCHANGE1_OUTPUT_NAME') or config["DEFAULT"]["CLI_WORKER_EXCHANGE1_OUTPUT_NAME"]

    config_params["CLI_WORKER_EXCHANGE1_OUTPUT_ROUTINGKEYS"] = os.getenv(
        'CLI_WORKER_EXCHANGE1_OUTPUT_ROUTINGKEYS') or config["DEFAULT"]["CLI_WORKER_EXCHANGE1_OUTPUT_ROUTINGKEYS"]


def load_input_exchange_config(config, config_params):
    config_params["CLI_WORKER_EXCHANGE_INPUT_NAME"] = os.getenv(
        'CLI_WORKER_EXCHANGE_INPUT_NAME') or config["DEFAULT"]["CLI_WORKER_EXCHANGE_INPUT_NAME"]

    config_params["CLI_WORKER_EXCHANGE_INPUT_ROUTINGKEYS"] = os.getenv(
        'CLI_WORKER_EXCHANGE_INPUT_ROUTINGKEYS') or config["DEFAULT"]["CLI_WORKER_EXCHANGE_INPUT_ROUTINGKEYS"]


def setup_client_handler_config_esentials(config, config_params):
    config_params["logging_level"] = os.getenv(
        'LOGGING_LEVEL') or config["DEFAULT"]["LOGGING_LEVEL"]
    config_params["CLI_WORKER_BROKER"] = os.getenv(
        'CLI_WORKER_BROKER') or config["DEFAULT"]["CLI_WORKER_BROKER"]
