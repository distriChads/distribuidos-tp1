from configparser import ConfigParser
import os

from dotenv import load_dotenv


def init_config() -> dict[str, str | int]:
    """
    Loads the configuration from the .env file, the existing environment and the config.ini file.
    Raises ValueError if a critical parameter is missing.
    """
    load_dotenv()
    config = ConfigParser()
    config.read(["./tools/config.ini", "./client_handler/tools/config.ini"])

    config_params: dict[str, str | int] = {}
    load_environmental_variables(config, config_params)

    return config_params


def load_environmental_variables(
    config: ConfigParser, config_params: dict[str, str | int]
):
    """
    Loads environmental variables from the .env file, the existing environment and the config.ini file.
    Raises ValueError if a critical parameter is missing.
    """
    try:
        setup_client_handler_config_esentials(config, config_params)
        load_exchange_config(config_params)
    except KeyError as e:
        raise KeyError(
            "Key was not found. Error: {} .Aborting client_handler".format(e)
        )
    except ValueError as e:
        raise ValueError(
            "Key could not be parsed. Error: {}. Aborting client_handler".format(e)
        )


def load_exchange_config(config_params):
    """
    Loads the exchange configuration from the .env file, the existing environment and the config.ini file.
    """
    config_params["INPUT_ROUTINGKEY"] = os.getenv("INPUT_ROUTINGKEY")

    output_node_names = [
        "FILTER_ARG",
        "FILTER_ONE_COUNTRY",
        "JOIN_MOVIES_RATING",
        "JOIN_MOVIES_CREDITS",
        "MACHINE_LEARNING",
    ]
    for node_name in output_node_names:
        output_routing_keys = os.getenv(f"OUTPUT_ROUTINGKEYS_{node_name}")
        output_routing_keys = output_routing_keys.split(",")
        config_params[node_name.lower()] = output_routing_keys


def setup_client_handler_config_esentials(config, config_params):
    """
    Sets up the client handler essential configuration parameters.
    """
    config_params["eof_expected"] = int(
        os.getenv("EOF_EXPECTED") or config["DEFAULT"]["EOF_EXPECTED"]
    )
    config_params["port"] = int(
        os.getenv("CLIENT_HANDLER_PORT") or config["DEFAULT"]["CLIENT_HANDLER_PORT"]
    )
    config_params["listen_backlog"] = int(
        os.getenv("CLIENT_HANDLER_LISTEN_BACKLOG")
        or config["DEFAULT"]["CLIENT_HANDLER_LISTEN_BACKLOG"]
    )
    config_params["logging_level"] = (
        os.getenv("LOGGING_LEVEL") or config["DEFAULT"]["LOGGING_LEVEL"]
    )
    config_params["CLI_WORKER_BROKER"] = (
        os.getenv("CLI_WORKER_BROKER") or config["DEFAULT"]["CLI_WORKER_BROKER"]
    )
    config_params["heartbeat_port"] = int(
        os.getenv("HEARTBEAT_PORT") or config["DEFAULT"]["HEARTBEAT_PORT"]
    )
    config_params["state_file_path"] = (
        os.getenv("STATE_FILE_PATH") or config["DEFAULT"]["STATE_FILE_PATH"]
    )
