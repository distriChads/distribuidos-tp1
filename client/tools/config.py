from configparser import ConfigParser
import os


def init_config():
    config = ConfigParser(os.environ)
    config.read('config.ini')

    config_params = {}
    try:
        # config_params["server_port"] = int(
        #     os.getenv('SERVER_PORT', config["DEFAULT"]["SERVER_PORT"]))
        # config_params["logging_level"] = os.getenv(
        #     'LOGGING_LEVEL', config["DEFAULT"]["LOGGING_LEVEL"])
        config_params["server_port"] = 3000
        config_params["logging_level"] = "INFO"
    except KeyError as e:
        raise KeyError(
            "Key was not found. Error: {} .Aborting server".format(e))
    except ValueError as e:
        raise ValueError(
            "Key could not be parsed. Error: {}. Aborting server".format(e))

    return config_params
