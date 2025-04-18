from configparser import ConfigParser
import os

from dotenv import load_dotenv


def init_config():
    config = ConfigParser(os.environ)
    config.read('config.ini')
    load_dotenv()

    config_params = {}
    try:
        # config_params["port"] = int(
        #     os.getenv('SERVER_PORT', config["DEFAULT"]["SERVER_PORT"]))
        # config_params["listen_backlog"] = int(
        #     os.getenv('SERVER_LISTEN_BACKLOG', config["DEFAULT"]["SERVER_LISTEN_BACKLOG"]))
        # config_params["logging_level"] = os.getenv(
        #     'LOGGING_LEVEL', config["DEFAULT"]["LOGGING_LEVEL"])
        config_params["port"] = 3000
        config_params["listen_backlog"] = 1
        config_params["logging_level"] = "debug"
    except KeyError as e:
        raise KeyError(
            "Key was not found. Error: {} .Aborting server".format(e))
    except ValueError as e:
        raise ValueError(
            "Key could not be parsed. Error: {}. Aborting server".format(e))

    return config_params
