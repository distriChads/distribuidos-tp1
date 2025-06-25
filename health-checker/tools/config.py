from configparser import ConfigParser
import os

from dotenv import load_dotenv


def init_config() -> dict[str, str | int]:
    load_dotenv()
    config = ConfigParser()
    config.read(['./tools/config.ini', './health-checker/tools/config.ini'])

    config_params: dict[str, str | int] = {}
    load_environmental_variables(config, config_params)

    return config_params


def load_environmental_variables(config: ConfigParser, config_params: dict[str, str | int]):
    try:
        setup_health_checker_config_esentials(config, config_params)
    except KeyError as e:
        raise KeyError(
            "Key was not found. Error: {} .Aborting health_checker".format(e))
    except ValueError as e:
        raise ValueError(
            "Key could not be parsed. Error: {}. Aborting health_checker".format(e))

def setup_health_checker_config_esentials(config, config_params):
    config_params["LOGGING_LEVEL"] = os.getenv(
        'CLI_LOGGING_LEVEL') or config["DEFAULT"]["LOGGING_LEVEL"]
    config_params["PING_INTERVAL"] = int(
        os.getenv('CLI_PING_INTERVAL') or config["DEFAULT"]["PING_INTERVAL"])
    config_params["SERVICES"] = (os.getenv(
        'CLI_SERVICES') or config["DEFAULT"]["SERVICES"]).split(",")
    config_params["HEARTBEAT_PORT"] = int(
        os.getenv('CLI_HEARTBEAT_PORT') or config["DEFAULT"]["HEARTBEAT_PORT"])
    config_params["MAX_CONCURRENT_HEALTH_CHECKS"] = int(
        os.getenv('CLI_MAX_CONCURRENT_HEALTH_CHECKS') or config["DEFAULT"]["MAX_CONCURRENT_HEALTH_CHECKS"])
    config_params["GRACE_PERIOD"] = int(
        os.getenv('CLI_GRACE_PERIOD') or config["DEFAULT"]["GRACE_PERIOD"])
    config_params["MAX_RETRIES"] = int(
        os.getenv('CLI_MAX_RETRIES') or config["DEFAULT"]["MAX_RETRIES"])
    config_params["SKIP_GRACE_PERIOD"] = os.getenv('CLI_SKIP_GRACE_PERIOD', False) or config["DEFAULT"]["SKIP_GRACE_PERIOD"]
