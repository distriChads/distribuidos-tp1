from configparser import ConfigParser
import os

from dotenv import load_dotenv


def init_config():
    load_dotenv()
    return load_config()




def load_config(config_file="./config.ini"):
    config = ConfigParser()
    if os.path.exists(config_file):
        config.read(config_file)
        print(f"Configuración leída desde {config_file}")
    else:
        print("No se pudo leer el archivo de configuración. Usando solo variables de entorno.")

    final_config = {}
    keys = [
        "log.level",
        "worker.exchange.input.name",
        "worker.exchange.input.routingkeys",
        "worker.exchange.secondinput.name",
        "worker.exchange.secondinput.routingkeys",
        "worker.exchange.output.name",
        "worker.exchange.output.routingkeys",
        "worker.broker",
        "worker.maxmessages",
    ]

    for full_key in keys:
        env_key = "CLI_" + full_key.upper().replace(".", "_")

        value = os.getenv(env_key)
        if value is not None:
            final_config[full_key] = value
            continue

        parts = full_key.split(".")
        section = parts[0]
        subkey = ".".join(parts[1:])
        if config.has_section(section) and config.has_option(section, subkey):
            final_config[full_key] = config.get(section, subkey)


    return final_config
