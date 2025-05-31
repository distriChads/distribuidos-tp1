import json
import yaml
import sys

DEFAULT_BROKER = "amqp://guest:guest@rabbitmq:5672/"
WORKER_EXCHANGE = "data_exchange"
CLI_HANDLER_PORT = 3000

COMMON_NETWORK = "movies_net"
COMMON_NETWORKS = [COMMON_NETWORK]
COMMON_DEPENDS_ON = ["rabbitmq"]
COMMON_BUILD = {
    "context": ".",
}

BUILD_ALIAS = "&id001"
DEPENDS_ALIAS = "&id002"
NETWORKS_ALIAS = "&id003"

RABBIT_PORTS = ["5672:5672", "15672:15672"]


def rabbitmq_service(silent):
    service = {
        "image": "rabbitmq:management",
        "container_name": "rabbitmq",
        "networks": COMMON_NETWORKS,
        "ports": RABBIT_PORTS
    }
    if silent:
        service["logging"] = {"driver": "none"}
    return service


def client_handler_service(broker,
                           logging_level,
                           listen_backlog,
                           input_routing_keys,
                           output_routing_keys):
    env = []
    env.extend([
        f"CLIENT_HANDLER_PORT={CLI_HANDLER_PORT}",
        f"LOGGING_LEVEL={logging_level}",
        f"CLI_WORKER_BROKER={broker}",
        f"CLI_WORKER_EXCHANGE={WORKER_EXCHANGE}",
        f"CLIENT_HANDLER_LISTEN_BACKLOG={listen_backlog}",
        f"CLI_WORKER_EXCHANGE_INPUT_ROUTINGKEYS={input_routing_keys}",
        f"CLI_WORKER_EXCHANGE_OUTPUT_ROUTINGKEYS={output_routing_keys}"
    ])

    return {
        "container_name": "client-handler",
        "build": {
            "context": ".",
            "dockerfile": "client-handler/Dockerfile"
        },
        "image": "client-handler:latest",
        "entrypoint": "python main.py",
        "networks": COMMON_NETWORKS,
        "ports": [f"{CLI_HANDLER_PORT}:{CLI_HANDLER_PORT}"],
        "depends_on": COMMON_DEPENDS_ON,
        "environment": env
    }


def router_service(spec):
    broker = spec.get("broker", DEFAULT_BROKER)

    env = [
        f"CLI_WORKER_BROKER={broker}",
        f"CLI_LOG_LEVEL={spec['log_level']}",
        f"CLI_WORKER_EXCHANGE_NAME={WORKER_EXCHANGE}",
        f"CLI_WORKER_EXCHANGE_INPUT_ROUTINGKEYS={','.join(spec['input_routing_keys'])}"
    ]

    return {
        "build": {
            "context": ".",
            "dockerfile": "router/Dockerfile"
        },
        "container_name": "router",
        "entrypoint": "go run .",
        "image": "router:latest",
        "networks": COMMON_NETWORKS,
        "depends_on": COMMON_DEPENDS_ON,
        "environment": env
    }


def generic_worker_service(name, replica, spec):
    broker = spec.get("broker", DEFAULT_BROKER)
    env = [
        f"CLI_WORKER_BROKER={broker}",
        f"CLI_LOG_LEVEL={spec['log_level']}",
        f"CLI_WORKER_EXCHANGE_NAME={WORKER_EXCHANGE}",
        f"CLI_WORKER_EXCHANGE_INPUT_ROUTINGKEYS={','.join(spec['input_routing_keys'])}",
        f"CLI_WORKER_EXCHANGE_OUTPUT_ROUTINGKEYS={','.join(spec['output_routing_keys'])}"
    ]

    return {
        "build": BUILD_ALIAS,
        "container_name": f"{name}-{replica}",
        "depends_on": DEPENDS_ALIAS,
        "entrypoint": "./run_worker",
        "environment": env,
        "image": f"{name.replace('-', '_')}:latest",
        "networks": NETWORKS_ALIAS
    }


def generate_compose(spec_path, output_path):
    with open(spec_path, "r") as f:
        spec = json.load(f)

    compose = {
        "name": spec.get("name", "tp1"),
        "networks": {
            COMMON_NETWORK: {
                "ipam": {
                    "driver": "default",
                    "config": [{"subnet": "172.25.125.0/24"}]
                }
            }
        },
        "services": {
            "rabbitmq": rabbitmq_service(silent=True)
        }
    }

    for service_spec in spec["services"]:
        name = service_spec["name"]
        replicas = service_spec["replicas"]

        if name == "client-handler":
            broker = service_spec.get("broker", DEFAULT_BROKER)
            logging_level = service_spec.get("log_level", "INFO")
            listen_backlog = service_spec.get("listen_backlog", 5)
            input_routing_keys = service_spec.get("input_routing_keys", [])
            output_routing_keys = service_spec.get("output_routing_keys", [])

            compose["services"][name] = client_handler_service(broker=broker,
                                                               logging_level=logging_level,
                                                               listen_backlog=listen_backlog,
                                                               input_routing_keys=input_routing_keys,
                                                               output_routing_keys=output_routing_keys)
        elif name == "router":
            compose["services"][name] = router_service(service_spec)
        else:
            dockerfile_path = f"filters/{name.replace('-', '_')}/Dockerfile"
            COMMON_BUILD["dockerfile"] = dockerfile_path

            for i in range(1, replicas + 1):
                instance_name = f"{name}-{i}"
                compose["services"][instance_name] = generic_worker_service(
                    name, i, service_spec)

    with open(output_path, "w") as f:
        f.write("version: '3.8'\n")
        f.write(yaml.dump(compose, sort_keys=False))


if __name__ == "__main__":
    args = sys.argv[1:]
    if len(args) > 0 and args[0] in ["-h", "--help"]:
        print("Usage: python generate_compose.py <args>")
        print("Args:")
        print("-s | --spec <path> is the path to the compose spec file")
        print("-o | --output <path> is the path to the output compose file")
        sys.exit(1)

    spec_path = "./scripts/compose-spec.json"
    output_path = "./scripts/docker-compose.yaml"

    for i, arg in enumerate(args):
        if arg.startswith("-s") or arg.startswith("--spec"):
            spec_path = args[i + 1]
        elif arg.startswith("-o") or arg.startswith("--output"):
            output_path = args[i + 1]

    generate_compose(spec_path, output_path)
