import json
import yaml
import sys

DEFAULT_BROKER = "amqp://guest:guest@rabbitmq:5672/"
CLI_HANDLER_PORT = 3000
CLI_HANDLER_BACKLOG = 5

COMMON_NETWORK = "movies_net"
COMMON_NETWORKS = [COMMON_NETWORK]
COMMON_DEPENDS_ON = ["rabbitmq"]

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
                           output_routing_keys,
                           heartbeat_port):
    input_routing_keys = ",".join(input_routing_keys)
    output_routing_keys = ",".join(output_routing_keys)

    env = []
    env.extend([
        f"CLIENT_HANDLER_PORT={CLI_HANDLER_PORT}",
        f"LOGGING_LEVEL={logging_level}",
        f"CLI_WORKER_BROKER={broker}",
        f"CLIENT_HANDLER_LISTEN_BACKLOG={listen_backlog}",
        f"CLI_WORKER_EXCHANGE_INPUT_ROUTINGKEYS={input_routing_keys}",
        f"CLI_WORKER_EXCHANGE_OUTPUT_ROUTINGKEYS={output_routing_keys}",
        f"HEARTBEAT_PORT={heartbeat_port}"
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
        "depends_on": COMMON_DEPENDS_ON[:],
        "environment": env
    }


def router_service(spec, replicas, replicas_services, replica_index):
    env = [
        f"CLI_LOG_LEVEL={spec['log_level']}",
        f"CLI_WORKER_EXCHANGE_INPUT_ROUTINGKEYS={','.join(spec['input_routing_keys'])}",
        f"CLI_WORKER_BROKER={spec.get('broker', DEFAULT_BROKER)}",
        f"CLI_HEARTBEAT_PORT={spec.get('heartbeat_port', 4444)}"
    ]

    for service in replicas_services:
        replica_for_service = replicas[service]
        output_routing_keys = ",".join(
            f"{service}.{i}" for i in range(1, replica_for_service + 1))
        env.append(
            f"CLI_WORKER_OUTPUT_ROUTINGKEYS_{service.upper()}={output_routing_keys}"
        )

    return {
        "build": {
            "context": ".",
            "dockerfile": "router/Dockerfile"
        },
        "container_name": f"router-{replica_index}",
        "entrypoint": "/run_worker",
        "image": "router:latest",
        "networks": COMMON_NETWORKS,
        "depends_on": COMMON_DEPENDS_ON,
        "environment": env
    }

def health_checker_services(spec, services_to_monitor, replicas):
    services = []
    for i in range(replicas):
        _services = []
        for j in range(len(services_to_monitor)):
            if j % replicas == i:
                _services.append(services_to_monitor[j])
        if replicas > 1:
            _services.append(f"health-checker-{((i+1) % replicas) + 1}:{spec.get('heartbeat_port', 4444)}")
        env = [
            f"CLI_LOGGING_LEVEL={spec['log_level']}",
            f"CLI_SERVICES={','.join(_services)}",
            f"CLI_PING_INTERVAL={spec['ping_interval']}",
            f"CLI_HEARTBEAT_PORT={spec.get('heartbeat_port', 4444)}",
            f"CLI_MAX_CONCURRENT_HEALTH_CHECKS={spec.get('max_concurrent_health_checks', 10)}",
            f"CLI_GRACE_PERIOD={spec.get('grace_period', 30)}"
        ]
        services.append({
            "container_name": f"health-checker-{i+1}",
            "build": {
                "context": ".",
                "dockerfile": "health-checker/Dockerfile"
            },
            "image": "health-checker:latest",
            "entrypoint": "python main.py",
            "networks": COMMON_NETWORKS,
            "volumes": [f"/var/run/docker.sock:/var/run/docker.sock", f"./docker-compose.yaml:/app/docker-compose.yaml"],
            "environment": env
        })
    return services

def generic_worker_service(name, dockerfile_path, replica, spec, entrypoint):
    broker = spec.get("broker", DEFAULT_BROKER)
    input_routing_keys = spec['input_routing_keys']

    if name.startswith("join") or name.startswith("group-by"):
        routing_keys = spec['input_routing_keys']
        input_routing_keys = [
            f"{key}.{replica}" for key in routing_keys]

    env = [
        f"CLI_WORKER_BROKER={broker}",
        f"CLI_LOG_LEVEL={spec['log_level']}",
        f"CLI_WORKER_EXCHANGE_INPUT_ROUTINGKEYS={','.join(input_routing_keys)}",
        f"CLI_WORKER_EXCHANGE_OUTPUT_ROUTINGKEYS={','.join(spec['output_routing_keys'])}",
        f"CLI_HEARTBEAT_PORT={spec.get('heartbeat_port', 4444)}"
    ]

    if "storage" in spec:
        env.append("CLI_WORKER_STORAGE=/app/storage")
    if "messages_per_commit" in spec:
        env.append(f"CLI_WORKER_MAXMESSAGES={spec['messages_per_commit']}")

    service = {
        "build": {
            "context": ".",
            "dockerfile": dockerfile_path
        },
        "container_name": f"{name}-{replica}",
        "depends_on": ["rabbitmq"],
        "entrypoint": entrypoint,
        "environment": env,
        "image": f"{name.replace('-', '_')}:latest",
        "networks": ["movies_net"],
    }

    if "storage" in spec:
        service["volumes"] = [f"{spec['storage']}/{name}-{replica}:/app/storage"]

    return service

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
            listen_backlog = service_spec.get(
                "listen_backlog", CLI_HANDLER_BACKLOG)
            input_routing_keys = service_spec.get("input_routing_keys", [])
            output_routing_keys = service_spec.get("output_routing_keys", [])

            compose["services"][name] = client_handler_service(
                broker=broker,
                logging_level=logging_level,
                listen_backlog=listen_backlog,
                input_routing_keys=input_routing_keys,
                output_routing_keys=output_routing_keys,
                heartbeat_port=service_spec.get("heartbeat_port", 4444)
            )

        elif name == "router":
            replicas_services = ["join-movie-credits", "join-movie-ratings",
                                 "group-by-overview-average", "group-by-actor-count",
                                 "group-by-movie-average", "group-by-country-sum"]
            replicas_for_nodes = get_service_replica_for_router(
                spec["services"], replicas_services
            )
            for i in range(1, replicas + 1):
                name_with_index = f"{name}-{i}"
                compose["services"][name_with_index] = router_service(
                    service_spec, replicas_for_nodes, replicas_services, i
                )

        elif name == "health-checker":
            services_to_monitor = [
                f"{service['name']}-{i+1}:{service.get('heartbeat_port', 4444)}" 
                for service in spec["services"] if service["name"] not in ["health-checker", "client-handler"]
                for i in range(service["replicas"])
            ]
            ch_spec = next(s for s in spec["services"] if s["name"] == "client-handler")
            services_to_monitor.append(f"client-handler:{ch_spec.get('heartbeat_port', 4444)}")
            health_checkers = health_checker_services(service_spec, services_to_monitor, replicas)
            for i in range(replicas):
                compose["services"][f"{name}-{i+1}"] = health_checkers[i]

        else:
            prefix = ""
            entrypoint = "./run_worker"

            if name.startswith("filter"):
                prefix = "filters/"
            elif name.startswith("join"):
                prefix = "joins/"
            elif name.startswith("group-by"):
                prefix = "group_by/"
            elif name.startswith("master-group-by"):
                prefix = "master_group_by/"
            elif name.startswith("top") or name.startswith("first"):
                prefix = "topn/"
            elif name.startswith("machine-learning"):
                prefix = ""
                entrypoint = "python main.py"

            dockerfile_path = f"{prefix}{name.replace('-', '_')}/Dockerfile"

            for i in range(1, replicas + 1):
                instance_name = f"{name}-{i}"
                compose["services"][instance_name] = generic_worker_service(
                    name, dockerfile_path, i, service_spec, entrypoint
                )

    with open(output_path, "w") as f:
        f.write(yaml.dump(compose, sort_keys=False))


def get_service_replica_for_router(spec, replicas_services):
    replicas = {}
    for service in replicas_services:
        service_spec = next(
            (s for s in spec if s.get("name") == service), None)
        replicas[service] = service_spec.get("replicas", -1)
        if replicas == -1:
            raise ValueError(
                f"Service {service} must have a defined number of replicas.")

    return replicas


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
