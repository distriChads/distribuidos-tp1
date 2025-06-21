import json
import yaml
import sys

# Constants for service names
CLIENT_HANDLER = "client-handler"

FILTER_ARG = "filter-argentina"
FILTER_SPAIN_2000 = "filter-spain-2000"
FILTER_ONE_COUNTRY = "filter-only-one-country"
FILTER_AFTER_2000 = "filter-after-2000"

GROUP_BY_COUNTRY_SUM = "group-by-country-sum"
GROUP_BY_MOVIE_AVG = "group-by-movie-avg"
GROUP_BY_ACTOR_COUNT = "group-by-actor-count"
GROUP_BY_OVERVIEW_AVG = "group-by-overview-avg"

MASTER_GROUP_BY_COUNTRY_SUM = "master-group-by-country-sum"
MASTER_GROUP_BY_MOVIE_AVG = "master-group-by-movie-avg"
MASTER_GROUP_BY_ACTOR_COUNT = "master-group-by-actor-count"
MASTER_GROUP_BY_OVERVIEW_AVG = "master-group-by-overview-avg"

MACHINE_LEARNING = "machine-learning"

TOP_FIVE_COUNTRY_BUDGET = "top-five-country-budget"
TOP_TEN_CAST_MOVIE = "top-ten-cast-movie"
FIRST_AND_LAST = "first-and-last"

JOIN_MOVIES_RATINGS = "join-movie-ratings"
JOIN_MOVIES_CREDITS = "join-movie-credits"


MOVIES_OUTPUT_NODES = [FILTER_ARG, FILTER_ONE_COUNTRY]

# Constants for configuration
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


def get_output_routing_keys_strings(service_name, replicas):
    return ",".join(f"{service_name}.{i}" for i in range(1, replicas + 1))


def client_handler_service(broker,
                           logging_level,
                           listen_backlog,
                           filter_arg_replicas,
                           filter_one_country_replicas,
                           ratings_join_replicas,
                           credits_join_replicas):
    filter_arg_out_routing_keys = get_output_routing_keys_strings(
        FILTER_ARG, filter_arg_replicas)
    filter_one_country_out_routing_keys = get_output_routing_keys_strings(
        FILTER_ONE_COUNTRY, filter_one_country_replicas)
    ratings_join_replicas = get_output_routing_keys_strings(
        JOIN_MOVIES_RATINGS, ratings_join_replicas)
    credits_join_replicas = get_output_routing_keys_strings(
        JOIN_MOVIES_CREDITS, credits_join_replicas)

    env = []
    env.extend([
        f"CLIENT_HANDLER_PORT={CLI_HANDLER_PORT}",
        f"LOGGING_LEVEL={logging_level}",
        f"CLI_WORKER_BROKER={broker}",
        f"CLIENT_HANDLER_LISTEN_BACKLOG={listen_backlog}",
        f"OUTPUT_ROUTINGKEYS_FILTER_ARG={filter_arg_out_routing_keys}",
        f"OUTPUT_ROUTINGKEYS_FILTER_ONE_COUNTRY={filter_one_country_out_routing_keys}",
        f"OUTPUT_ROUTINGKEYS_JOIN_MOVIES_RATING={ratings_join_replicas}",
        f"OUTPUT_ROUTINGKEYS_JOIN_MOVIES_CREDITS={credits_join_replicas}",
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


def generic_worker_service(name, dockerfile_path, replica, spec, entrypoint, output_routing_keys):
    broker = spec.get("broker", DEFAULT_BROKER)

    input_routing_keys = f"{name}.{replica}"

    env = [
        f"CLI_WORKER_BROKER={broker}",
        f"CLI_LOG_LEVEL={spec['log_level']}",
        f"ROUTINGKEYS_INPUT={input_routing_keys}",
    ]

    for key, value in output_routing_keys.items():
        env.append(f"ROUTINGKEYS_OUTPUT_{key.upper()}={value}")

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
        service["volumes"] = [
            f"{spec['storage']}/{name}-{replica}:/app/storage"
        ]

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

    node_replica_mapping = get_replicas(spec["services"])
    print("Node replica mapping:", node_replica_mapping)

    for service_spec in spec["services"]:
        name = service_spec["name"]

        if name == CLIENT_HANDLER:
            broker = service_spec.get("broker", DEFAULT_BROKER)
            logging_level = service_spec.get("log_level", "INFO")
            listen_backlog = service_spec.get(
                "listen_backlog", CLI_HANDLER_BACKLOG)

            compose["services"][name] = client_handler_service(
                broker=broker,
                logging_level=logging_level,
                listen_backlog=listen_backlog,
                filter_arg_replicas=node_replica_mapping[FILTER_ARG.upper().replace(
                    "-", "_")],
                filter_one_country_replicas=node_replica_mapping[FILTER_ONE_COUNTRY.upper(
                ).replace("-", "_")],
                credits_join_replicas=node_replica_mapping[JOIN_MOVIES_CREDITS.upper().replace(
                    "-", "_")],
                ratings_join_replicas=node_replica_mapping[JOIN_MOVIES_RATINGS.upper().replace(
                    "-", "_")],
            )
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
            elif name.startswith(MACHINE_LEARNING):
                prefix = ""
                entrypoint = "python main.py"

            output_routing_keys = {}

            if "output_routing_key" in service_spec:
                output_routing_keys["OUTPUT_ROUTINGKEY"] = service_spec["output_routing_key"][0]
            else:
                output_nodes = service_spec.get("output_nodes", [])
                if not output_nodes:
                    raise ValueError(
                        f"Service {name} must have at least one output node defined.")
                output_routing_keys = get_output_routing_keys(
                    output_nodes, node_replica_mapping)

            dockerfile_path = f"{prefix}{name.replace('-', '_')}/Dockerfile"

            for i in range(1, node_replica_mapping[name.upper().replace("-", "_")] + 1):
                instance_name = f"{name}-{i}"
                compose["services"][instance_name] = generic_worker_service(
                    name, dockerfile_path, i, service_spec, entrypoint, output_routing_keys
                )

    with open(output_path, "w") as f:
        f.write(yaml.dump(compose, sort_keys=False))


def get_replicas(services):
    replicas = {}
    for service in services:
        name = service["name"]
        name = name.upper().replace("-", "_")
        if name not in replicas:
            replicas[name] = service.get("replicas", 1)
        else:
            raise ValueError(f"Duplicate service name found: {name}")

    return replicas


def get_output_routing_keys(output_nodes, node_replica_mapping):
    output_routing_keys = {}

    for node in output_nodes:
        if node not in output_routing_keys:
            output_routing_keys[node] = []

        node_replicas = node_replica_mapping[node.upper().replace("-", "_")]
        for i in range(1, node_replicas + 1):
            output_routing_keys[node].append(f"{node}.{i}")

    return {k: ",".join(v) for k, v in output_routing_keys.items()}


def get_service_replica(spec, replicas_services):
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
