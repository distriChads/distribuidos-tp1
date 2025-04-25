import json
import yaml
import sys

DEFAULT_BROKER = "amqp://guest:guest@rabbitmq:5672/"

def rabbitmq_service(silent):
  service = {
    "image": "rabbitmq:latest",
    "container_name": "rabbitmq",
    "networks": ["movies_net"],
    "ports": ["5672:5672", "15672:15672"]
  }
  if silent:
    service["logging"] = {
      "driver": "none"
    }    
  return service

def server_service(spec):
  assert spec["replicas"] == 1
  
  env = []
  broker = spec.get("broker", DEFAULT_BROKER)
  env.append(f"CLI_WORKER_BROKER={broker}")
  
  service = {
    "build": {
      "context": ".",
      "dockerfile": "server/Dockerfile"
    },
    "image": "server:latest",
    "networks": ["movies_net"],
    "ports": ["3000:3000"],
    "depends_on": ["rabbitmq"]
  }

def generate_compose(spec_path, output_path):
  with open(spec_path, "r") as f:
    compose_spec = json.load(f)
      
  compose = {}
  compose["name"] = "tp1"
  compose["networks"] = {
    "movies_net": {
      "ipam": {
        "driver": "default",
        "config": [
          {
            "subnet": "127.0.0.0/24"
          }
        ]
      }
    }
  }
  
  services = [rabbitmq_service(silent=True)]
    
  for service in compose_spec["services"]:
    compose["services"][service["name"]] = {
      "build": {
        "context": ".",
        "dockerfile": service["dockerfile"]
      },
      "image": service["image"],
      "networks": ["movies_net"],
      "depends_on": service["depends_on"],
      "volumes": service["volumes"],
      "environment": service["environment"]
    }

  with open("docker-compose.yaml", "w") as f:
    yaml.dump(docker_compose_template, f)


if __name__ == "__main__":
  args = sys.argv[1:]
  if len(args) != 1:
    print("Usage: python generate_compose.py <args>")
    print("Args:")
    print("-s | --spec <path> is the path to the compose spec file")
    print("-o | --output <path> is the path to the output compose file")
    sys.exit(1)
    
  spec_path = "./compose-spec.json"
  output_path = "./docker-compose.yaml"
    
  for i, arg in enumerate(args):
    if arg.startswith("-s") or arg.startswith("--spec"):
      spec_path = args[i + 1]
    elif arg.startswith("-o") or arg.startswith("--output"):
      output_path = args[i + 1]

  generate_compose(spec_path, output_path)
