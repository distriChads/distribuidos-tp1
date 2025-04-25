import json
import yaml
import sys

DEFAULT_BROKER = "amqp://guest:guest@rabbitmq:5672/"

def rabbitmq_service(silent):
  service = {
    "image": "rabbitmq:management",
    "container_name": "rabbitmq",
    "networks": ["movies_net"],
    "ports": ["5672:5672", "15672:15672"]
  }
  if silent:
    service["logging"] = {
      "driver": "none"
    }    
  return service

def server_service(spec, movies_input_replicas):
  assert spec["replicas"] == 1
  
  env = []
  broker = spec.get("broker", DEFAULT_BROKER)
  env.append(f"SERVER_PORT=3000")
  env.append(f"SERVER_LISTEN_BACKLOG=1")
  env.append(f"CLI_WORKER_BROKER={broker}")
  env.append(f"CLI_WORKER_EXCHANGE1_OUTPUT_NAME={spec['movies_exchange_name']}")
  env.append(f"CLI_WORKER_EXCHANGE2_OUTPUT_NAME={spec['credits_exchange_name']}")
  env.append(f"CLI_WORKER_EXCHANGE3_OUTPUT_NAME={spec['ratings_exchange_name']}")
  
  for i in range(movies_input_replicas):
    env.append(f"CLI_WORKER_EXCHANGE1_OUTPUT_ROUTINGKEYS=movies.input{i+1}")
  env.append(f"CLI_WORKER_EXCHANGE2_OUTPUT_ROUTINGKEYS=credits.input")
  env.append(f"CLI_WORKER_EXCHANGE3_OUTPUT_ROUTINGKEYS=ratings.input")
    
  env.append(f"CLI_WORKER_EXCHANGE_INPUT_NAME={spec['results_exchange_name']}")
  input_keys = ",".join(spec["results_exchange_routing_keys"])
  env.append(f"CLI_WORKER_EXCHANGE_INPUT_ROUTINGKEYS={input_keys}")
  
  service = {
    "container_name": "server",
    "build": {
      "context": ".",
      "dockerfile": "server/Dockerfile"
    },
    "image": "server:latest",
    "entrypoint": "python main.py",
    "networks": ["movies_net"],
    "ports": ["3000:3000"],
    "depends_on": ["rabbitmq"],
    "environment": env
  }
  return service

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
            "subnet": "172.25.125.0/24"
          }
        ]
      }
    }
  }  
  services = {
    "rabbitmq": rabbitmq_service(silent=True)
  }
  
  specs = {service["name"]: service for service in compose_spec["services"]}
  
  # first pass to get output keys
  for service in compose_spec["services"]:
    if service["name"] == "server":
      continue
    
    if "output" in service:
      output_keys = service["output"]["routing_key"]
      if "%d" in output_keys:
        output_keys = [output_keys % (i + 1) for i in range(service["replicas"])]
      else:
        output_keys = [output_keys]
      service["output_keys"] = output_keys
  
  # second pass to generate compose
  for service in compose_spec["services"]:
    if service["name"] == "server":
      movies_input_replicas = specs["filter-argentina"]["replicas"]
      services["server"] = server_service(specs["server"], movies_input_replicas)
      continue
    
    srv = {}
    prefix = ""
    if service["name"].startswith("filter"):
      prefix = "filters/"
    elif service["name"].startswith("join"):
      prefix = "joins/"
    elif service["name"].startswith("group-by"):
      prefix = "group_by/"
    elif service["name"].startswith("master-group-by"):
      prefix = "master_group_by/"
    elif service["name"].startswith("top") or service["name"].startswith("first"):
      prefix = "topn/"
    elif service["name"].startswith("machine-learning"):
      prefix = ""
      
    dockerfile = f"{prefix}{service['name'].replace('-', '_')}/Dockerfile"
    srv["build"] = {
      "context": ".",
      "dockerfile": dockerfile
    }
    srv["image"] = f"{service['name'].replace('-', '_')}:latest"
    srv["networks"] = ["movies_net"]
    srv["depends_on"] = ["rabbitmq"]
    srv["entrypoint"] = "python main.py" if service["name"] == "machine-learning" else "./run_worker"
    
    env = []
    env.append(f"CLI_WORKER_BROKER={DEFAULT_BROKER}")
    env.append(f"CLI_LOG_LEVEL={service['log_level']}")
    env.append(f"CLI_WORKER_EXCHANGE_OUTPUT_NAME={service['output']['exchange_name']}")
    
    srv["environment"] = env
    
    for i in range(service["replicas"]):
      ith_service = srv.copy()
      ith_service["container_name"] = f"{service['name']}-{i+1}"
      env = ith_service["environment"]
      
      env = list(ith_service["environment"]) # Make a copy for this replica

      # --- Input Handling ---
      input_spec = service['input']
      input_from = input_spec['from']
      input_type = input_spec['type']
      input_keys_list = []
      input_exchange = ""
      expected_eof = 1

      # Use specs which is the compose_spec after the first pass modifications
      # It should contain the calculated 'output_keys' for each service.
      # The server service logic (outside this loop) should handle calculating
      # its specific output keys based on consumers. We assume 'specs' contains this info.

      if input_from in ["movies", "credits", "ratings"]:
          # Input comes from the central server
          server_spec = specs['server']
          
          if input_from == "movies":
            server_keys_for_consumer = [f"{input_from}.input{k+1}" for k in range(service['replicas'])] # Placeholder/Fallback
          else:
            server_keys_for_consumer = [f"{input_from}.input"] # Placeholder/Fallback

          if input_from == "movies":
              input_exchange = server_spec['movies_exchange_name']
          elif input_from == "credits":
              input_exchange = server_spec['credits_exchange_name']
          elif input_from == "ratings":
              input_exchange = server_spec['ratings_exchange_name']

          if input_type == 'seq':
              # This replica gets the i-th key generated by the server for this service group
              if i < len(server_keys_for_consumer):
                   input_keys_list = [server_keys_for_consumer[i]]
              else:
                   # Avoid index error if server generates fewer keys than replicas
                   print(f"Warning: Server key mismatch for {service['name']}-{i+1}. Need key index {i}, only {len(server_keys_for_consumer)} available.")
                   input_keys_list = []
          else: # 'all'
              # This replica listens to all keys generated by the server for this service group
              input_keys_list = server_keys_for_consumer
          expected_eof = 1 # Server sends 1 logical EOF per stream it manages

      else:
          # Input comes from another service
          if input_from not in specs:
              raise ValueError(f"Input source service '{input_from}' not found in specs for service '{service['name']}'")
          input_service_spec = specs[input_from]
          input_exchange = input_service_spec['output']['exchange_name']
          # 'output_keys' should have been calculated in the first pass
          all_input_source_keys = input_service_spec.get('output_keys', [])

          if not all_input_source_keys:
               print(f"Warning: Input source service '{input_from}' has no output keys defined for service '{service['name']}'.")

          if input_type == 'seq':
              # This replica gets the i-th output key from the source service
              if i < len(all_input_source_keys):
                  input_keys_list = [all_input_source_keys[i]]
              else:
                  print(f"Warning: Input key mismatch for {service['name']}-{i+1}. Need key index {i} from {input_from}, only {len(all_input_source_keys)} available.")
                  input_keys_list = []

              # Determine EOF based on the source's output behavior and replicas
              if input_service_spec['output']['type'] == 'all':
                   # Source fans out from all its replicas via one exchange, this replica gets one key
                   expected_eof = input_service_spec.get('replicas', 1)
              else: # 'seq', 'unique' -> source sends specific key from one replica (or corresponding replica)
                   expected_eof = 1
          else: # 'all'
              # This replica listens to all output keys from the source service
              input_keys_list = all_input_source_keys
              # It will receive messages potentially from all replicas of the source service
              expected_eof = input_service_spec.get('replicas', 1)

      if input_keys_list: # Only add if keys were assigned
          env.append(f"CLI_WORKER_EXCHANGE_INPUT_NAME={input_exchange}")
          env.append(f"CLI_WORKER_EXCHANGE_INPUT_ROUTINGKEYS={','.join(input_keys_list)}")
          # Use the first assigned input key as the base for the queue name
          # Ensure queue names are unique per consumer replica even if they listen to the same key in 'all' mode?
          # For now, assume queue name matches the first key it binds.
          queue_name = ith_service['container_name']
          # Let's make queue name unique per replica instance when input type is 'all'
          # if input_type == 'all' and service['replicas'] > 1:
          #    queue_name = f"{service['name']}.{i+1}.{input_keys_list[0]}" # Append replica index
          # else:
          #    queue_name = input_keys_list[0] # Use key directly for seq or single replica
          # Sticking to simpler logic for now: queue name = first key
          env.append(f"CLI_WORKER_QUEUE_NAME={queue_name}")
          env.append(f"CLI_WORKER_EXPECTEDEOF={expected_eof}")

      # --- Output Handling ---
      output_spec = service['output']
      output_type = output_spec['type']
      # 'output_keys' for *this* service were calculated in the first pass
      all_output_keys = service.get('output_keys', [])
      output_keys_list = []

      if not all_output_keys:
          print(f"Warning: Service '{service['name']}' has no output keys defined.")

      if output_type == 'seq':
          # This replica is responsible for sending to the i-th output key
          if i < len(all_output_keys):
              output_keys_list = [all_output_keys[i]]
          else:
              print(f"Warning: Output key mismatch for {service['name']}-{i+1}. Need key index {i}, only {len(all_output_keys)} available.")
              output_keys_list = []
      else: # 'all', 'unique'
          # This replica sends to all output keys defined for the service
          output_keys_list = all_output_keys

      if output_keys_list: # Only add if keys were assigned
          # Output exchange name is already added before the loop
          env.append(f"CLI_WORKER_EXCHANGE_OUTPUT_ROUTINGKEYS={','.join(output_keys_list)}")


      # --- Second Input Handling (if exists) ---
      if 'second_input' in service:
          sec_input_spec = service['second_input']
          sec_input_from = sec_input_spec['from']
          sec_input_type = sec_input_spec['type']
          sec_input_keys_list = []
          sec_input_exchange = ""
          sec_expected_eof = 1

          if sec_input_from in ["movies", "credits", "ratings"]:
              server_spec = specs['server']
              if sec_input_from == "movies":
                sec_server_keys_for_consumer = [f"{sec_input_from}.input{k+1}" for k in range(service['replicas'])]
              else:
                sec_server_keys_for_consumer = [f"{sec_input_from}.input"]

              if sec_input_from == "movies": sec_input_exchange = server_spec['movies_exchange_name']
              elif sec_input_from == "credits": sec_input_exchange = server_spec['credits_exchange_name']
              elif sec_input_from == "ratings": sec_input_exchange = server_spec['ratings_exchange_name']

              if sec_input_type == 'seq':
                  if i < len(sec_server_keys_for_consumer): sec_input_keys_list = [sec_server_keys_for_consumer[i]]
                  else: sec_input_keys_list = []
              else: # 'all'
                  sec_input_keys_list = sec_server_keys_for_consumer
              sec_expected_eof = 1
          else:
              # Input from another service
              if sec_input_from not in specs:
                   raise ValueError(f"Second input source service '{sec_input_from}' not found in specs for service '{service['name']}'")
              sec_input_service_spec = specs[sec_input_from]
              sec_input_exchange = sec_input_service_spec['output']['exchange_name']
              sec_all_input_source_keys = sec_input_service_spec.get('output_keys', [])

              if not sec_all_input_source_keys:
                   print(f"Warning: Second input source service '{sec_input_from}' has no output keys defined for service '{service['name']}'.")

              if sec_input_type == 'seq':
                  if i < len(sec_all_input_source_keys): sec_input_keys_list = [sec_all_input_source_keys[i]]
                  else: sec_input_keys_list = []

                  if sec_input_service_spec['output']['type'] == 'all':
                       sec_expected_eof = sec_input_service_spec.get('replicas', 1)
                  else: # seq, unique
                       sec_expected_eof = 1
              else: # 'all'
                  sec_input_keys_list = sec_all_input_source_keys
                  sec_expected_eof = sec_input_service_spec.get('replicas', 1)

          if sec_input_keys_list:
              env.append(f"CLI_WORKER_EXCHANGE_SECONDINPUT_NAME={sec_input_exchange}")
              env.append(f"CLI_WORKER_EXCHANGE_SECONDINPUT_ROUTINGKEYS={','.join(sec_input_keys_list)}")
              env.append(f"CLI_WORKER_SECONDQUEUE_NAME={ith_service['container_name']}")
              env.append(f"CLI_WORKER_EXPECTEDEOF2={sec_expected_eof}")

      # --- Machine Learning ---
      if service['name'] == "machine-learning":
          ith_service["cpus"] = "2"


      ith_service["environment"] = env
      
      services[service['name']] = ith_service
    
  compose["services"] = services
  
  with open(output_path, "w") as f:
    yaml.dump(compose, f)


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
