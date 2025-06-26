.PHONY: generate_compose build_docker run_docker stop_docker verify_results chancho_loco

generate_compose:
	clear ; python3 scripts/generate_compose.py -o ./docker-compose.yaml

build_docker:
	clear ; docker compose build

run_docker:
	clear ; docker compose down ; docker compose up -d

stop_docker:
	clear; docker compose down

verify_results:
	clear; py scripts/compare_results.py

chancho_loco:
	clear; python3 scripts/chancho_loco.py
