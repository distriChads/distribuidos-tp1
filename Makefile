.PHONY: generate_compose build_docker run_docker stop_docker chancho_loco verify

generate_compose:
	clear ; python3 scripts/generate_compose.py -o ./docker-compose.yaml

build_docker:
	clear ; docker compose build

run_docker:
	clear ; docker compose down ; docker compose up -d

stop_docker:
	clear; docker compose down

chancho_loco:
	clear; python3 scripts/chancho_loco.py

verify:
	clear; python3 scripts/compare_results.py
