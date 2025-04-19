.PHONY: rabbitmq build-filters run-filters server

build-filters:
	docker build -t filter-after-2000 -f filters/filter_after_2000/Dockerfile .
	docker build -t filter-argentina -f filters/filter_argentina/Dockerfile .
	docker build -t filter-spain-2000 -f filters/filter_spain_2000/Dockerfile .
	docker build -t filter-only-one-country -f filters/filter_only_one_country/Dockerfile .

run-filters:
	docker run -d --name filter-after-2000 --network host filter-after-2000
	docker run -d --name filter-argentina --network host filter-argentina
	docker run -d --name filter-spain-2000 --network host filter-spain-2000
	docker run -d --name filter-only-one-country --network host filter-only-one-country

stop-filters:
	docker stop filter-after-2000 filter-argentina filter-spain-2000 filter-only-one-country
	docker rm filter-after-2000 filter-argentina filter-spain-2000 filter-only-one-country

rabbitmq:
	docker run -it --rm --name rabbitmq -p 5672:5672 -p 15672:15672 rabbitmq:4.0-management

server:
	clear; PYTHONPATH=. python3 ./server/main.py