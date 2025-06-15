FROM python:alpine

RUN apt-get update && apt-get install -y iputils-ping docker-cli

COPY monitor.py /monitor.py

CMD ["python3", "/monitor.py"]