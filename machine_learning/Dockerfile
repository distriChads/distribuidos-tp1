# syntax=docker/dockerfile:1.4

FROM python:3.12.3-slim
COPY ./machine_learning ./app
ENV PYTHONPATH=/app
ENV PYTHONBUFFERED=1
WORKDIR /app
RUN pip install --upgrade pip
RUN pip install -r requirements.txt

ENTRYPOINT [ "python3", "main.py" ]