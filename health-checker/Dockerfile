# syntax=docker/dockerfile:1.4

FROM python:alpine
RUN apk add --no-cache docker-cli docker-compose
RUN pip install --upgrade pip
ENV PYTHONPATH=/app
ENV PYTHONBUFFERED=1
COPY ./health-checker ./app
WORKDIR /app
RUN pip install -r requirements.txt
