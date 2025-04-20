# syntax=docker/dockerfile:1.4

FROM python:3.12-slim

WORKDIR /app/server

RUN pip install --upgrade pip

ENV PYTHONPATH=/app

ENV PYTHONBUFFERED=1

COPY ./server .

RUN pip install -r requirements.txt

CMD ["python", "main.py"]
