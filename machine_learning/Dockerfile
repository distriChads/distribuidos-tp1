# syntax=docker/dockerfile:1.4

FROM python:alpine
RUN pip install --upgrade pip
RUN pip install -r requirements.txt
ENV PYTHONPATH=/app
ENV PYTHONBUFFERED=1
COPY ./server ./app
WORKDIR /app

