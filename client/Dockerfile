# syntax=docker/dockerfile:1.4

FROM python:alpine
RUN pip install --upgrade pip
ENV PYTHONPATH=/app
ENV PYTHONBUFFERED=1
COPY ./client /app
WORKDIR /app
RUN pip install -r requirements.txt

ENTRYPOINT ["python", "main.py"]
