# syntax=docker/dockerfile:1.4

FROM python:3.12.10-slim

# Install system dependencies that some libs need
RUN apt-get update && apt-get install -y \
    build-essential \
    libffi-dev \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

# install dependencies before copying the code to avoid reinstalling
COPY ./machine_learning/requirements.txt ./app/requirements.txt
WORKDIR /app

ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1

RUN pip install --upgrade pip
# RUN pip install --no-cache-dir torch==2.2.2+cpu \
#     -f https://download.pytorch.org/whl/cpu/torch_stable.html && \
#     pip install --no-cache-dir -r requirements.txt

# comment this line to run with torch, and uncomment the one above and transformers in requirements.txt
RUN pip install --no-cache-dir -r requirements.txt 

RUN python -m textblob.download_corpora

# copy the code
COPY ./machine_learning ./

ENTRYPOINT ["python3", "main.py"]
