import pika
import time
import logging
from contextlib import contextmanager
import uuid

MESSAGE_SEPARATOR = "|"
MESSAGE_ARRAY_SEPARATOR = ","
MESSAGE_EOF = "EOF"

EXCHANGE_NAME = "data_exchange"
EXCHANGE_TYPE = "topic"

log = logging.getLogger("worker")


class ExchangeSpec:
    def __init__(self, input_routing_key, output_routing_keys):
        self.name = EXCHANGE_NAME
        self.input_routing_key = input_routing_key
        self.output_routing_keys = output_routing_keys


class WorkerConfig:
    def __init__(self, exchange, message_broker):
        self.exchange = exchange
        self.message_broker = message_broker


class Sender:
    def __init__(self, conn, ch):
        self.conn = conn
        self.ch = ch


class Receiver:
    def __init__(self, conn, ch, queue, messages):
        self.conn = conn
        self.ch = ch
        self.queue = queue
        self.messages = messages


class Worker:
    def __init__(self, config: WorkerConfig):
        self.exchange = config.exchange
        self.message_broker = config.message_broker
        self.sender = None
        self.receiver = None

    def _init_connection(self):
        max_retries = 3
        retry_sleep = 10
        backoff_factor = 2

        for i in range(max_retries):
            try:
                conn = pika.BlockingConnection(pika.URLParameters(self.message_broker))
                return conn
            except Exception as e:
                log.warning(f"Failed to connect to broker on attempt {i+1}: {e}")
                if i < max_retries - 1:
                    time.sleep(i * backoff_factor + retry_sleep)

        log.error("Failed to connect to broker")
        raise ConnectionError("Failed to connect to broker")

    def init_senders(self):
        conn = self._init_connection()
        ch = conn.channel()

        ch.exchange_declare(
            exchange=self.exchange.name,
            exchange_type=EXCHANGE_TYPE,
            durable=True,
            auto_delete=False,
        )

        self.sender = Sender(conn, ch)
        log.info("Sender initialized")

    def init_receiver(self):
        conn = self._init_connection()
        ch = conn.channel()

        ch.exchange_declare(
            exchange=self.exchange.name,
            exchange_type=EXCHANGE_TYPE,
            durable=True,
            auto_delete=False,
        )

        result = ch.queue_declare(
            queue=self.exchange.input_routing_key, exclusive=False, auto_delete=False
        )
        queue_name = result.method.queue

        ch.queue_bind(
            exchange=self.exchange.name,
            queue=self.exchange.input_routing_key,
            routing_key=self.exchange.input_routing_key,
        )

        messages = ch.consume(queue=queue_name, auto_ack=False)
        self.receiver = Receiver(conn, ch, queue_name, messages)
        log.info("Receiver initialized")

    def send_ack(self, delivery_tag):
        self.receiver.ch.basic_ack(delivery_tag=delivery_tag, multiple=False)

    def send_message(self, message: str, routing_key: str):
        if not self.sender:
            raise Exception("Sender not initialized")

        self.sender.ch.basic_publish(
            exchange=self.exchange.name,
            routing_key=routing_key,
            body=message,
            properties=pika.BasicProperties(content_type="text/plain"),
        )

        log.debug(f"Sent message to routing_key {routing_key}: " f"{message}")

    def received_messages(self):
        if not self.receiver:
            raise Exception("Receiver not initialized")
        return self.receiver.messages

    def close_worker(self):
        self._close_sender()
        self._close_receiver()

    def _close_sender(self):
        if self.sender:
            self.sender.ch.close()
            self.sender.conn.close()
            self.sender = None

    def _close_receiver(self):
        if self.receiver:
            self.receiver.ch.close()
            self.receiver.conn.close()
            self.receiver = None
