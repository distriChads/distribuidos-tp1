import pika
import time
import logging
from contextlib import contextmanager

MESSAGE_SEPARATOR = "|"
MESSAGE_ARRAY_SEPARATOR = ","
MESSAGE_EOF = "EOF"

log = logging.getLogger("worker")


class ExchangeSpec:
    def __init__(self, name, routing_keys, queue_name):
        self.name = name
        self.routing_keys = routing_keys
        self.queue_name = queue_name


class WorkerConfig:
    def __init__(self, input_exchange, output_exchange, message_broker):
        self.input_exchange = input_exchange
        self.output_exchange = output_exchange
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
        self.input_exchange = config.input_exchange
        self.output_exchange = config.output_exchange
        self.message_broker = config.message_broker
        log.debug(f"Worker config: {config.__dict__}")
        self.sender = None
        self.receiver = None

    def _init_connection(self):
        max_retries = 3
        retry_sleep = 10
        backoff_factor = 2

        for i in range(max_retries):
            try:
                conn = pika.BlockingConnection(
                    pika.URLParameters(self.message_broker))
                return conn
            except Exception as e:
                log.warning(
                    f"Failed to connect to broker on attempt {i+1}: {e}")
                if i < max_retries - 1:
                    time.sleep(i * backoff_factor + retry_sleep)
        log.error("Failed to connect to broker")
        raise ConnectionError("Failed to connect to broker")

    def init_sender(self):
        conn = self._init_connection()
        ch = conn.channel()

        ch.exchange_declare(
            exchange=self.output_exchange.name,
            exchange_type='topic',
            durable=False,
            auto_delete=False
        )

        self.sender = Sender(conn, ch)
        log.info("Sender initialized")

    def init_receiver(self):
        self.receiver = self._init_generic_receiver(self.input_exchange)
        log.info("Receiver initialized")

    def _init_generic_receiver(self, exchange_spec):
        conn = self._init_connection()
        ch = conn.channel()

        ch.exchange_declare(
            exchange=exchange_spec.name,
            exchange_type='topic',
            durable=False,
            auto_delete=False
        )

        result = ch.queue_declare(
            queue=exchange_spec.queue_name, exclusive=True, auto_delete=False)
        queue_name = result.method.queue

        for routing_key in exchange_spec.routing_keys:
            ch.queue_bind(
                exchange=exchange_spec.name,
                queue=queue_name,
                routing_key=routing_key
            )

        messages = ch.consume(queue=queue_name, auto_ack=True)
        return Receiver(conn, ch, queue_name, messages)

    def send_message(self, message, routing_key):
        if not self.sender:
            raise Exception("Sender not initialized")

        self.sender.ch.basic_publish(
            exchange=self.output_exchange.name,
            routing_key=routing_key,
            body=message,
            properties=pika.BasicProperties(content_type="text/plain")
        )
        log.debug(f"Sent message to exchange {self.output_exchange.name} "
                  f"(routing key: {routing_key}): {message}")

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
