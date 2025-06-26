import pika
import time
import logging
import uuid
import threading
from typing import Generator

MESSAGE_SEPARATOR = "|"
MESSAGE_ARRAY_SEPARATOR = ","
MESSAGE_EOF = "EOF"

EXCHANGE_NAME = "data_exchange"
EXCHANGE_TYPE = "topic"

log = logging.getLogger("worker")


class ExchangeSpec:
    """
    Data class for a Message Broker exchange specification.
    """

    def __init__(self, input_routing_keys, output_routing_keys, queue_name):
        self.name = EXCHANGE_NAME
        self.input_routing_keys = input_routing_keys
        self.output_routing_keys = output_routing_keys


class WorkerConfig:
    """
    Data class for worker configuration.
    """

    def __init__(self, exchange, message_broker):
        self.exchange = exchange
        self.message_broker = message_broker


class Sender:
    """
    Wrapper for a Message Broker connection meant for sending messages.
    """

    def __init__(self, conn, ch):
        self.conn = conn
        self.ch = ch


class Receiver:
    """
    Wrapper for a Message Broker connection meant for receiving messages.
    """

    def __init__(self, conn, ch, queue, messages):
        self.conn = conn
        self.ch = ch
        self.queue = queue
        self.messages = messages


class Worker:
    """
    Worker class to handle the communication with the message broker.
    Handles the sending and receiving of messages.
    """

    def __init__(self, config: WorkerConfig):
        """
        Initializes the worker.
        Only loads configuration, does not connect to the message broker.
        init_senders and init_receiver must be called before attempting to send or receive messages.
        :param config: WorkerConfig object containing the exchange and message broker configuration
        """
        self.exchange = config.exchange
        self.message_broker = config.message_broker
        self.sender = None
        self.receiver = None

    def _init_connection(self) -> pika.BlockingConnection:
        """
        Initializes a connection to the message broker, with retries.
        :return: pika.BlockingConnection object
        """
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

    def init_senders(self) -> None:
        """
        Initializes the message broker sender.
        :return: None
        """
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

    def init_receiver(self) -> None:
        """
        Initializes the message broker receiver.
        :return: None
        """
        conn = self._init_connection()
        ch = conn.channel()

        ch.exchange_declare(
            exchange=self.exchange.name,
            exchange_type=EXCHANGE_TYPE,
            durable=True,
            auto_delete=False,
        )

        result = ch.queue_declare(
            queue=self.exchange.input_routing_keys[0],
            exclusive=False,
            auto_delete=False,
        )
        queue_name = result.method.queue

        for routing_key in self.exchange.input_routing_keys:
            ch.queue_bind(
                exchange=self.exchange.name, queue=queue_name, routing_key=routing_key
            )

        messages = ch.consume(queue=queue_name, auto_ack=False)
        self.receiver = Receiver(conn, ch, queue_name, messages)
        log.info("Receiver initialized")

    def send_message(self, message: str, routing_key: str, client_id: str) -> None:
        """
        Sends a message to the message broker.
        :param message: message to send
        :param routing_key: routing key to send the message to
        :param client_id: client id to identify the message
        :return: None
        """
        if not self.sender:
            raise Exception("Sender not initialized")

        identifier = str(uuid.uuid4())
        message = f"{client_id}|{identifier}|{message}"
        self.sender.ch.basic_publish(
            exchange=self.exchange.name,
            routing_key=routing_key,
            body=message,
            properties=pika.BasicProperties(content_type="text/plain"),
        )

        log.debug(f"Sent message to routing_key {routing_key}: " f"{message}")

    def send_ack(self, delivery_tag: int) -> None:
        """
        Acknowledges a processed message to the message broker.
        Raises an exception if the receiver is not initialized.
        :param delivery_tag: delivery tag of the message to acknowledge
        :return: None
        """
        if not self.receiver:
            raise Exception("Receiver not initialized")
        self.receiver.ch.basic_ack(delivery_tag=delivery_tag, multiple=False)

    def received_messages(
        self, shutdown_event: threading.Event
    ) -> Generator[
        tuple[pika.spec.Basic.Deliver, pika.spec.BasicProperties, str], any, str
    ]:
        """
        Generator for received messages from the message broker.
        Finishes when the shutdown event is set or when the worker is shut down calling close_worker.
        Raises an exception if the receiver is not initialized.
        :param shutdown_event: event to signal the shutdown of the worker
        :return: generator for received messages
        """
        if not self.receiver:
            raise Exception("Receiver not initialized")
        while not shutdown_event.is_set():
            try:
                for method_frame, _properties, result_encoded in self.receiver.messages:
                    result = result_encoded.decode("utf-8")
                    yield method_frame, _properties, result
            except pika.exceptions.StreamLostError:
                log.info("Receiving queue closed")
                continue

    def close_worker(self) -> None:
        """
        Closes sender and receiver connections to the message broker.
        """
        self._close_receiver()
        self._close_sender()

    def _close_sender(self) -> None:
        """
        Closes the sender connection to the message broker.
        """
        if self.sender:
            self.sender.conn.close()
            self.sender = None
            log.info("Sender closed")

    def _close_receiver(self) -> None:
        """
        Closes the receiver connection to the message broker.
        """
        if self.receiver:
            self.receiver.ch.cancel()
            self.receiver.ch.close()
            self.receiver.conn.close()
            self.receiver = None
            log.info("Receiver closed")
