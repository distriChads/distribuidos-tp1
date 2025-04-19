import pika
import logging

from typing import Callable
import pika.adapters.blocking_connection


class RabbitWorker:
    def __init__(self,
                 rabbitmq_host: str,
                 queues_send: set[str],
                 queue_callbacks: dict[str, Callable[[str], None]]):
        self.rabbitmq_host = rabbitmq_host
        self._sender = Sender(rabbitmq_host, queues_send)
        self._receiver = Receiver(rabbitmq_host, queue_callbacks)

    def start_listening(self):
        try:
            self._receiver.run()
            logging.info(
                f"Worker started, listening on queue: {self._receiver.queues_name}")
        except Exception as e:
            logging.critical(f"Failed worker: {e}")
            self._sender.close()
            self._receiver.close()

    def close(self):
        self._sender.close()
        self._receiver.close()

    def send_message_to(self, queue_name: str, message: str):
        try:
            self._sender.send_to(queue_name, message)
        except Exception as e:
            logging.error(f"Failed to send message: {e}")
            raise


class RabbitClient:
    def __init__(self, rabbitmq_host: str, queues_name: set[str]):
        self.rabbitmq_host = rabbitmq_host
        self.queues_name = queues_name
        self.connection: pika.BlockingConnection | None = None
        self.channel: pika.adapters.blocking_connection.BlockingChannel | None = None

        self.__connect()

    def __connect(self):
        try:
            self.connection = pika.BlockingConnection(
                pika.ConnectionParameters(host=self.rabbitmq_host))
            self.channel = self.connection.channel()
            if not self.channel:
                raise Exception("Failed to create channel for RabbitMQ")
            for queue_name in self.queues_name:
                # TODO: change to durable=True for persistent messages
                self.channel.queue_declare(queue=queue_name)
            logging.info("Connected to RabbitMQ")
        except Exception as e:
            logging.error(f"Failed to connect RabbitClient to RabbitMQ: {e}")
            raise

    def close(self):
        if self.connection:
            self.connection.close()


class Sender(RabbitClient):
    def send_to(self, queue_name: str, message: str):
        if queue_name not in self.queues_name:
            raise Exception(
                f"Queue {queue_name} is not in the sender's queues: {self.queues_name}")
        if not self.connection or not self.channel:
            raise Exception("Sender is not connected to RabbitMQ")
        try:
            self.channel.basic_publish(
                exchange='',
                routing_key=queue_name,
                body=message,
                # TODO: change to durable=True for persistent messages
                # properties=pika.BasicProperties(
                #     delivery_mode=2,  # make message persistent
                # ))
            )
            logging.debug(f"Sent message from Sender to RabbitMQ: {message}")
        except Exception as e:
            logging.error(f"Failed to send message: {e}")
            raise


class Receiver(RabbitClient):
    def __init__(self,
                 rabbitmq_host: str,
                 queue_callbacks: dict[str, Callable[[str], None]]):
        # dict[queue_name] = callback
        self._queue_callbacks: dict[str,
                                    Callable[[str], None]] = queue_callbacks
        queues_name = set(queue_callbacks.keys())
        super().__init__(rabbitmq_host, queues_name)

    def run(self):
        queue_name = self.queues_name.pop()
        if not self.connection or not self.channel:
            raise Exception("Receiver is not connected to RabbitMQ")
        try:
            for queue_name, callback in self._queue_callbacks.items():
                self.channel.basic_consume(
                    queue=queue_name,
                    on_message_callback=callback,
                    auto_ack=True  # TODO: change to False for manual ack
                )
            logging.info(
                f"Waiting for messages in {self.queues_name}. To exit press CTRL+C")
            self.channel.start_consuming()
        except Exception as e:
            logging.error(f"Failed to run Receiver: {e}")
            raise
