import time
import pika
import logging

from typing import Callable
import pika.adapters.blocking_connection
import pika.exceptions


class RabbitWorker:
    def __init__(self,
                 rabbitmq_host: str,
                 queues_send: set[str],
                 queue_callbacks: dict[str, Callable[[str], None]],
                 exchange_name: str
        ):
        self.rabbitmq_host = rabbitmq_host
        self._sender = Sender(rabbitmq_host, queues_send, exchange_name, "server_sender_queue")
        self._receiver = Receiver(rabbitmq_host, queue_callbacks, exchange_name, "server_receiver_queue")
        
    def start_listening(self):
        try:
            self._receiver.run()
            logging.info(
                f"Worker started, listening on queue: {self._receiver.routings}")
        except Exception as e:
            logging.critical(f"Failed worker: {e}")
            self._sender.close()
            self._receiver.close()

    def close(self):
        self._sender.close()
        self._receiver.close()

    def send_message_to(self, exchange_name: str, routing_key: str, message: str):
        try:
            self._sender.send_to(exchange_name, routing_key, message)
        except Exception as e:
            logging.error(f"Failed to send message: {e}")
            raise


class RabbitClient:
    def __init__(self, rabbitmq_host: str, routings: set[str], exchange_name: str, queue_name: str):
        self.rabbitmq_host = rabbitmq_host
        self.routings = routings
        self.exchange_name = exchange_name
        self.queue_name = queue_name
        self.connection: pika.BlockingConnection | None = None
        self.channel: pika.adapters.blocking_connection.BlockingChannel | None = None

        self.__connect()

    def __connect(self):
        for _ in range(5):
            try:
                self.connection = pika.BlockingConnection(
                    pika.ConnectionParameters(host=self.rabbitmq_host))
                self.channel = self.connection.channel()
                if not self.channel:
                    raise Exception("Failed to create channel for RabbitMQ")
                for route in self.routings:
                    # TODO: change to durable=True for persistent messages
                    result = self.channel.queue_declare(
                        queue=self.queue_name,
                        durable=False,
                        auto_delete=True,
                        exclusive=True,
                        arguments=None
                    )
                    q_name = result.method.queue
                    self.channel.exchange_declare(
                        exchange=self.exchange_name,
                        exchange_type="topic",
                        durable=False,
                        auto_delete=True,
                        internal=False,
                        arguments=None
                    )
                    self.channel.queue_bind(
                        queue=q_name, exchange=self.exchange_name, routing_key=f"{route}.input"
                    )
                logging.info("Connected to RabbitMQ")
                return
            except pika.exceptions.AMQPConnectionError as e:
                logging.info("Waiting for RabbitMQ to start...")
                time.sleep(3)
            except Exception as e:
                logging.error(
                    f"Failed to connect RabbitClient to RabbitMQ: {e}")
                raise
        raise Exception("Failed to connect to RabbitMQ after 5 attempts")

    def close(self):
        if self.connection:
            self.connection.close()


class Sender(RabbitClient):
    def send_to(self, exchange_name: str, routing_key: str, message: str):
        if routing_key not in self.routings:
            raise Exception(
                f"Exchange {exchange_name} is not in the sender's exchanges: {self.routings}")
        if not self.connection or not self.channel:
            raise Exception("Sender is not connected to RabbitMQ")
        try:
            self.channel.basic_publish(
                exchange=exchange_name,
                routing_key=f"{routing_key}.input",
                body=message,
                # TODO: change to durable=True for persistent messages
                # properties=pika.BasicProperties(
                #     delivery_mode=2,  # make message persistent
                # )
            )
            logging.debug(f"Sent message from Sender to RabbitMQ: {message}")
        except Exception as e:
            logging.error(f"Failed to send message: {e}")
            raise


class Receiver(RabbitClient):
    def __init__(self,
                 rabbitmq_host: str,
                 queue_callbacks: dict[str, Callable[[str], None]],
                 exchange_name: str,
                 queue_name: str):
        # dict[queue_name] = callback
        self._queue_callbacks: dict[str,
                                    Callable[[str], None]] = queue_callbacks
        queues_name = set(queue_callbacks.keys())
        super().__init__(rabbitmq_host, queues_name, exchange_name, queue_name)

    def run(self):
        queue_name = self.routings.pop()
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
                f"Waiting for messages in {self.routings}. To exit press CTRL+C")
            self.channel.start_consuming()
        except Exception as e:
            logging.error(f"Failed to run Receiver: {e}")
            raise
