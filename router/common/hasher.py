import logging
import signal
import threading
from types import FrameType
from typing import Optional
from .worker import Worker, WorkerConfig

EOF = "EOF"
FIELD_SEPARATOR = "|"
VALUE_SEPARATOR = ","

MOVIES_ID = 0
CREDITS_ID = 1
RATINGS_ID = 2


class HasherConfig(WorkerConfig):
    pass


class Hasher:
    def __init__(self,
                 client_handler_config: HasherConfig,
                 credits_joiner_nodes: int,
                 ratings_joiner_nodes: int
                 ):
        self._running = True
        # Handle SIGINT (Ctrl+C) and SIGTERM (docker stop)
        signal.signal(signal.SIGINT, self.__graceful_shutdown_handler)
        signal.signal(signal.SIGTERM, self.__graceful_shutdown_handler)

        self.client_handler_config = client_handler_config
        self.worker = Worker(client_handler_config)

        self.movies_credits_hash = [[] for _ in range(credits_joiner_nodes)]
        self.movies_ratings_hash = [[] for _ in range(ratings_joiner_nodes)]
        self.credits_hash = [[] for _ in range(credits_joiner_nodes)]
        self.ratings_hash = [[] for _ in range(ratings_joiner_nodes)]

        try:
            self.worker.init_receivers()
            self.worker.init_senders()
        except Exception as e:
            logging.error(f"Error initializing worker: {e}")
            return e

    def __graceful_shutdown_handler(self, signum: Optional[int] = None, frame: Optional[FrameType] = None):
        self._running = False
        self.worker.close_worker()

    def run(self):
        threads = [
            threading.Thread(target=self.__listen_for_movies),
            threading.Thread(target=self.__listen_for_credits),
            threading.Thread(target=self.__listen_for_ratings),
        ]
        for thread in threads:
            thread.start()

        # TODO: use threading.Event() to wait for all threads to finish

    def __listen_generic(self,
                         handler_fn: callable,
                         hashes: list[list[str]],
                         log_prefix: str,
                         type: int,
                         ):
        routing_key = self.worker.input_exchange.routing_keys[type]

        for _, _, result_encoded in self.worker.received_messages_from_routing_key(routing_key):
            result = self.worker.decode_message(result_encoded)
            if result == EOF:
                break

            processed, client_id = handler_fn(result)

            # movies case, two hashes, hash per movies_credits and movies_ratings
            if isinstance(hashes[0], list):
                exchange = 0
                for result_hash in hashes:
                    self.send_movie_result(result_hash, client_id, exchange)
                    exchange += 1
                    for bucket in result_hash:
                        bucket.clear()
            else:  # credits and ratings case, one hash
                self.send_join_result(hashes, client_id, type)
                for bucket in hashes:
                    bucket.clear()

            logging.info(f"{log_prefix} processed and sent: {processed}")

    def __listen_for_movies(self):
        self.__listen_generic(
            self.hashear_y_dividir_movies,
            [self.movies_credits_hash, self.movies_ratings_hash],
            "Movies",
            MOVIES_ID,
        )

    def __listen_for_credits(self):
        self.__listen_generic(lambda received_message: self.hashear_joiner(
            received_message, self.credits_hash),
            self.credits_hash,
            "Credits",
            CREDITS_ID,
        )

    def __listen_for_ratings(self):
        self.__listen_generic(lambda received_message: self.hashear_joiner(
            received_message, self.ratings_hash),
            self.ratings_hash,
            "Ratings",
            RATINGS_ID,
        )

    def hashear_y_dividir_movies(self, result: str) -> tuple[int, str]:
        movies = result.split(FIELD_SEPARATOR)
        client_id = movies[0]  # it is no a movie, it is the client id
        movies_processed = 0
        credits_node_count = len(self.movies_credits_hash)
        ratings_node_count = len(self.movies_ratings_hash)

        for movie in movies[1:]:
            movies_processed += 1
            movie_id = movie.split(VALUE_SEPARATOR)[0]
            hash_id_movie_credits = self.hashear_id(
                movie_id, credits_node_count)
            hash_id_movie_ratings = self.hashear_id(
                movie_id, ratings_node_count)

            self.movies_credits_hash[hash_id_movie_credits].append(movie)
            self.movies_ratings_hash[hash_id_movie_ratings].append(movie)

        self.join_hash_lists(self.movies_credits_hash)
        self.join_hash_lists(self.movies_ratings_hash)

        return movies_processed, client_id

    def join_hash_lists(self, hash_list: list[list[str]]) -> None:
        for i in range(len(hash_list)):
            hash_list[i] = ['|'.join(hash_list[i])]

    def hashear_joiner(self, result: str, join_hash: list[list[str]]) -> tuple[int, str]:
        parts = result.split(FIELD_SEPARATOR)
        client_id = parts[0]
        values_processed = 0
        joiner_node_count = len(join_hash)

        for credit in parts[1:]:
            values_processed += 1
            credit_id = credit.split(VALUE_SEPARATOR)[0]
            hash_id_credit = self.hashear_id(credit_id, joiner_node_count)
            join_hash[hash_id_credit].append(credit)

        for i in range(joiner_node_count):
            join_hash[i] = ['|'.join(join_hash[i])]

        return values_processed, client_id

    def hashear_id(self, id: str, joiner_nodes: int) -> int:
        return int(id) % joiner_nodes

    def send_movie_result(self,
                          result_hash: list[str],
                          client_id: str,
                          exchange_for_movies: int,
                          ):
        exchange = self.worker.output_exchange1 if exchange_for_movies < 1 else self.worker.output_exchange2
        self.send_results(result_hash, client_id, exchange)

    def send_join_result(self,
                         result_hash: list[list[str]],
                         client_id: str,
                         exchange_for_credits_or_ratings: int,
                         ):
        if exchange_for_credits_or_ratings < 1:
            exchange = self.worker.output_exchange1
        else:
            exchange = self.worker.output_exchange2
        self.send_results(result_hash, client_id, exchange)

    def send_results(self,
                     results_hash: list[list[str]],
                     client_id: str,
                     exchange,
                     ):
        for i in range(len(results_hash)):
            result = results_hash[i][0]
            if len(result) > 0:
                result = f"{client_id}{FIELD_SEPARATOR}{result}"
                routing_key = exchange.routing_keys[i]
                self.worker.send_message(result, routing_key, exchange)
