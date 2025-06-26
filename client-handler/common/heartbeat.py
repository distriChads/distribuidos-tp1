import socket
import logging
import time
import threading
import traceback

logger = logging.getLogger(__name__)


class Heartbeat:
    """
    Heartbeat server to monitor health of the service.
    """

    def __init__(self, port: int):
        """
        Initializes the heartbeat server.
        :param port: port to listen on
        """
        self.port = port
        self._shutdown = threading.Event()

        # Create UDP socket
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind(("", port))

    def stop(self):
        """
        Shuts down the heartbeat server by setting the shutdown event and sending a shutdown signal to itself.
        """
        logger.info("HeartBeat server shutting down")
        self._shutdown.set()
        self.sock.sendto(b"\0", ("localhost", self.port))
        self.sock.close()

        logger.info("HeartBeat server socket closed")

    def run(self):
        """
        Main loop for the heartbeat server.
        Listens in the configured udp port.
        Ignores every message except the null-terminated "PING" string and the shutdown signal.
        Sends a null-terminated "PONG" response back to the sender with 3 retries.
        Exits only after Heartbeat.stop is called.
        """
        try:
            logger.info(f"HeartBeat server listening on port {self.port}")

            while not self._shutdown.is_set():
                try:
                    # Receive data from socket
                    data, addr = self.sock.recvfrom(5)
                    if data == b"\0" and addr == ("127.0.0.1", self.port):
                        logger.info("HeartBeat server received shutdown signal")
                        continue
                    if len(data) < 5:
                        logger.warning(f"Short read from {addr}: {data}")
                        continue
                    if data[4] != 0:
                        logger.warning(f"Invalid message from {addr}: {data}")
                        continue

                    # Print received message
                    message = data[:-1].decode("utf-8")
                    logger.debug(f"Received from {addr}: {message}")

                    # Send "OK" response back to the sender
                    response = b"PONG\0"
                    for i in range(3):
                        try:
                            total_sent = 0
                            while total_sent < len(response):
                                sent = self.sock.sendto(response[total_sent:], addr)
                                if sent == 0:
                                    raise RuntimeError("Socket connection broken")
                                total_sent += sent
                            logger.debug(f"Sent response to {addr}")
                            break
                        except Exception as e:
                            logger.error(f"Error sending response (attempt {i+1}): {e}")
                            if i == 2:
                                logger.error("Failed to send response after 3 attempts")
                            time.sleep(2 + i * 1.2)

                except socket.timeout:
                    # No data available, continue
                    continue
                except OSError:
                    logger.info("HeartBeat server socket closed")
                    continue
                except Exception as e:
                    logger.error(
                        f"Error reading from UDP: {e}\nTraceback: {traceback.format_exc()}"
                    )
                    continue

        except Exception as e:
            logger.critical(f"Error listening on UDP port: {e}")
            return
        logger.info("HeartBeat server loop finished")
        return
