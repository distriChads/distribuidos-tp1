import codecs
import socket
import asyncio
import logging

logger = logging.getLogger(__name__)

class Socket:
    """
    Wrapper class for socket communication.
    Handles safely sending and receiving asynchronously.
    """

    def __init__(self):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.port = self.sock.getsockname()[1]
        self.decoder = codecs.getincrementaldecoder('utf-8')()

    def close(self):
        """
        Close the socket
        """
        self.sock.close()

    async def send_to(self, msg: str, addr: str, port: int):
        """
        Write a message to a socket

        :param socket: socket to write to
        :param msg: message to write
        """
        # Encode the message to bytes
        msg_encoded = msg.encode('utf-8')
        data = msg_encoded + b"\0"
        total_sent = 0
        while total_sent < len(data):
            sent = await asyncio.to_thread(self.sock.sendto, data[total_sent:], (addr, port))
            if sent == 0:
                raise RuntimeError("Socket connection broken")
            total_sent += sent
        logger.debug(f"Sent message to {addr}:{port} - {msg}")

    async def read(self) -> str:
        """
        Read a 5 byte message from a socket, ensuring that all bytes are received.

        :param sock: socket to read from
        :return: message read
        """
        bytes, addr = await asyncio.to_thread(self.sock.recvfrom, 5)
        logger.debug(f"Read from {addr}: {bytes}")
        if len(bytes) < 5:
            logger.warning(f"Short read from {addr}: {bytes}")
            raise IOError(f"Short read from {addr}: {bytes}")
        if bytes[4] != 0:
            logger.warning(f"Invalid message from {addr}: {bytes}")
            raise ValueError(f"Invalid message from {addr}: {bytes}")
        msg = self.decoder.decode(bytes[:-1])
        logger.debug(f"Read message: {msg}")
        return msg
