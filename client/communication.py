import codecs
import socket


class Socket:
    """
    Socket wrapper to handle message encoding and decoding and avoid short reads/writes.
    """
    def __init__(self, sock: socket.socket):
        self.sock = sock
        self.decoder = codecs.getincrementaldecoder('utf-8')()

    def send(self, msg: bytes):
        """
        Write a message to a socket.

        :param socket: socket to write to
        :param msg: message to write
        """
        self.sock.sendall(len(msg).to_bytes(4, 'big') + msg)

    def read(self) -> tuple[int, str]:
        """
        Read a message from a socket, ensuring that all bytes are received.

        :param sock: socket to read from
        :return: message read
        """
        # Read the first 4 bytes to get the message length
        length_data = self._recv_exactly(4)
        msg_len = int.from_bytes(length_data, 'big')

        # Read the actual message
        message_data = self._recv_exactly(msg_len)
        decoded_data = self.decoder.decode(message_data)

        return msg_len, decoded_data

    def _recv_exactly(self, num_bytes: int) -> bytes:
        """
        Receive exactly num_bytes from the socket.
        Raises ConnectionError if the socket is closed before receiving the expected data.
        """
        data = b""
        while len(data) < num_bytes:
            chunk = self.sock.recv(num_bytes - len(data))
            if not chunk:  # Connection closed before receiving expected data
                raise ConnectionError(
                    "Socket closed before receiving full message.")
            data += chunk
        return data
