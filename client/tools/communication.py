import socket


def write_to_socket(socket: socket.socket, msg: bytes):
    """
    Write a message to a socket

    :param socket: socket to write to
    :param msg: message to write
    """
    socket.sendall(len(msg).to_bytes(4, 'big') + msg)


def read_from_socket(sock: socket.socket) -> str:
    """
    Read a message from a socket, ensuring that all bytes are received.

    :param sock: socket to read from
    :return: message read
    """
    # Read the first 4 bytes to get the message length
    length_data = recv_exactly(sock, 4)
    msg_len = int.from_bytes(length_data, 'big')

    # Read the actual message
    message_data = recv_exactly(sock, msg_len)

    return message_data.decode('utf-8')


def recv_exactly(sock: socket.socket, num_bytes: int) -> bytes:
    """
    Ensure we receive exactly num_bytes from the socket
    """
    data = b""
    while len(data) < num_bytes:
        chunk = sock.recv(num_bytes - len(data))
        if not chunk:  # Connection closed before receiving expected data
            raise ConnectionError(
                "Socket closed before receiving full message.")
        data += chunk
    return data
