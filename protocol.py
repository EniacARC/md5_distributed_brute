"""
protocol struct: LENGTH(4 bytes - int) + OP_CODE(2 bytes - char + DATA[(LENGTH - 4 - 2 bytes) - byte]
"""

import socket
import struct

INT_SIZE = 4
OP_SIZE = 2
PACK_SIGN = ">I"


def format_msg(op_code: str, data: bytes) -> bytes:
    length = INT_SIZE + OP_SIZE + len(data)
    msg = struct.pack(PACK_SIGN, socket.htonl(length)) + op_code.encode('utf-8') + data
    return msg


def send_msg(sock: socket.socket, data: bytes) -> bool:
    try:
        sent = 0
        while sent < len(data):
            sent += sock.send(data[sent:])
        return True
    except socket.error as err:
        print(f"error while sending: {err}")
        return False


def receive_data(sock: socket.socket) -> (str, bytes):
    pass

