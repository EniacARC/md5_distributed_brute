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
    # print(f"sending: {data}")
    was_sent = False
    try:
        sent = 0
        while sent < len(data):
            sent += sock.send(data[sent:])
        was_sent = True
    except socket.error as err:
        print(f"error while sending: {err}")
    finally:
        return was_sent


def receive_data(sock: socket.socket) -> (str, bytes):
    buf = b''
    data_len_encoded = b''
    op_code_encoded = b''
    data = b''
    data_len = 0
    op_code = ""
    try:
        while len(data_len_encoded) < INT_SIZE:
            buf = sock.recv(INT_SIZE - len(data_len_encoded))
            if buf == b'':
                data_len_encoded = b''
                break
            data_len_encoded += buf

        if data_len_encoded != b'':
            data_len = socket.htonl(struct.unpack(PACK_SIGN, data_len_encoded)[0]) - 6  # remove your length + op length

            while len(op_code_encoded) < OP_SIZE:
                buf = sock.recv(OP_SIZE - len(op_code_encoded))
                if buf == b'':
                    op_code_encoded = b''
                    break
                op_code_encoded += buf

            op_code = op_code_encoded.decode()
            while len(data) < data_len:
                buf = sock.recv(data_len - len(data))
                if buf == b'':
                    op_code = ""
                    data = b''
                    break
                data += buf

    except socket.error as err:
        print(f"err while recv: {err}")
    finally:
        return op_code, data


def decode_int(data):
    return socket.htonl(struct.unpack(PACK_SIGN, data)[0])
