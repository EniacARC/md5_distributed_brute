import socket
import struct
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

INT_SIZE = 4
OP_SIZE = 2
PACK_SIGN = ">I"


def format_msg(op_code: str, data: bytes) -> bytes:
    """
    Formats a message for sending over the socket.

    :param op_code: The operation code for the message.
    :type op_code: str
    :param data: The data to be sent.
    :type data: bytes
    :return: The formatted message as bytes.
    :rtype: bytes
    """
    length = INT_SIZE + OP_SIZE + len(data)
    msg = struct.pack(PACK_SIGN, socket.htonl(length)) + op_code.encode('utf-8') + data
    logging.info(f"Formatted message: op_code={op_code}, length={length}, data_length={len(data)}")
    return msg


def send_msg(sock: socket.socket, data: bytes) -> bool:
    """
    Sends a message over the socket.

    :param sock: The socket to send data through.
    :type sock: socket.socket
    :param data: The data to send.
    :type data: bytes
    :return: True if the message was sent successfully, False otherwise.
    :rtype: bool
    """
    was_sent = False
    try:
        sent = 0
        while sent < len(data):
            sent += sock.send(data[sent:])
        was_sent = True
        logging.info(f"Sent message: {data}")
    except socket.error as err:
        logging.error(f"Error while sending: {err}")
    finally:
        return was_sent


def receive_data(sock: socket.socket) -> (str, bytes):
    """
    Receives data from the socket and unpacks it.

    :param sock: The socket to receive data from.
    :type sock: socket.socket
    :return: A tuple containing the operation code and data received.
    :rtype: (str, bytes)
    """
    buf = b''
    data_len_encoded = b''
    op_code_encoded = b''
    data = b''
    data_len = 0
    op_code = ""

    try:
        # Receive length of the message
        while len(data_len_encoded) < INT_SIZE:
            buf = sock.recv(INT_SIZE - len(data_len_encoded))
            if buf == b'':
                data_len_encoded = b''
                logging.warning("Connection closed while receiving data length.")
                break
            data_len_encoded += buf

        if data_len_encoded != b'':
            data_len = socket.htonl(struct.unpack(PACK_SIGN, data_len_encoded)[0]) - 6  # remove length and op length

            # Receive operation code
            while len(op_code_encoded) < OP_SIZE:
                buf = sock.recv(OP_SIZE - len(op_code_encoded))
                if buf == b'':
                    op_code_encoded = b''
                    logging.warning("Connection closed while receiving operation code.")
                    break
                op_code_encoded += buf

            op_code = op_code_encoded.decode()

            # Receive the actual data
            while len(data) < data_len:
                buf = sock.recv(data_len - len(data))
                if buf == b'':
                    op_code = ""
                    data = b''
                    logging.warning("Connection closed while receiving data.")
                    break
                data += buf

    except socket.error as err:
        logging.error(f"Error while receiving data: {err}")
    finally:
        logging.info(f"Received data: op_code={op_code}, data_length={len(data)}")
        return op_code, data


def decode_int(data):
    """
    Decodes an integer from bytes.

    :param data: The data to decode.
    :type data: bytes
    :return: The decoded integer.
    :rtype: int
    """
    decoded_value = socket.htonl(struct.unpack(PACK_SIGN, data)[0])
    logging.info(f"Decoded integer: {decoded_value}")
    return decoded_value
