import hashlib
import socket
import threading
import time
from collections import deque
import select
import logging
from protocol import *

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


class Server:
    LISTEN_IP = '0.0.0.0'
    LISTEN_PORT = 4587

    HANDSHAKE_OP = "HS"
    NOT_NEEDED_OP = "ND"
    ALLOCATE_OP = "AL"
    FOUND_OP = "FN"
    PACK_SIGN = ">I"

    def __init__(self, desired_hash):
        """
        Initializes the Server instance.

        :param desired_hash: The hash value to be found by clients.
        :type desired_hash: str
        """
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        # variables for multithreading
        self.stop_event = threading.Event()
        self.work_lock = threading.Lock()

        # calc vars
        self.found_num = None
        self.hash = desired_hash

        # threading work vars
        self.chunk = 1000  # arbitrary number
        self.max_num = 9999999999
        self.work_queue = deque()

        # fill the work queue
        lst = 1
        while lst <= self.max_num:
            end = min(self.max_num, lst + self.chunk - 1)
            self.work_queue.append((lst, end))
            lst = end + 1

    def __handshake(self, sock):
        """
        Handles the handshake process with a client.

        :param sock: The socket for the client connection.
        :type sock: socket.socket
        :return: The number of CPU cores provided by the client.
        :rtype: int
        """
        num_of_cors = 0
        op_code, data = receive_data(sock)
        if op_code == self.HANDSHAKE_OP:
            with self.work_lock:
                if self.work_queue:
                    num_of_cors = decode_int(data)
                    logging.info(f"Number of CPU cores: {num_of_cors}")
                    if num_of_cors <= 0:
                        logging.warning("Incorrect core count provided by the client.")
                        num_of_cors = 0
                else:
                    send_msg(sock, format_msg(self.NOT_NEEDED_OP, b''))
        return num_of_cors

    def __allocate_work(self, sock, work_range):
        """
        Allocates a work range to the client.

        :param sock: The socket for the client connection.
        :type sock: socket.socket
        :param work_range: The range of numbers allocated to the client.
        :type work_range: tuple
        """
        if work_range:
            work_range = work_range[0][0], work_range[-1][-1]
            work_range_bytes = struct.pack(self.PACK_SIGN, socket.htonl(work_range[0])) + struct.pack(
                self.PACK_SIGN, socket.htonl(work_range[1]))
            send_msg(sock, format_msg(self.ALLOCATE_OP, work_range_bytes))
            logging.info(f"Allocated work range: {work_range[0]} - {work_range[1]}")
        else:
            send_msg(sock, format_msg(self.NOT_NEEDED_OP, b''))

    def get_popped(self, num_of_cors):
        """
        Pops a specified number of work chunks from the work queue.

        :param num_of_cors: The number of CPU cores requesting work.
        :type num_of_cors: int
        :return: A list of work chunks allocated to the client.
        :rtype: list
        """
        popped_arr = []
        previous_chunk = None

        with self.work_lock:
            if not self.work_queue:
                logging.warning("Work queue is empty.")
                self.stop_event.set()
                return popped_arr  # Return an empty list if there's no work

            for _ in range(num_of_cors):
                if not self.work_queue:
                    logging.info("No more work to pop.")
                    break  # Stop if the deque is empty

                current_chunk = self.work_queue.popleft()

                if previous_chunk is not None:
                    # Check if the end of the previous chunk matches the start of the current chunk
                    if previous_chunk[1] + 1 != current_chunk[0]:
                        logging.error(
                            f"Chunks do not match: previous end {previous_chunk[1]}, current start {current_chunk[0]}")
                        # Stop if the chunks don't match
                        self.work_queue.appendleft(current_chunk)  # Put back the chunk that doesn't match
                        break

                popped_arr.append(current_chunk)
                previous_chunk = current_chunk

        return popped_arr

    def handle_quant(self, sock):
        """
        Handles the connection with a client (quant) and processes requests.

        :param sock: The socket for the client connection.
        :type sock: socket.socket
        """
        work_ranges = []
        num_of_cors = self.__handshake(sock)
        if num_of_cors > 0:
            if send_msg(sock, format_msg(self.HANDSHAKE_OP, self.hash.encode())):
                while not self.stop_event.is_set():
                    waiting, _, _ = select.select([sock], [], [], 1)
                    if waiting:
                        op_code, data = receive_data(sock)
                        if op_code == self.FOUND_OP:
                            self.found_num = decode_int(data)
                            logging.info(f"Found number: {self.found_num}. Stopping server.")
                            self.stop_event.set()
                        elif op_code == self.ALLOCATE_OP:
                            work_ranges = self.__get_popped(num_of_cors)
                            self.__allocate_work(sock, work_ranges)
                        else:
                            logging.error("Client disconnected or sent an unknown operation.")
                            if work_ranges is not None:
                                logging.info(f"Returning work ranges to queue: {work_ranges}")
                                work_ranges.reverse()
                                with self.work_lock:
                                    for i in work_ranges:
                                        self.work_queue.appendleft(i)
                            break

        sock.close()

    def start_server(self):
        """
        Starts the server and listens for incoming client connections.

        :return: The number found by a client or None if an error occurs.
        :rtype: int or None
        """
        try:
            # open server for clients
            self.server_socket.bind((self.LISTEN_IP, self.LISTEN_PORT))
            self.server_socket.listen(1)
            logging.info("Server started. Waiting for clients...")
            while not self.stop_event.is_set():
                waiting, _, _ = select.select([self.server_socket], [], [], 1)
                if waiting:
                    quant_socket, quant_addr = self.server_socket.accept()
                    logging.info(f"Client connected from address: {quant_addr}")
                    quant_thread = threading.Thread(target=self.handle_quant, args=(quant_socket,))
                    quant_thread.start()
        except socket.error as err:
            logging.error(f"Socket error: {err}")
        finally:
            return self.found_num


def main():
    """
    Main function to start the server and measure the execution time.
    """
    server = Server(hashlib.md5(str(500000).encode()).hexdigest())
    print(server.start_server())


if __name__ == '__main__':
    """ assert server functionality """
    desired_hash = hashlib.md5(str(500000).encode()).hexdigest()
    server_test = Server(desired_hash)

    expected_num_chunks = (server_test.max_num + server_test.chunk - 1) // server_test.chunk
    actual_num_chunks = len(server_test.work_queue)
    assert actual_num_chunks == expected_num_chunks

    num_of_cors_test = 5
    popped = server_test.get_popped(num_of_cors_test)
    assert len(popped) == 5
    for i, chunk in enumerate(popped):
        expected_start = 1 + i * server_test.chunk
        expected_end = min(server_test.max_num, (i + 1) * server_test.chunk)
        assert chunk == (expected_start, expected_end)

    # Ensure the work queue has been updated
    remaining = len(server_test.work_queue)
    expected_remaining = ((server_test.max_num + server_test.chunk - 1) // server_test.chunk) - 5
    assert remaining == expected_remaining

    start_time = time.time()
    main()
    print(time.time() - start_time)

"""
Server Workflow:
For each client {
1. Wait for client to connect - each client gives its core number.
2. Allocate a chunk (const) * cores.
3. Reply with DESIRED_HASH.
4. Send chunk to calculate.
}

If a client returns a number, send stop to all other clients.
If a client wants a chunk and there aren't any more chunks, send NOT_NEEDED.

Message Types:
HANDSHAKE - server sends hash; quant sends num of cores = HS.
NOT NEEDED - no need for more quants.
HEARTBEAT - client is alive.
ALLOCATE - quant requests work; server gives range.
"""
