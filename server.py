"""
server workflow:
for each client {
1. wait for client to connect - each client gives his core number.
2. allocate a chunk(const) * cors
3. replay with DESIRED_HASH
4. send chunk to calculate
}

if client returns number sends stop to all other clients.
if client wants chunk and there isn't anymore chunks send NOT_NEEDED

"""
import select
import socket
import threading
from collections import deque
from protocol import *


class Server:
    LISTEN_IP = '0.0.0.0'
    LISTEN_PORT = 4587

    work_lock = threading.Lock()
    stop_event = threading.Event()

    def __init__(self, desired_hash: str) -> None:
        # network vars
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        # logic vars
        self.found_num = -1  # the number that made the hash (-1 = not found)
        self.hash = desired_hash

        self.chunk = 1000  # arbitrary number
        self.max_num = 9999999999
        self.quants = {}
        self.work_queue = deque()

        # fill the work queue
        lst = 1
        while lst <= self.max_num:
            end = min(self.max_num, lst + self.chunk - 1)
            self.work_queue.append(lst, end)
            lst = end + 1

    def handle_quant(self):
        """
        will get num of cors, allocate work, stop the client if necessary, set stop event if needed
        """

        pass

    def accept_quants(self) -> int | None:
        try:
            # open server for clients
            self.server_socket.bind((self.LISTEN_IP, self.LISTEN_PORT))
            self.server_socket.listen()
            while not self.stop_event.is_set():
                waiting, _, _ = select.select([self.server_socket], [], [])
                if waiting:
                    quant_socket, quant_addr = self.server_socket.accept()
                    quant_thread = threading.Thread(target=self.handle_quant, args=(quant_socket,))
                    quant_thread.start()
            return self.found_num
        except socket.error as err:
            print(err)
            return None
