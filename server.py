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


msg types:
HANDSHAKE - server sends hash quant sends num of cors = HS
NOT NEEDED - no need for more quants
HEARTBEAT - client is alive
ALLOCATE - quant requests work, server gives range

"""
import hashlib
import time

import select
import socket
import threading
from collections import deque
from protocol import *


class Server:
    LISTEN_IP = '0.0.0.0'
    LISTEN_PORT = 4587

    HANDSHAKE_OP = "HS"
    NOT_NEEDED_OP = "ND"
    ALLOCATE_OP = "AL"
    HEARTBEAT_OP = "HB"
    FOUND_OP = "FN"
    PACK_SIGN = ">I"

    work_lock = threading.Lock()
    stop_event = threading.Event()

    def __init__(self, desired_hash: str) -> None:
        # network vars
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        # logic vars
        self.found_num = -1  # the number that made the hash (-1 = not found)
        self.hash = desired_hash

        self.chunk = 100  # arbitrary number
        self.max_num = 999
        # self.quants = {}
        self.work_queue = deque()

        # fill the work queue
        lst = 1
        while lst <= self.max_num:
            end = min(self.max_num, lst + self.chunk - 1)
            self.work_queue.append((lst, end))
            lst = end + 1

    def handshake(self, sock: socket.socket):
        # send_msg(sock, format_msg(self.HANDSHAKE_OP, self.hash.encode())) != -1
        num_of_cors = 0
        with self.work_lock:
            if self.work_queue:
                opcode, num_of_cors = receive_data(sock)
                if opcode == self.HANDSHAKE_OP:
                    num_of_cors = socket.htonl(struct.unpack(PACK_SIGN, num_of_cors)[0])
                    print(f"num of cors = {num_of_cors}")
                    if num_of_cors <= 0:
                        print("incorrect core count")
                        num_of_cors = 0
            else:
                send_msg(sock, format_msg(self.NOT_NEEDED_OP, b''))
        return num_of_cors

    def allocate_work(self, sock, work_range):

        if work_range is not []:
            work_range = work_range[0][0], work_range[-1][-1]
            work_range_bytes = struct.pack(PACK_SIGN, socket.htonl(work_range[0])) + b'|' + struct.pack(PACK_SIGN,
                                                                                                            socket.htonl(
                                                                                                                work_range[
                                                                                                                    1]))
            if send_msg(sock, format_msg(self.ALLOCATE_OP, work_range_bytes)):
                return True
        else:
            send_msg(sock, format_msg(self.NOT_NEEDED_OP, b''))
            return False

    def get_popped(self, num_of_cors):
        popped_arr = []
        previous_chunk = None

        with self.work_lock:
            if self.work_queue:
                for _ in range(num_of_cors):
                    if not self.work_queue:
                        break  # Stop if the deque is empty

                    current_chunk = self.work_queue.popleft()

                    if previous_chunk is not None:
                        # Check if the end of the previous chunk matches the start of the current chunk
                        if previous_chunk[1] + 1 != current_chunk[0]:
                            # Stop if the chunks don't match
                            self.work_queue.appendleft(current_chunk)  # Put back the chunk that doesn't match
                            break

                    popped_arr.append(current_chunk)
                    previous_chunk = current_chunk
        return popped_arr

    def handle_quant(self, sock):
        print("hello")
        """
        will get num of cors, allocate work, stop the client if necessary, set stop event if needed

                popped_items = [self.work_queue.popleft() for _ in range(num_of_cors)] need to check for double ranges
                work_range = (popped_items[0][0], popped_items[-1][-1])
        """

        popped_items = []
        found_num = -1

        num_of_cors = self.handshake(sock)
        if num_of_cors != 0:
            if send_msg(sock, format_msg(self.HANDSHAKE_OP, self.hash.encode())):
                # check for msgs
                start_time = time.time()
                active = True
                while active:
                    if self.stop_event.is_set():
                        send_msg(sock, format_msg(self.FOUND_OP, b''))
                        break
                    current_time = time.time()
                    if current_time - start_time > 5:
                        if popped_items is not None:
                            with self.work_lock:
                                for i in popped_items:
                                    self.work_queue.appendleft(i)
                        # active = False
                        break

                    # op_code, data = receive_data(sock)
                    waiting, _, _ = select.select([sock], [], [], 1)
                    if waiting:
                        op_code, data = receive_data(sock)
                        print(op_code)
                        if op_code == self.HEARTBEAT_OP:
                            start_time = current_time
                        elif op_code == self.ALLOCATE_OP:
                            popped_items = self.get_popped(num_of_cors)
                            if not self.allocate_work(sock, popped_items):
                                # active = False
                                break
                        elif op_code == self.FOUND_OP:
                            found_num = struct.pack(PACK_SIGN, socket.htonl(data))
                            self.stop_event.set()

                sock.close()
                return found_num

    def accept_quants(self) -> int | None:
        try:
            # open server for clients
            self.server_socket.bind((self.LISTEN_IP, self.LISTEN_PORT))
            self.server_socket.listen(1)
            while not self.stop_event.is_set():
                waiting, _, _ = select.select([self.server_socket], [], [], 1)
                if waiting:
                    quant_socket, quant_addr = self.server_socket.accept()
                    print(f"got quant at addr: {quant_addr}")
                    quant_thread = threading.Thread(target=self.handle_quant, args=(quant_socket,))
                    quant_thread.start()
            return self.found_num
        except socket.error as err:
            print(err)
            return None


def main():
    server = Server(hashlib.md5(str(51).encode()).hexdigest())
    print(server.accept_quants())

if __name__ == '__main__':
    main()
