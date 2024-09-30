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
import socket
import threading
from collections import deque

import select

from protocol import *


class Server:
    LISTEN_IP = '0.0.0.0'
    LISTEN_PORT = 4587

    HANDSHAKE_OP = "HS"
    NOT_NEEDED_OP = "ND"
    ALLOCATE_OP = "AL"
    # HEARTBEAT_OP = "HB"
    FOUND_OP = "FN"
    PACK_SIGN = ">I"

    def __init__(self, desired_hash):
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        # variables for multithreading
        self.stop_event = threading.Event()
        self.work_lock = threading.Lock()

        # calc vars
        self.found_num = None
        self.hash = desired_hash

        # threading work vars
        self.chunk = 1000000  # arbitrary number
        self.max_num = 9999999999
        # self.quants = {}
        self.work_queue = deque()

        # fill the work queue
        lst = 1
        while lst <= self.max_num:
            end = min(self.max_num, lst + self.chunk - 1)
            self.work_queue.append((lst, end))
            lst = end + 1

    def __handshake(self, sock):
        num_of_cors = 0
        op_code, data = receive_data(sock)
        if op_code == self.HANDSHAKE_OP:
            with self.work_lock:
                if self.work_queue:
                    num_of_cors = decode_int(data)
                    print(f"num of cors = {num_of_cors}")
                    if num_of_cors <= 0:
                        print("incorrect core count")
                        num_of_cors = 0
                else:
                    send_msg(sock, format_msg(self.NOT_NEEDED_OP, b''))
        return num_of_cors

    def __allocate_work(self, sock, work_range):
        # valid_allocate = False
        if work_range:
            print(work_range[0][0])
            work_range = work_range[0][0], work_range[-1][-1]
            work_range_bytes = struct.pack(PACK_SIGN, socket.htonl(work_range[0])) + b'|' + struct.pack(PACK_SIGN,
                                                                                                        socket.htonl(
                                                                                                            work_range[
                                                                                                                1]))
            send_msg(sock, format_msg(self.ALLOCATE_OP, work_range_bytes))
            # valid_allocate = True
        else:
            send_msg(sock, format_msg(self.NOT_NEEDED_OP, b''))

        # return valid_allocate

    def __get_popped(self, num_of_cors):
        popped_arr = []
        previous_chunk = None

        with self.work_lock:
            if not self.work_queue:
                print("Work queue is empty.")
                self.stop_event.set()
                return popped_arr  # Return an empty list if there's no work

            for _ in range(num_of_cors):
                if not self.work_queue:
                    print("No more work to pop.")
                    break  # Stop if the deque is empty

                current_chunk = self.work_queue.popleft()

                if previous_chunk is not None:
                    # Check if the end of the previous chunk matches the start of the current chunk
                    if previous_chunk[1] + 1 != current_chunk[0]:
                        print(
                            f"Chunks do not match: previous end {previous_chunk[1]}, current start {current_chunk[0]}")
                        # Stop if the chunks don't match
                        self.work_queue.appendleft(current_chunk)  # Put back the chunk that doesn't match
                        break

                popped_arr.append(current_chunk)
                previous_chunk = current_chunk

        return popped_arr

    def handle_quant(self, sock):
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
                            self.stop_event.set()
                        elif op_code == self.ALLOCATE_OP:
                            work_ranges = self.__get_popped(num_of_cors)
                            self.__allocate_work(sock, work_ranges)
                        else:
                            # client disconnected - error
                            if work_ranges is not None:
                                print(work_ranges)
                                # the order is smallest to biggest.
                                # we need to insert biggest to smallest
                                work_ranges.reverse()
                                with self.work_lock:
                                    for i in work_ranges:
                                        self.work_queue.appendleft(i)
                            break

        sock.close()

    def start_server(self):
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
    server = Server(hashlib.md5(str(14567897).encode()).hexdigest())
    print(server.start_server())


if __name__ == '__main__':
    main()
