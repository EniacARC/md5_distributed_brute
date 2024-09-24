import hashlib
import socket
import os
import multiprocessing as mp
import threading
import time
import struct

from protocol import *
from utils import check_md5


# Helper function outside of the Client class, which can be pickled
# def check_md5(num, target_hash, running):
#     """Calculates MD5 and checks if it matches the target hash."""
#     if running.is_set():
#         return None  # Stop early if hash is already found
#
#     # Calculate the MD5 hash for the number
#     md5_hash = hashlib.md5(str(num).encode()).hexdigest()
#
#     # DEBUG: Print the number and its MD5 hash
#     print(f"Checking number: {num}, MD5: {md5_hash}")
#
#     if md5_hash == target_hash:
#         print(f"Found match: {num}")  # DEBUG: Output when a match is found
#         running.set()  # Set the flag to stop other processes
#         return num  # Return the found number
#
#     return None


class Client:
    HANDSHAKE_OP = "HS"
    NOT_NEEDED_OP = "ND"
    ALLOCATE_OP = "AL"
    HEARTBEAT_OP = "HB"
    FOUND_OP = "FN"

    SERVER_IP = '127.0.0.1'
    SERVER_PORT = 4587

    def __init__(self):
        # Use Manager to create an Event that can be shared between processes
        self.manager = mp.Manager()
        self.running = self.manager.Event()
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.hash = ""
        self.found = None
        self.range = None

    def handshake(self):
        try:
            self.sock.connect((self.SERVER_IP, self.SERVER_PORT))
            if send_msg(self.sock,
                        format_msg(self.HANDSHAKE_OP, struct.pack(PACK_SIGN, socket.htonl(os.cpu_count())))):
                opcode, data = receive_data(self.sock)
                if opcode == self.HANDSHAKE_OP:
                    self.hash = data.decode().strip()  # Strip any extra whitespace
                    print(f"Target Hash: {self.hash}")  # DEBUG: Output the target hash
                else:
                    self.running.set()

            return self.hash != ""
        except socket.error as err:
            print(f"error at handshake: {err}")

    def send_heartbeat(self):
        try:
            while True:
                msg = format_msg(self.HEARTBEAT_OP, b'')
                if not send_msg(self.sock, msg):
                    self.running.set()
                time.sleep(9)
        except socket.error as err:
            print(err)
            self.running.set()

    def request_work(self):
        return send_msg(self.sock, format_msg(self.ALLOCATE_OP, b''))

    def work_chunk(self, num_range):
        """Distributes work across multiple processes."""
        # Pass the check_md5 function with arguments to the pool
        with mp.Pool(processes=os.cpu_count()) as pool:
            result = []

            for num in num_range:
                # Check if running is set and stop processing if it is
                if self.running.is_set():
                    print(f"Stopping processing early for range {num_range[0]}-{num_range[-1]}")
                    break  # Stop the loop if the running flag is set

                # Use starmap for multiprocessing, processing one batch at a time
                result = pool.starmap(
                    check_md5,
                    [(num, self.hash, self.running)]
                )

                # Check if we found the number with the desired hash
                for res in result:
                    if res is not None:
                        print(f"Found: {res}")  # DEBUG: Output when the correct number is found
                        return res

        return None

    def handle_work(self):
        while not self.running.is_set():
            if not self.request_work():
                self.running.set()
                break
            while self.range is None and not self.running.is_set():
                pass
            if not self.running.is_set():
                print(f"got range: {self.range[0]}-{self.range[1]}")
                self.found = self.work_chunk(range(self.range[0], self.range[1]))
                if self.found:
                    print(self.found)
                    self.running.set()
                    send_msg(self.sock, format_msg(self.FOUND_OP, struct.pack(PACK_SIGN, socket.htonl(self.found))))
                else:
                    print(f"No match found in range: {self.range[0]}-{self.range[1]}")  # DEBUG

    def start(self):
        self.handshake()
        if self.hash == "":
            print("no hash")
            self.running.set()
        threading.Thread(target=self.send_heartbeat, daemon=True).start()
        threading.Thread(target=self.handle_work).start()

        while not self.running.is_set():
            op_code, data = receive_data(self.sock)
            if op_code == self.ALLOCATE_OP:
                start, end = data.split(b'|')
                start = socket.htonl(struct.unpack(PACK_SIGN, start)[0])
                end = socket.htonl(struct.unpack(PACK_SIGN, end)[0])
                self.range = (start, end)
            elif op_code == self.NOT_NEEDED_OP or op_code == self.FOUND_OP:
                print(op_code)
                self.running.set()


def main():
    client = Client()
    client.start()


if __name__ == '__main__':
    main()
