import hashlib
import multiprocessing
import os
import socket
import threading

import select

from protocol import *


class Client:
    HANDSHAKE_OP = "HS"
    NOT_NEEDED_OP = "ND"
    ALLOCATE_OP = "AL"
    FOUND_OP = "FN"

    SERVER_IP = '127.0.0.1'
    SERVER_PORT = 4587

    def __init__(self):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        manager = multiprocessing.Manager()
        self.event = manager.Event()
        self.range = None
        self.has_range = threading.Event()
        self.hash = ""
        self.found = -1

    @staticmethod
    def calculate_md5(num, target_hash, event):
        returned = None
        """Calculates MD5 and checks if it matches the target hash."""
        if not event.is_set():
            # Calculate the MD5 hash for the number
            md5_hash = hashlib.md5(str(num).encode()).hexdigest()

            # DEBUG: Print the number and its MD5 hash
            # print(f"Checking number: {num}, MD5: {md5_hash}")

            if md5_hash == target_hash:
                print(f"Found match: {num}")  # DEBUG: Output when a match is found
                event.set()  # Set the flag to stop other processes
                returned = num  # Return the found number

        return returned

    def work_chunk(self, work_range):
        with multiprocessing.Pool(processes=os.cpu_count()) as pool:
            results = pool.starmap(
                self.calculate_md5,
                [(num, self.hash, self.event) for num in work_range]
            )

            # Check if we found the number with the desired hash
            for res in results:
                if res is not None:
                    print(f"Found: {res}")  # DEBUG: Output when the correct number is found
                    return res
        return None

    def __handshake(self):
        try:
            self.sock.connect((self.SERVER_IP, self.SERVER_PORT))
            if send_msg(self.sock,
                        format_msg(self.HANDSHAKE_OP, struct.pack(PACK_SIGN, socket.htonl(os.cpu_count())))):
                opcode, data = receive_data(self.sock)
                if opcode == self.HANDSHAKE_OP:
                    self.hash = data.decode().strip()  # Strip any extra whitespace
                    print(f"Target Hash: {self.hash}")  # DEBUG: Output the target hash
                else:
                    self.event.set()

            return self.hash != ""
        except socket.error as err:
            print(f"error at handshake: {err}")

    def request_work(self):
        return send_msg(self.sock, format_msg(self.ALLOCATE_OP, b''))

    def __handle_work(self):
        while not self.event.is_set():
            if self.has_range.is_set():
                self.has_range.clear()
                print(f"got range: {self.range[0]}-{self.range[1]}")
                self.found = self.work_chunk(range(self.range[0], self.range[1]+1))
                if self.found:
                    print(self.found)
                    self.event.set()
                    send_msg(self.sock, format_msg(self.FOUND_OP, struct.pack(PACK_SIGN, socket.htonl(self.found))))
                else:
                    print(f"No match found in range: {self.range[0]}-{self.range[1]}")  # DEBUG

                return

    def __boss_func(self):
        while not self.event.is_set():
            if not self.request_work():
                self.event.set()
                break
            self.__handle_work()

    def start_client(self):
        self.__handshake()
        if self.hash == "":
            print("no hash")
            self.event.set()

        threading.Thread(target=self.__boss_func).start()

        while not self.event.is_set():
            waiting, _, _ = select.select([self.sock], [], [], 1)
            if waiting:
                op_code, data = receive_data(self.sock)
                if op_code == self.ALLOCATE_OP:
                    start, end = data.split(b'|')
                    start = socket.htonl(struct.unpack(PACK_SIGN, start)[0])
                    end = socket.htonl(struct.unpack(PACK_SIGN, end)[0])
                    self.range = (start, end)
                    self.has_range.set()
                elif op_code == self.NOT_NEEDED_OP or op_code == self.FOUND_OP:
                    print(op_code)
                    self.event.set()
                else:
                    self.event.set()

            """
            start a handle_work thread that requests work, waits for range to come in.
            """


def main():
    client = Client()
    client.start_client()


if __name__ == '__main__':
    main()
