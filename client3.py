import hashlib
import socket
import os
import multiprocessing as mp
import threading
import time

from protocol import *


class Client:
    HANDSHAKE_OP = "HS"
    NOT_NEEDED_OP = "ND"
    ALLOCATE_OP = "AL"
    HEARTBEAT_OP = "HB"
    FOUND_OP = "FN"

    SERVER_IP = '127.0.0.1'
    SERVER_PORT = 4587

    def __init__(self):
        self.running = mp.Event()
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.hash = ""
        self.found = None
        self.range = None

    def handshake(self):
        try:
            self.sock.connect((self.SERVER_IP, self.SERVER_PORT))
            # print(struct.pack(PACK_SIGN, socket.htonl(os.cpu_count())))
            # print(os.cpu_count())
            # print(socket.htonl(struct.unpack(PACK_SIGN, struct.pack(PACK_SIGN, socket.htonl(os.cpu_count())))[0]))
            if send_msg(self.sock,
                        format_msg(self.HANDSHAKE_OP, struct.pack(PACK_SIGN, socket.htonl(os.cpu_count())))):
                opcode, data = receive_data(self.sock)
                print(opcode)
                print(data)
                if opcode == self.HANDSHAKE_OP:
                    self.hash = data.decode()
                else:
                    self.running.set()

            return self.hash != ""
        except socket.error as err:
            print(f"error at handshake: {err}")

    def send_heartbeat(self):
        try:
            while True:
                # while not self.running.is_set():
                msg = format_msg(self.HEARTBEAT_OP, b'')
                if not send_msg(self.sock, msg):
                    self.running.set()
                time.sleep(2)
        except socket.error as err:
            print(err)
            self.running.set()

    # def request_work(self):
    #     start, end = -1, -1
    #     if send_msg(self.sock, format_msg(self.ALLOCATE_OP, b'')) != -1:
    #         opcode, data = receive_data(self.sock)
    #         if opcode == self.ALLOCATE_OP:
    #             start, end = data.split(b'|')
    #             start = socket.htonl(struct.unpack(PACK_SIGN, start)[0])
    #             end = socket.htonl(struct.unpack(PACK_SIGN, end)[0])
    #         elif opcode == self.NOT_NEEDED_OP:
    #             self.running.set()
    #         else:
    #             self.running.set()
    #
    #         return start, end

    def request_work(self):
        return send_msg(self.sock, format_msg(self.ALLOCATE_OP, b''))

    def calculate_md5(self, num):
        if not self.running.is_set():
            md5_hash = hashlib.md5(str(num).encode()).hexdigest()
            if md5_hash == self.hash:
                self.running.set()
                return md5_hash
        return None

    def work_chunk(self, num_range):
        num = None
        with mp.Pool(processes=mp.cpu_count()) as pool:
            results = [pool.apply_async(self.calculate_md5, n) for n in num_range]

            for r in results:
                if r is not None:
                    print(f"found: {r}")
                    num = r
                    pool.terminate()
                    break

        self.range = None
        return num

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
                    self.running.set()
                    send_msg(self.sock, format_msg(self.FOUND_OP, struct.pack(PACK_SIGN, socket.htonl(self.found))))

    def start(self):
        self.handshake()
        if self.hash == "":
            print("no hash")
            self.running.set()
        threading.Thread(target=self.send_heartbeat, daemon=True).start()
        threading.Thread(target=self.handle_work).start()

        while not self.running.is_set():
            op_code, data = receive_data(self.sock)
            print(op_code)
            if op_code == self.ALLOCATE_OP:
                start, end = data.split(b'|')
                print("got data")
                start = socket.htonl(struct.unpack(PACK_SIGN, start)[0])
                end = socket.htonl(struct.unpack(PACK_SIGN, end)[0])
                self.range = (start, end)
            elif op_code == self.NOT_NEEDED_OP or op_code == self.FOUND_OP:
                self.running.set()
            else:
                self.running.set()


def main():
    client = Client()
    client.start()


if __name__ == '__main__':
    main()
