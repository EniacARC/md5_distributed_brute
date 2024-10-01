import hashlib
import multiprocessing
import os
import socket
import threading
import select
import logging

from protocol import *

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


class Client:
    HANDSHAKE_OP = "HS"
    NOT_NEEDED_OP = "ND"
    ALLOCATE_OP = "AL"
    FOUND_OP = "FN"

    SERVER_IP = '127.0.0.1'
    SERVER_PORT = 4587

    def __init__(self):
        """
        Initializes the Client instance.

        """
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        manager = multiprocessing.Manager()
        self.event = manager.Event()
        self.range = None
        self.has_range = threading.Event()
        self.hash = ""
        self.found = -1

    @staticmethod
    def calculate_md5(num, target_hash, event):
        """
        Calculates MD5 and checks if it matches the target hash.

        :param num: The number to calculate the MD5 hash for.
        :type num: int
        :param target_hash: The target hash to compare against.
        :type target_hash: str
        :param event: The event flag to signal when a match is found.
        :type event: multiprocessing.Event
        :return: The found number if a match is found, else None.
        :rtype: int or None
        """
        returned = None
        if not event.is_set():
            # Calculate the MD5 hash for the number
            md5_hash = hashlib.md5(str(num).encode()).hexdigest()

            if md5_hash == target_hash:
                logging.info(f"Found match: {num}")  # Log when a match is found
                event.set()  # Set the flag to stop other processes
                returned = num  # Return the found number

        return returned

    def work_chunk(self, work_range):
        """
        Processes a range of numbers to find the target hash.

        :param work_range: The range of numbers to process.
        :type work_range: range
        :return: The found number if a match is found, else None.
        :rtype: int or None
        """
        with multiprocessing.Pool(processes=os.cpu_count()) as pool:
            results = pool.starmap(
                self.calculate_md5,
                [(num, self.hash, self.event) for num in work_range]
            )

            # Check if we found the number with the desired hash
            for res in results:
                if res is not None:
                    logging.info(f"Found: {res}")  # Log when the correct number is found
                    return res
        return None

    def __handshake(self):
        """
        Handles the handshake process with the server.

        :return: True if handshake was successful, False otherwise.
        :rtype: bool
        """
        try:
            self.sock.connect((self.SERVER_IP, self.SERVER_PORT))
            if send_msg(self.sock,
                        format_msg(self.HANDSHAKE_OP, struct.pack(PACK_SIGN, socket.htonl(os.cpu_count())))):
                opcode, data = receive_data(self.sock)
                if opcode == self.HANDSHAKE_OP:
                    self.hash = data.decode().strip()  # Strip any extra whitespace
                    logging.info(f"Target Hash: {self.hash}")  # Log the target hash
                else:
                    self.event.set()

            return self.hash != ""
        except socket.error as err:
            logging.error(f"Error during handshake: {err}")

    def request_work(self):
        """
        Requests work from the server.

        :return: True if the request was successful, False otherwise.
        :rtype: bool
        """
        return send_msg(self.sock, format_msg(self.ALLOCATE_OP, b''))

    def __handle_work(self):
        """
        Handles the processing of work when a range is available.

        """
        while not self.event.is_set():
            if self.has_range.is_set():
                self.has_range.clear()
                logging.info(f"Got range: {self.range[0]}-{self.range[1]}")
                self.found = self.work_chunk(range(self.range[0], self.range[1] + 1))
                if self.found:
                    logging.info(f"Found: {self.found}")
                    self.event.set()
                    send_msg(self.sock, format_msg(self.FOUND_OP, struct.pack(PACK_SIGN, socket.htonl(self.found))))
                else:
                    logging.info(
                        f"No match found in range: {self.range[0]}-{self.range[1]}")  # Log if no match is found

                return

    def __boss_func(self):
        """
        Main loop for the client to request work and handle results.

        """
        while not self.event.is_set():
            if not self.request_work():
                self.event.set()
                break
            self.__handle_work()

    def start_client(self):
        """
        Starts the client and initiates the connection to the server.

        """
        self.__handshake()
        if self.hash == "":
            logging.warning("No hash received. Stopping client.")
            self.event.set()

        threading.Thread(target=self.__boss_func).start()

        while not self.event.is_set():
            waiting, _, _ = select.select([self.sock], [], [], 1)
            if waiting:
                op_code, data = receive_data(self.sock)
                if op_code == self.ALLOCATE_OP:
                    start = data[:INT_SIZE]
                    end = data[INT_SIZE: INT_SIZE * 2]
                    start = socket.htonl(struct.unpack(PACK_SIGN, start)[0])
                    end = socket.htonl(struct.unpack(PACK_SIGN, end)[0])
                    self.range = (start, end)
                    self.has_range.set()
                elif op_code == self.NOT_NEEDED_OP or op_code == self.FOUND_OP:
                    logging.info(f"Received operation code: {op_code}")
                    self.event.set()
                else:
                    self.event.set()
