"""
1. connect to the server.
2. while didn't get a msg to stop:
3. request chunk of numbers.
4. calculate hash for each.
"""
import hashlib
import os
import threading
import time

from protocol import *
import socket
import multiprocessing as mp

TYPE_CONNECT = "CN"
TYPE_ALLOCATION = "AL"
TYPE_STOP = "SP"
TYPE_HEARTBEAT = "HB"
TYPE_FOUND = "FN"
PACK_SIGN = "I"

running = mp.Event()


def connect_to_server(sock, addr):
    try:
        sock.connect(addr)
        num_of_cores = os.cpu_count()
        msg = format_msg(TYPE_CONNECT, struct.pack(PACK_SIGN, socket.htonl(num_of_cores)))
        if send_data(sock, msg) != -1:
            return None

        return recv_data()[1].decode()  # return the desired hash
    except socket.error as err:
        print(err)
        return None


def am_alive(sock):
    try:
        while not running.is_set():
            msg = format_msg(TYPE_HEARTBEAT, b'')
            if send_data(sock, msg) == -1:
                running.set()
            time.sleep(2)
    except socket.error as err:
        print(err)
        running.set()


def send_found(sock, num):
    # send a work request
    try:
        running.set()
        msg = format_msg(TYPE_FOUND, struct.pack(PACK_SIGN, socket.htonl(num)))
        send_data(msg)  # send the server my core count
    except socket.error as err:
        print(err)


def get_msgs(sock, desired_hash):
    # constantly wait for new msgs. add them to a msg queue.
    # execute the correct function based on the msg type
    while not running.is_set():
        dt_type, data = recv_data(sock)
        if dt_type != b'':
            if dt_type.decode() == TYPE_ALLOCATION:
                start, end = data.decode().split('|')
                num = work_chunk(desired_hash, range(start, end + 1))
                if num is not None:
                    send_found(sock, num)
                request_work()
            elif dt_type.decode == TYPE_STOP:
                running.set()
                return


def request_work():
    # send a work request
    try:
        msg = format_msg(TYPE_ALLOCATION, b'')
        return send_data(msg)  # send the server my core count
    except socket.error as err:
        print(err)
        return -1  # something went wrong


def calculate_md5(num, desired_hash):
    if not running.is_set():
        md5_hash = hashlib.md5(str(num).encode()).hexdigest()
        if md5_hash == desired_hash:
            running.set()
            return md5_hash
    return None


def work_chunk(desired_hash, num_range):
    num = None
    with mp.Pool(processes=mp.cpu_count()) as pool:
        results = [pool.apply_async(calculate_md5, (n, desired_hash)) for n in num_range]

        for r in results:
            if r is not None:
                print("found")
                num = r
                pool.terminate()
                break

    return num


def main():
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    des_hash = connect_to_server(sock, ("0.0.0.0", 1234))
    if des_hash is not None:
        threading.Thread(target=get_msgs, daemon=True, args=(sock, des_hash)).start()
        threading.Thread(target=get_msgs, daemon=True, args=(sock,)).start()
        request_work()
