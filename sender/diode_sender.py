#!/usr/bin/env python3

import gzip
import os
import pickle
import socket
import time
from tools import diode_utils

# Todo: Check out UDT https://udt.sourceforge.io/doc.html

# This gets the current directory of the program and the config file.
__location__ = os.path.realpath(os.path.join(os.getcwd(), os.path.dirname(__file__)))
CONFIG_FILE = os.path.join(__location__, "diode_sender.conf")
DEBUG = False

SLEEP_TIME_SECONDS=2


def read_config(config_file=CONFIG_FILE):
    with open(config_file, 'r') as f:
        for line in f:
            line_split = line.rstrip().replace(" ", "").split("=")
            if line_split[0] == "diode_receiver_hostname":
                diode_receiver_hostname = line_split[1]
            if line_split[0] == "diode_receiver_port":
                diode_receiver_port = int(line_split[1])  # Must be integer
            if line_split[0] == "sending_interface":
                sending_interface = line_split[1]
            if line_split[0] == "redis_source_queue":
                redis_source_queue = line_split[1]
            if line_split[0] == "redis_hostname":
                redis_hostname = line_split[1]
            if line_split[0] == "redis_port":
                redis_port = int(line_split[1])  # Must be integer
            if line_split[0] == "redis_password":
                redis_password = line_split[1]
    return diode_receiver_hostname, diode_receiver_port, sending_interface, redis_source_queue, redis_hostname, redis_port, redis_password


def decompress_dict(zipped_dict):
    """
    Takes gzipped json data, unzips and returns dictionary
    :param zipped_dict:
    :return:
    """
    unzipped = gzip.decompress(zipped_dict).decode('utf-8')
    return unzipped


def udp_send_data(data, ip="localhost", port=8888):
    client_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    client_socket.sendto(data, (ip, port))


def bytearray_to_udp_frame_generator(in_bytearray, redundant_copies=2):
    """ Will Reed Solomon encode data, split it up and put it in some weird tuple frame, then yield a generator.
    Format: (total_packets, packet_nr, redundant_copies_to_send, nr_of_copy, md5sum_data[-6:], md5sum_bytes[-2:], bytes)
    """
    # Will send data $num if connection is crap
    split_bytearray = diode_utils.bytearray_splitter_by_bytes(in_bytearray)
    md5sum_bytearray = diode_utils.md5sum_bytestring(in_bytearray)
    total_packets = len(split_bytearray)
    copy_num = 0
    for copy in range(redundant_copies):
        copy_num += 1  # if redundant copies more than 1, send everything again.
        for packet_num in range(len(split_bytearray)):
            # RS Encode every chunk and add to frame, then send.
            byte_chunk = split_bytearray[packet_num]
            rs_byte_chunk = diode_utils.rs_encode(byte_chunk)
            md5sum_bytes = diode_utils.md5sum_bytestring(split_bytearray[packet_num])
            md5sum_data = md5sum_bytearray
            packet_tuple = (
                total_packets, packet_num, redundant_copies, copy_num, md5sum_data[-6:], md5sum_bytes[-2:],
                rs_byte_chunk)
            yield packet_tuple


def listen_to_redis_send_diode(redis_queue, redis_hostname, redis_password):
    """
    This function takes every item in a redis queue and sends them through the diode.

    """
    connected = True
    redis_server = diode_utils.redis_connect_server(ip=redis_hostname, password=redis_password)
    published_items = 0
    while connected:
        time_string = time.ctime()
        item_to_publish = redis_server.lpop(redis_queue)
        # item_to_publish = decompress_dict(item_to_publish)
        if not item_to_publish:
            print("%s:  Redis queue empty, published: %s items" % (time_string, published_items))
            time.sleep(SLEEP_TIME_SECONDS)
        elif len(item_to_publish) == 0:
            time.sleep(SLEEP_TIME_SECONDS)
            print("0byte object from redis.")
        else:
            """ If there is data in the object from Redis, send to diode.
             First split object to chunks and add reed solomon information
             Make sure object is a bytes """
            if type(item_to_publish) != bytes:
                item_to_publish = item_to_publish.encode()
            item_generator = bytearray_to_udp_frame_generator(item_to_publish)

            try:
                while item_generator:
                    send_item = pickle.dumps(next(item_generator))
                    # send_item = next(item_generator)
                    udp_send_data(send_item)
                    time.sleep(0.001)  # This was necessary in one test occasion.
            except StopIteration:
                # Exhausted the generator containing bytes from object to send.
                if DEBUG:
                    print("[*] Sent item.")

            published_items += 1
            if published_items % 2000 == 1:
                print("[*] Sent %s items." % published_items)
                exit()


def main():
    diode_receiver_hostname, diode_receiver_port, sending_interface, redis_source_queue, redis_hostname, redis_port, redis_password = read_config()
    listen_to_redis_send_diode(redis_source_queue, redis_hostname, redis_password)


main()
