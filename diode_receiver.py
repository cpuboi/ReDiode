#!/usr/bin/env python3

import os
import pickle
import socket
import time

import redis

from tools import diode_utils

"""
This programs listens to a UDP port, receives data and validates it.

Data comes in the form of a tuple: 
(total_packets, packet_nr, redundant_copies_sent, nr_of_copy, md5sum[-6:], bytes)

Data is stored in a dictionary, when all packets of a session has been received, the receiver puts all items in the redis queue.

It is recommended to use one sender and receiver per port.


Program flow:
start_udp_server ->     # Checks that Redis is up, starts server
process_frame ->        # Sends data to UDP_frame_to_dict, if returned status is 0, adds to redis
UDP_frame_to_dict  -    # Unpacks the tuple, creates a key in the queue dict based on md5sum of data. 
                        creates a list in the queue_dict and adds all recieved data. 
                        When complete, return list and status of 0
                        

"""

# This gets the current directory of the program and the config file.
__location__ = os.path.realpath(os.path.join(os.getcwd(), os.path.dirname(__file__)))
CONFIG_FILE = os.path.join(__location__, "diode_receiver.conf")
SEEN_DICT = {}  # md5: timestamp
DEBUG = False


def read_config(config_file=CONFIG_FILE):
    with open(config_file, 'r') as f:
        for line in f:
            line_split = line.strip().replace(" ", "").split("=")
            if line_split[0] == "listener_port":
                listener_port = int(line_split[1])
            if line_split[0] == "listener_ip":
                listener_ip = line_split[1]
            if line_split[0] == "redis_out_queue":
                redis_out_queue = line_split[1]
            if line_split[0] == "redis_hostname":
                redis_hostname = line_split[1]
            if line_split[0] == "redis_port":
                redis_port = int(line_split[1])  # Must be integer
            if line_split[0] == "redis_password":
                redis_password = line_split[1]
    return listener_port, listener_ip, redis_out_queue, redis_hostname, redis_port, redis_password


def start_udp_server(ip, port, redis_hostname, redis_port, redis_password, redis_queue, buffersize=4096):
    """
    Listens to a port, receives pickled data over UDP.
    Unpickles and sends to function that processes the data.
    If processed data is OK, send it to Redis-list.

    """
    server_running = True
    udp_server_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)  # Start TCP/IP socket
    udp_server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)  # Allow reuse of socket

    redis_connected = False
    redis_server = False
    timeout = 10
    item_counter = 0
    duplicate_item_counter = 0
    while not redis_connected:
        redis_server = diode_utils.redis_connect_server(redis_hostname, redis_port, redis_password)
        try:
            redis_server.set("diode_test", "connected")  # Test if Redis is up.
            if redis_server.get("diode_test") == "connected".encode():
                print("[*] Connected to Redis.")
                redis_connected = True
        except ConnectionError:
            print(f"[X] Error: Can't connect to redis-server, retrying in {timeout} seconds..")
            time.sleep(timeout)
        except redis.exceptions.ConnectionError:
            print(f"[X] Error: Can't connect to redis-server, retrying in {timeout} seconds.")
            time.sleep(timeout)
    try:
        udp_server_socket.bind((ip, port))
        print("[*] Socket created")
    except Exception as e:
        print("[x] Error creating socket. ", e)
        exit(1)
    print("[*] Socket listening on port %s " % str(port))

    queue_dictionary = {}  # Create an empty dictionary that will hold all received items.

    try:
        while server_running:
            bytes_addr_recv = udp_server_socket.recvfrom(buffersize)
            recv_bytes = pickle.loads(bytes_addr_recv[0])
            recv_addr = bytes_addr_recv[1]
            if DEBUG:
                print(f"[*] Received: {len(recv_bytes)}  from: {recv_addr}")
            # Process recieved frame
            queue_dictionary, queue_md5_key, status, item_counter, duplicate_item_counter = process_frame(recv_bytes,
                                                                                                          queue_dictionary,
                                                                                                          redis_server,
                                                                                                          redis_queue,
                                                                                                          item_counter,
                                                                                                          duplicate_item_counter)
            if len(queue_dictionary) > 20:
                clean_up_queue_dict(queue_dictionary)
            if status == 0:
                if item_counter % 100 == 1:
                    print("[*] Sent %s items to redis. Has received %s duplicates" % (
                    item_counter, duplicate_item_counter), end="\r")
    except KeyboardInterrupt:
        print("[*] Shutting down server")
        udp_server_socket.close()
        exit(0)


def process_frame(UDP_frame, queue_dict, redis_server, redis_queue, item_counter, duplicate_item_counter):
    """
    Will send UDP-frame to udp_frame function, if status is 0, all data has been received and will get sent to redis.
    """
    queue_dict, md5sum_data, status = UDP_frame_to_dict(UDP_frame, queue_dict)
    if status == 0:
        # All data received, check if data has been received.
        item_seen, last_epoch = handle_seen_items(md5sum_data)
        if item_seen:
            duplicate_item_counter += 1
            if DEBUG:
                print("[*] %s was seen %s seconds ago." % (md5sum_data, last_epoch))
        else:
            # If not before received, send to redis.
            if DEBUG:
                print("[*] New data, will add %s to redis. " % md5sum_data)
            process_queue_dict_quick(queue_dict, md5sum_data, status, redis_server, redis_queue)
            item_counter += 1

    return queue_dict, md5sum_data, status, item_counter, duplicate_item_counter


def handle_seen_items(md5sum_data):
    """ Will check if MD5sum exists in dictionary.
    If it exists, it will return the number of seconds ago it was seen.
    If it has not been seen before, function will return -1
    If dictionary is longer than 1000 items, it will remove all items older than 1h."""
    new_epoch = int(time.time())

    oldest_items = 3
    max_items = 10
    if len(SEEN_DICT) > max_items:
        for item in list(SEEN_DICT.keys()):
            item_time = SEEN_DICT[item]
            if new_epoch - item_time > oldest_items:
                if DEBUG:
                    print("[*] Cleaning up old seen items.")
                SEEN_DICT.pop(item)

    if md5sum_data in SEEN_DICT:
        old_epoch = SEEN_DICT[md5sum_data]
        updated_epoch = new_epoch - old_epoch
        return True, updated_epoch
    else:
        SEEN_DICT[md5sum_data] = new_epoch
        return False, 0


def clean_up_queue_dict(queue_dict):
    """ If items older than 1h, remove items."""
    new_epoch = int(time.time())
    check_time = 3  # one hour
    for item in list(queue_dict.keys()):
        item_epoch = queue_dict[item]["timestamp"]
        if new_epoch - item_epoch > check_time:
            if DEBUG:
                print("[*] Removing old item from queue.")
            queue_dict.pop(item)


def process_queue_dict_quick(queue_dict, md5sum_data, status, redis_server, redis_queue):
    complete_bytearray_list = queue_dict[md5sum_data]["data_list"]
    restored_bytes = diode_utils.bytearray_joiner(complete_bytearray_list)
    diode_utils.redis_add_item_to_list(redis_server, redis_queue, restored_bytes)
    queue_dict.pop(md5sum_data)    # Remove item



def UDP_frame_to_dict(UDP_frame, queue_dict):
    """
    Will take frame and add the md5sum of the data as the key to the dict.
    The values will consist of:
     latest timestamp
     list containing all the packets.
     other stuff..
    When list is complete, it will call on a function that processes the list
    All bytes are encoded with the Reed Solomon error coding.
    Format: (total_packets, packet_nr, redundant_copies_sent, nr_of_copy, md5sum_data[-6:], md5sum_bytes[-2:], bytes)
    Status: 0:ok, 1:not full, 2: error, 3: Processing function error
    """
    status = 1

    def validate_packet(rs_byte_chunk, md5sum_chunk):
        byte_chunk = diode_utils.rs_decode(rs_byte_chunk)
        checksum = diode_utils.md5sum_bytestring(byte_chunk)[-2:]
        if DEBUG:
            print("[*] Diffing checksums", checksum, md5sum_chunk)
        if checksum == md5sum_chunk:
            return byte_chunk
        else:
            return False

    total_packets = UDP_frame[0]
    packet_nr = UDP_frame[1]
    redundant_copies = UDP_frame[2]
    copy_nr = UDP_frame[3]
    md5sum_data = UDP_frame[4]
    md5sum_chunk = UDP_frame[5]
    rs_byte_chunk = UDP_frame[6]
    epoch_timestamp = int(time.time())
    if md5sum_data in queue_dict:
        data_dict = queue_dict[md5sum_data]
        data_dict["timestamp"] = epoch_timestamp

        # Check if byte_chunk already in list.
        if data_dict["data_list"][packet_nr]:
            # Data already exists and should be valid, continue.
            if DEBUG:
                print("[*] Valid data already exists")
        else:
            validated_packet = validate_packet(rs_byte_chunk, md5sum_chunk)
            if validated_packet:
                data_dict["data_list"][packet_nr] = validated_packet
                status = 1
                data_dict["status"] = status
            else:
                data_dict["data_list"][packet_nr] = False
                status = 2
                data_dict["status"] = status

        # Check if final packet.
        packet_nr += 1
        if packet_nr == total_packets:
            # Check everything OK:
            for chunk in data_dict["data_list"]:
                # Loop over data list, if any chunk is None, status = Error (2)
                if not chunk:
                    print("[x] Not all data chunks present, UDP error.")
                    # Log error to some file
                    status = 2
                    data_dict["status"] = status

            if data_dict["status"] != 2:
                # If status is not error, return OK data.
                try:
                    status = 0
                    data_dict["status"] = status
                    # Update redis "fetched_ok_set" with MD5
                    return queue_dict, md5sum_data, status
                    # data_dict.pop(md5sum_data)
                except:
                    status = 3
                    data_dict["status"] = status

    else:  # Creating new dict item
        if DEBUG:
            print("[*] Adding new item to queue.")
        queue_dict[md5sum_data] = {}
        data_dict = queue_dict[md5sum_data]
        data_list = [None] * total_packets
        data_dict["data_list"] = data_list
        data_dict["timestamp"] = epoch_timestamp

        validated_packet = validate_packet(rs_byte_chunk, md5sum_chunk)

        if validated_packet:
            data_dict["data_list"][packet_nr] = validated_packet
            status = 1
            data_dict["status"] = status
        else:
            data_dict["data_list"][packet_nr] = False
            status = 2
            data_dict["status"] = status

    return queue_dict, md5sum_data, status


def redis_appender():
    pass


def main():
    listener_port, listener_ip, redis_out_queue, redis_hostname, redis_port, redis_password = read_config()
    start_udp_server(listener_ip, listener_port, redis_hostname, redis_port, redis_password, redis_out_queue)


main()
