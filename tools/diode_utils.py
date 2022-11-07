


import sys
import redis
from hashlib import md5
from reedsolo import RSCodec


## Bytearray utils ###########
def bytearray_splitter_by_bytes(in_bytes, split_bytes=1024):
    counter = 1
    out_list = []
    _bytearray = bytearray()
    for i in in_bytes:
        _bytearray.append(i)
        if counter % split_bytes == 0:
            out_list.append(_bytearray)
            _bytearray = bytearray()
        counter += 1
    if len(_bytearray) > 0:
        out_list.append(_bytearray)
    out_tuple = tuple(out_list)
    return out_tuple


def bytearray_splitter_by_pieces(in_bytes, pieces=8):
    counter = 1
    out_list = []
    _bytearray = bytearray()
    split_by = sys.getsizeof(in_bytes) // pieces
    for i in in_bytes:
        _bytearray.append(i)
        counter += 1
        if counter % split_by == 0:
            out_list.append(_bytearray)
            _bytearray = bytearray()
    out_tuple = tuple(out_list)
    return out_tuple


def bytearray_joiner(sequence_of_bytearrays):
    _bytearray = bytearray()
    for item in sequence_of_bytearrays:
        try:
            rs_decoded_item = rs_decode(item)
        except:
            rs_decoded_item = item
        for item_byte in item:
            _bytearray.append(item_byte)
    _bytes = bytes(_bytearray)
    return _bytes


################################

# Reed Solomon
def rs_encode(data, checksum_bytes=4):
    rs_codec = RSCodec(checksum_bytes)
    rs_data = rs_codec.encode(data)
    return rs_data


def rs_decode(data, checksum_bytes=4):
    rs_codec = RSCodec(checksum_bytes)
    rs_data = rs_codec.decode(data)[0]
    return rs_data


##########################


## Redis ##########################

def redis_connect_server(ip="localhost", port=6379, password="password", db=0):
    try:
        redis_server = redis.Redis(
            host=ip,
            port=port,
            password=password,
            db=db)
    except Exception as e:
        print("Redis Error: ", e)
        redis_server = None
    return redis_server


def redis_add_item_to_list(redis_server, redis_key, item):
    try:
        count = redis_server.rpush(redis_key, item)
    except Exception as e:
        print("Redis add item Error: ", e)
        exit(1)
    return count


def redis_check_connected(redis_server, daemon=True):
    try:
        redis_server.set("connection_test", "connected")
        if redis_server.get("mqtt_test") == "connected".encode():
            if not daemon:
                print("[*] Connected to Redis.")
            return True
    except ConnectionError:
        if not daemon:
            print(f"[X] Error: Not connected to redis-server.")
        return False
    except redis.exceptions.ConnectionError:
        if not daemon:
            print(f"[X] Error: Can't connect to redis-server.")
        return False
    except Exception as e:
        if not daemon:
            print("[x] Redis not connected, ", e)
        return False


def redis_get_length_of_list(redis_server, list_name):
    try:
        redis_length = len(redis_server.lrange(list_name, 0, -1))
    except Exception as e:
        print(e)
        exit(1)
    return redis_length


#####################


def md5sum_bytestring(bytestring):
    byte_md5 = md5(bytestring)
    md5sum = byte_md5.hexdigest()
    return md5sum

