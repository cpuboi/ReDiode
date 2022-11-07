
import time
from tools import diode_utils

def test_send_data(max_count=10_000):
    redis_server = diode_utils.redis_connect_server()
    while max_count > 0:
        diode_utils.redis_add_item_to_list(redis_server, "diode_out", time.ctime())
        max_count -= 1



