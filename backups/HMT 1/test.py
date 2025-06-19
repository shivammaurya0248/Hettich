import snap7
import time
import struct
from logger import log

# Gateway ip is 192.168.1.154

client = snap7.client.Client()
# c = DBHelper()
parameterName = [
    'shift_ok',
    'shift_Not_ok',
    'L1',
    'L2'
    'L3',
    'L4',
    'L5',
    'L6',
    'L7',
    'L8',
    'L9',
    'L10',
    'L11',
    'L12',
    'L13',
    'L14'
]


def initiate():
    global client
    try:
        client.connect("10.120.70.150", 0, 1)  # 2 for s7-300 # first 0 for port number(0 arrg  automatically assign
        # an available port), second 0 for timeout value.
        if client.get_connected():
            log.info("Client Connected!")
            return client
        else:
            log.info("No Communication from the client.")
    except Exception as e:
        log.error(f"ERROR initiating {e}")
    return None


def bytearray_to_bool_list(byte_array):
    bool_list = []
    for byte in byte_array:
        for _ in range(8):
            bool_list.append(bool(byte & 1))
            byte >>= 1
    return bool_list


def read_s7_data():
    #global client, parameterName
    #while True:
    #client = snap7.client.Client()
    client = initiate()
    if client is not None:
        try:
            data = list()
            buffer1 = client.read_area(snap7.types.Areas.DB, 400, 4, 4)
            buffer1 = struct.unpack(">I", buffer1)[0]

            buffer2 = client.read_area(snap7.types.Areas.DB, 400, 8, 4)
            buffer2 = struct.unpack(">I", buffer2)[0]

            buffer3 = client.read_area(snap7.types.Areas.DB, 400, 0, 8)

            buffer3 = bytearray_to_bool_list(buffer3)

            buffer4 = client.read_area(snap7.types.Areas.DB, 400, 2, 8)
            buffer4 = bytearray_to_bool_list(buffer4)

            data.append(buffer1)
            data.append(buffer2)
            data = data + list(buffer3[:3])

            client.disconnect()
            return data
        except Exception as e:
            log.error(f'error in read plc{e}')
            client.disconnect()
            return None
    else:
        return None
