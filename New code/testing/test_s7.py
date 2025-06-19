import time
import snap7
import struct

client = snap7.client.Client()

s7_ip = "10.120.70.50"


def connect():
    global client
    try:
        client.connect(s7_ip, 0, 1)  # 2 for s7-300 # first 0 for port number(0 arrg  automatically assign
        # an available port), second 0 for timeout value.
        if client.get_connected():
            print("Client Connected!")
            return client
        else:
            print("No Communication from the client.")
    except Exception as e:
        print(f"ERROR initiating {e}")
    return None


def bytearray_to_bool_list(byte_array):
    bool_list = []
    for byte in byte_array:
        for _ in range(8):
            bool_list.append(bool(byte & 1))
            byte >>= 1
    return bool_list


def read_s7_data():
    client = connect()
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

            print(f'S7-data: {data}')

            client.disconnect()
            return data
        except Exception as e:
            print(f'error in read plc{e}')
            client.disconnect()
            return None
    else:
        return None


if __name__ == '__main__':
    while True:
        read_s7_data()
        time.sleep(2)
