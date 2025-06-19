import time
import snap7
import struct
from snap7.util import get_bool
from snap7.types import Areas

client = snap7.client.Client()
# s7_ip = "192.3.1.2"
ip_list = ["10.120.70.50", "10.120.70.150"]


def connect(s7_ip):
    global client
    try:
        client.connect(s7_ip, 0, 1)
        if client.get_connected():
            print("Client Connected!")
            return client
        else:
            print("No Communication from the client.")
    except Exception as e:
        print(f"ERROR initiating {e}")
    return None


def read_integer_values(client, db_num: int, offsets: list) -> list:
    try:
        data = []
        for off in offsets:
            buf = client.read_area(Areas.DB, db_num, off, 4)
            data.append(struct.unpack(">i", buf)[0])
        print(f"integer_data: {data}")
        return data
    except Exception as e:
        print(f"[!] Error reading integer values: {e}")
        return []


def read_output_booleans(client, output_byte_addr: int, bit_positions: list) -> list:
    try:
        byte_data = client.read_area(Areas.PA, 0, output_byte_addr, 1)
        data = [get_bool(byte_data, 0, bit) for bit in bit_positions]
        print(f"booleans: {data}")
        return data
    except Exception as e:
        print(f"[!] Error reading boolean outputs: {e}")
        return [False] * len(bit_positions)


def bytearray_to_bool_list(byte_array):
    bool_list = []
    for byte in byte_array:
        for _ in range(8):
            bool_list.append(bool(byte & 1))
            byte >>= 1
    return bool_list


def hmt_snap7():
    try:
        data = list()
        buffer1 = client.read_area(snap7.types.Areas.DB, 400, 4, 4)
        buffer1 = struct.unpack(">I", buffer1)[0]

        buffer2 = client.read_area(snap7.types.Areas.DB, 400, 8, 4)
        buffer2 = struct.unpack(">I", buffer2)[0]

        buffer3 = client.read_area(snap7.types.Areas.DB, 400, 0, 8)

        buffer3 = bytearray_to_bool_list(buffer3)
        buffer3 = list(buffer3[:3])
        data.append(buffer1)
        data.append(buffer2)

        print(f'int data: {data}')
        print(f'bool data: {buffer3}')

    except Exception as e:
        print(f"[!] Error reading boolean outputs: {e}")


def read_s7_data():
    for ip in ip_list:
        print(f'Connected to {ip}')
        client = connect(ip)
        if client is not None:
            try:
                # int_data = read_integer_values(client, 101, [56, 60])
                # part_count, reject_count = int_data

                # print(f'part_count: {part_count}, reject_count: {reject_count}')
                # read_output_booleans(client, 5, [2, 3, 4])

                hmt_snap7()
                client.disconnect()
            except Exception as e:
                print(f'Error reading PLC: {e}')
                client.disconnect()
                return None
        print(f' ')
        time.sleep(5)


if __name__ == '__main__':
    while True:
        read_s7_data()
        time.sleep(0.5)
