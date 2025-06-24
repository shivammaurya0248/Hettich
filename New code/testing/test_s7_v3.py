import time
import snap7
import struct
from snap7.util import get_bool
from snap7.types import Areas

client = snap7.client.Client()
# s7_ip = "192.3.1.2"
ip_list = ["192.3.1.2", "192.3.1.7"]




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


from snap7.types import Areas


def read_booleans(client, area, db_number=0, start_address=0, bit_positions=None, byte_count=1):
    """
    Read booleans from PLC memory areas.

    Args:
        client: snap7 client
        area: 'DB', 'PA', 'PE', 'MK'
        db_number: DB number (for DB area only)
        start_address: starting byte address
        bit_positions: list of bit positions [0-7], None for all bits
        byte_count: number of bytes to read
    """
    areas = {'DB': Areas.DB, 'PA': Areas.PA, 'PE': Areas.PE, 'MK': Areas.MK}

    try:
        data = client.read_area(areas[area], db_number if area == 'DB' else 0, start_address, byte_count)
        result = []

        for i in range(byte_count):
            bit_list = bit_positions if bit_positions else range(8)
            result.extend([bool(data[i] & (1 << b)) for b in bit_list])

        return result
    except Exception as e:
        print(f"Error reading {area}: {e}")
        return []


# Usage examples:
# read_booleans(client, 'DB', db_number=400, start_address=0, bit_positions=[0,1,2])     # DB400 byte 0, bits 0,1,2
# read_booleans(client, 'PA', start_address=5, bit_positions=[0,1,2])                    # PA byte 5, bits 0,1,2
# read_booleans(client, 'PE', start_address=10)

def read_boolean() -> list:
    try:
        buffer3 = client.read_area(snap7.types.Areas.DB, 400, 0, 8)
        buffer3 = bytearray_to_bool_list(buffer3)
        buffer3 = list(buffer3[:3])
        print(f'boolean_list: {buffer3}')
    except Exception as e:
        print(f"[!] Error reading boolean values: {e}")
        return []


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
                if ip == "10.120.70.50" or ip == "10.120.70.150":
                    int_data = read_integer_values(client, 400, [4, 8])
                    part_count, reject_count = int_data
                    print(f'part_count: {part_count}, reject_count: {reject_count}')
                    read_boolean()
                    read_booleans(client, 'DB', 400, 0, [0, 1, 2])

                else:
                    int_data = read_integer_values(client, 101, [56, 60])
                    part_count, reject_count = int_data

                    print(f'part_count: {part_count}, reject_count: {reject_count}')
                    read_output_booleans(client, 5, [0, 1, 7])

                # hmt_snap7()
                client.disconnect()
            except Exception as e:
                print(f'Error reading PLC: {e}')
                client.disconnect()
                return None
        print(f' ')
        time.sleep(1)


if __name__ == '__main__':
    while True:
        read_s7_data()
        time.sleep(0.5)
