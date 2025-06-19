import time
import snap7
import struct
from snap7.util import get_bool
from snap7.types import Areas

client = snap7.client.Client()
s7_ip = "192.3.2.15"


def connect():
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


def read_s7_data():
    client = connect()
    if client is not None:
        try:
            data = []

            # Read 32-bit integers from DB101 at offsets 56 and 60
            for off in [56, 60]:
                buf = client.read_area(Areas.DB, 1110, off, 4)
                data.append(struct.unpack(">i", buf)[0])  # big-endian signed int

            # Read output bits Q5.2, Q5.3, Q5.4
            output_byte = client.read_area(Areas.PA, 0, 5, 1)  # Q5.x → byte 5 from Process Outputs
            green = get_bool(output_byte, 0, 2)  # Q5.2
            yellow = get_bool(output_byte, 0, 3)  # Q5.3
            red = get_bool(output_byte, 0, 4)  # Q5.4

            data += [green, yellow, red]

            print(f'S7-data: {data}')
            client.disconnect()
            return data
        except Exception as e:
            print(f'Error reading PLC: {e}')
            client.disconnect()
            return None
    else:
        return None


if __name__ == '__main__':
    while True:
        read_s7_data()
        time.sleep(0.5)

