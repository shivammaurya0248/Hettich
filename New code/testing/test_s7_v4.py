import time
import snap7
import struct
from snap7.util import get_bool

client = snap7.client.Client()


machine_master_dict = {
    "Cosberg Assy-01": {
        "plc_ip": "192.3.1.7",
        "area": "PA",
        "int_db_number": 101,
        "bool_db_number": 0,
        "bool_start_address": 5,
        "part_count": 56,
        "reject_count": 60,
        "red_light": 2,
        "yellow_light": 3,
        "green_light": 4
    },
    "Cosberg Assy-02": {
        "plc_ip": "192.3.1.2",
        "area": "PA",
        "int_db_number": 101,
        "bool_db_number": 0,
        "bool_start_address": 5,
        "part_count": 56,
        "reject_count": 60,
        "red_light": 2,
        "yellow_light": 3,
        "green_light": 4
    },
    "Cosberg Assy-03": {
        "plc_ip": "192.3.1.15",
        "area": "PA",
        "int_db_number": 1110,
        "bool_db_number": 0,
        "bool_start_address": 5,
        "part_count": 56,
        "reject_count": 60,
        "red_light": 0,
        "yellow_light": 1,
        "green_light": 7  # If any missing, use None
    },
    "HMT Assy-1": {
        "plc_ip": "10.120.70.50",
        "area": "DB",
        "int_db_number": 400,
        "bool_db_number": 400,
        "bool_start_address": 0,
        "part_count": 4,
        "reject_count": 8,
        "red_light": 0,
        "yellow_light": 1,
        "green_light": 2
    },
    "HMT Assy-2": {
        "plc_ip": "10.120.70.150",
        "area": "DB",
        "int_db_number": 400,
        "bool_db_number": 400,
        "bool_start_address": 0,
        "part_count": 4,
        "reject_count": 8,
        "red_light": 0,
        "yellow_light": 1,
        "green_light": 2
    }
}

class CL_SNAP7:
    def __init__(self, plc_ip):

        self.plc_ip = plc_ip

    def is_connected(self) -> bool:
        try:
            client = snap7.client.Client()
            for i in range(3):
                client.connect(self.plc_ip, 0, 1)
                # buf = client.read_area(Areas.DB, 101, 56, 4)
                # if buf is not None:
                #     print(f"PLC [{self.plc_ip}] is connected")
                return True
            print(f"Failed to connect to PLC [{self.plc_ip}]. Connection attempt unsuccessful.")
            return False
        except Exception as e:
            print(f"[!] Can't connect to the machine {e}")
            return False

    def read_integer(self, db_num: int, offsets: list) -> list:
        try:
            client = snap7.client.Client()
            client.connect(self.plc_ip, 0, 1)
            data = []
            for off in offsets:
                buf = client.read_area(Areas.DB, db_num, off, 4)
                data.append(struct.unpack(">i", buf)[0])
            print(f"integer_data: {data}")
            return data
        except Exception as e:
            print(f"[!] Error reading integer values: {e}")
            return []

    def read_booleans(self, area, db_number=0, start_address=0, bit_positions=None, byte_count=1):
        """
        Read booleans from PLC memory areas.

        Args:
            area: 'DB', 'PA', 'PE', 'MK'
            db_number: DB number (for DB area only)
            start_address: starting byte address
            bit_positions: list of bit positions [0-7], None for all bits
            byte_count: number of bytes to read
        """

        areas = {'DB': Areas.DB, 'PA': Areas.PA, 'PE': Areas.PE, 'MK': Areas.MK}
        try:
            client = snap7.client.Client()
            client.connect(self.plc_ip, 0, 1)
            data = client.read_area(areas[area], db_number if area == 'DB' else 0, start_address, byte_count)
            result = []

            for i in range(byte_count):
                bit_list = bit_positions if bit_positions else range(8)
                result.extend([bool(data[i] & (1 << b)) for b in bit_list])

            print(f'boolean_list: {result}')

            return result
        except Exception as e:
            print(f"Error reading {area}: {e}")
            return []


def main(m_name):

    plc_ip = machine_master_dict[m_name]['plc_ip']
    db_num = machine_master_dict[m_name]['int_db_number']
    pc_addr = machine_master_dict[m_name]['part_count']
    rc_addr = machine_master_dict[m_name]['reject_count']
    offsets = [pc_addr, rc_addr]

    area = machine_master_dict[m_name]['area']
    bool_db_number = machine_master_dict[m_name]['bool_db_number']
    bool_start_address = machine_master_dict[m_name]['bool_start_address']
    red_light = machine_master_dict[m_name]['red_light']
    yellow_light = machine_master_dict[m_name]['yellow_light']
    green_light = machine_master_dict[m_name]['green_light']
    bool_offsets = [red_light, yellow_light, green_light]


    obj_plc = CL_SNAP7(plc_ip)

    connected = obj_plc.is_connected()
    print(f'{m_name} is connected({connected}) with {plc_ip}')
    print(' ')

    obj_plc.read_integer(db_num, offsets)
    obj_plc.read_booleans(area, bool_db_number, bool_start_address, bool_offsets)




if __name__ == '__main__':
    while True:
        m_list = ['Cosberg Assy-01', 'Cosberg Assy-02', 'Cosberg Assy-03', 'HMT Assy-1', 'HMT Assy-2']

        for m_name_ in m_list:

            main(m_name_)
            print('-' * 60)
            print(' ')
            time.sleep(1)

        time.sleep(3)
        print('=' * 60)
        print(' ')
