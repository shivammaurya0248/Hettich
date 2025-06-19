from pyModbusTCP.client import ModbusClient
import time

# PLC_IPS = ['192.168.0.190', '192.168.0.7', '192.168.0.5']
PLC_IPS = ['192.168.0.190']

machine_master = {
    'mac_1': {"ip": '192.168.0.190', "pc_addr": 4114, "rc_addr": 4112},
    'mac_2': {"ip": '192.168.0.7', "pc_addr": 4114, "rc_addr": 4112},
    'mac_3': {"ip": '192.168.0.5', "pc_addr": 4114, "rc_addr": 4112},
}

while True:
    try:
        mc_list = ['mac_1', 'mac_2', 'mac_3']

        for mc_name in mc_list:
            unit_id = 1
            ip = machine_master[mc_name].get('ip')
            client = ModbusClient(ip, port=502, unit_id=unit_id, auto_open=True, auto_close=True, timeout=5)

            print(f"Connected with {mc_name}, [{ip}]")

            data_list = client.read_holding_registers(4112, 5)
            part_count = client.read_holding_registers(machine_master[mc_name].get('pc_addr'), 2)[0]
            reject_count = client.read_holding_registers(machine_master[mc_name].get('rc_addr'), 2)[0]
            status = client.read_discrete_inputs(2050, 5)

            print(f'data_list: {data_list}')
            print(f'part_count: {part_count}')
            print(f'reject_count: {reject_count}')
            print(f'status: {status}')

            # reset_counter = client.write_single_register(4100, 28000)

            # pc_value = int(input('Enter the part count value: '))
            # reset_counter = client.write_single_register(machine_master[mc_name].get('pc_addr'), pc_value)
            # print(reset_counter)
            #
            # rc_value = int(input('Enter the reject value: '))
            # reset_counter = client.write_single_register(machine_master[mc_name].get('rc_addr'), rc_value)
            # print(reset_counter)

            time.sleep(3)

        print("-" * 90)
        print(" ")

    except Exception as e:
        print(f"Error: {e}")

