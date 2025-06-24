from pyModbusTCP.client import ModbusClient
import time

# PLC_IPS = ['192.168.0.190', '192.168.0.7', '192.168.0.5']
PLC_IPS = ['192.168.0.190']
while True:
    try:
        for plc_ip in PLC_IPS:
            unit_id = 1
            client = ModbusClient(plc_ip, port=502, unit_id=unit_id, auto_open=True, auto_close=True, timeout=5)

            print('Connected !')

            data_list = client.read_holding_registers(4100, 10)  # rej = 4112, part = 4114, reset pc = 4100 with 28000
            data_list2 = client.read_discrete_inputs(2050, 5)

            print(f"Holding reg  from: {plc_ip}: {data_list}")
            print(f"Discrete reg from: {plc_ip}: {data_list2}")
            print(" ")

            reset_counter = client.write_single_register(4100, 28000)
            print(reset_counter)
            # reset_counter = client.write_single_register(4113, 0)
            # print(reset_counter)
            # reset_counter = client.write_single_register(4114, 0)
            # print(reset_counter)

            time.sleep(1)

        print(" ")
        print(" ")
    except Exception as e:
        print(f"Unknown error: {e}")
