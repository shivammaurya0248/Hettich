from pyModbusTCP.client import ModbusClient
import time
import requests
from logger import log

IP_ADDRESS = '192.168.0.190'
# gateway ip : 192.168.0.192
PORT = 502


def conn():
    try:
        client = ModbusClient(host=IP_ADDRESS, port=PORT, unit_id=1, auto_open=True, auto_close=True, timeout=2)
        log.info(f"")
        log.info(f"connected with plc")
        return client
    except Exception as e:
        log.error(f"Error: {e}")
        return None


def reset_plc_counter():
    mb_client = conn()
    if mb_client:
        try:
            mb_client.write_single_register(4, 28000)
            log.info(f"Plc counter reset done")
        except Exception as e:
            print(f"Error while writing reject count {e}")


def read_plc():
    mb_client = conn()
    if mb_client:
        try:
            count_data = mb_client.read_holding_registers(4096, 2)
            status_data = mb_client.read_discrete_inputs(1026, 3)
            # log.info(f"count_data: {count_data} || status_data: {status_data}")
            if status_data:
                return count_data + status_data
            else:
                return [None, None, None, None, None]
        except Exception as msg:
            log.error(f"Error: {msg}")
            return [None, None, None, None, None]
    return None

# def reset_plc_counter():
#     mb_client = conn()
#     if mb_client:
#         try:
#             mb_client.write_multiple_registers(4096, [0, 0])
#         except Exception as e:
#             log.error(f"Error: {e}")
