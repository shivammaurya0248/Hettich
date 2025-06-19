import json
import sys
import requests
from logger import log
from opc_client import cl_opc_client
from pyModbusTCP.client import ModbusClient
import time
from database import DBHelper
from shift import get_shift, shift_a_start, shift_b_start, shift_c_start, get_current_total_time, break_check
from datetime import datetime, timedelta
import schedule
import os


def manage_ip_config(default_machine_name=None):
    try:
        file_path = "machine_config"

        if not os.path.exists(file_path):
            with open(file_path, "w") as file:
                file.write(f"Machine_NAME={default_machine_name}\n")
            log.info(f"File '{file_path}' created with default configurations.")
            return default_machine_name
        else:
            config = {}
            with open(file_path, "r") as file:
                for line_ in file:
                    key, value = line_.strip().split("=", 1)
                    config[key] = value if value != "None" else None

            machine_name = config.get("Machine_NAME", default_machine_name)
            log.info(f"Configurations from file: Machine_NAME={machine_name}")
            return machine_name
    except Exception as e:
        log.error(f"Error: {e}, while managing HVIR configuration file.")
        return None


config_machine_name = manage_ip_config()
MACHINE_NAME = config_machine_name
log.info(f"config_machine_name: {config_machine_name}\n")

machine_details = {
    'Cosberg Assy-1': {
        "opc_server_ip": "192.3.1.7",
        "modbus_ip": "192.168.0.190",
        "access_token": "p0mmGwZoiwwsb3awMJt6",
        "connected_to_gw": "192.3.1.90",
        "opc_node_ids": {
            "ok_count": 'ns=3;s="COP_DB_L"."Totali"."Pezzi_Ok"',
            "ko_count": 'ns=3;s="COP_DB_L"."Totali"."Pezzi_Ko"',
            # "red_light": 'ns=3;s="ledRed"',
            # "yellow_light": 'ns=3;s="ledYellow"',
            # "green_light": 'ns=3;s="ledGreen"',
        }
    },

    'Cosberg Assy-2': {
        "opc_server_ip": "192.3.1.2",
        "modbus_ip": "192.168.0.7",
        "access_token": "F5yOzQNQJ8EnDGiWqSOF",
        "connected_to_gw": "192.3.1.90",
        "opc_node_ids": {
            "ok_count": 'ns=3;s="COP_DB_L"."Totali"."Pezzi_Ok"',
            "ko_count": 'ns=3;s="COP_DB_L"."Totali"."Pezzi_Ko"',
            # "red_light": 'ns=3;s="ledRed"',
            # "yellow_light": 'ns=3;s="ledYellow"',
            # "green_light": 'ns=3;s="ledGreen"',
        }
    },

    'Cosberg Assy-3': {
        "opc_server_ip": "192.3.1.15",
        "access_token": "e7B7vdMQACbxM7ImmCFi",
        "connected_to_gw": "192.3.1.90",
        "opc_node_ids": {
            "ok_count": 'ns=3;s="COP_DB_L"."Totali"."Pezzi_Ok"',
            "ko_count": 'ns=3;s="COP_DB_L"."Totali"."Pezzi_Ko"',
            "red_light": None,
            "yellow_light": None,
            "green_light": None,
        }
    },
}

machine_opc_ip = machine_details[config_machine_name]["opc_server_ip"]
access_token = machine_details[config_machine_name]["access_token"]

log.info(f"machine: {config_machine_name}, machine_opc_ip: {machine_opc_ip}, access_token: {access_token}\n")

# opc_url = f'opc.tcp://{machine_opc_ip}:4840'
# log.info(f"OPC URL: {opc_url} for machine: {config_machine_name}\n")
# node_ids = machine_details[config_machine_name]["opc_node_ids"]
# obj_opc_client = cl_opc_client(opc_url, node_ids)

URL_ATTR = f'https://ithingspro.cloud/api/v1/{access_token}/attributes'
URL_TELE = f'https://ithingspro.cloud/api/v1/{access_token}/telemetry'
API = "https://ithingspro.cloud/Hettich/create_shift_data/"
URL_GENERAL = 'https://ithingspro.cloud/Hettich/check_generalshift_status'

SEND_DATA = True

GL_breakdown_start = 0
GL_breakdown_stop = 0
MACHINE_STATUS = {
    "ready": None,
    "stop": None,
    "healthy": None
}

FL_RESET = False
First_time = True
GL_PREVIOUS_SHIFT = ""

GL_TOTAL_COUNT = 0
GL_TOTAL_REJECT_COUNT = 0
GL_CURR_PART_COUNT = 0
GL_CURR_REJECT_COUNT = 0

GL_PREV_PART_COUNT = 0
GL_PREV_REJECT_COUNT = 0
start_duration = 0

GL_PREV_READY_STATUS = True
CONSTANT_YELLOW = True  # it is used to run only when ready is constant yellow
GL_BREAKDOWN_STATUS = False
PREVIOUS_CONSTANT_YELLOW_STAT = False
GL_PREV_HEALTHY_STATUS = False
GL_PREV_STOP_STATUS = False

GL_HEALTHY_START_TIME = sys.maxsize
GL_BLINKING_START_TIME = sys.maxsize
GL_STOP_START_TIME = sys.maxsize
GL_READY_START_TIME = sys.maxsize
db = DBHelper()

breakdown_status = False
prev_breakdown_status = False
FL_LAST_SENT = time.time()

today = ""
shift = ""
alert = sys.maxsize
GENERAL_SHIFT_STATUS = False
HEADERS = {'content-type': 'application/json'}


def modbus_read():
    try:
        plc_ip = machine_details[config_machine_name]["modbus_ip"]
        client = ModbusClient(plc_ip, port=502, unit_id=1, auto_open=True, auto_close=True, timeout=5)
        lights_status_list = client.read_discrete_inputs(2050, 4)
        log.info(f'lights_status_list: {lights_status_list}')
        red_light_status = lights_status_list[0]
        yellow_light_status = lights_status_list[1]
        green_light_status = lights_status_list[2]
        return green_light_status, yellow_light_status, red_light_status
    except Exception as e:
        log.error(f"Error: {e}, error in modbus read function")
        return None, None, None


def reset_counter():
    global GL_HEALTHY_START_TIME, GL_STOP_START_TIME, GL_READY_START_TIME, GL_PREV_HEALTHY_STATUS, GL_PREV_STOP_STATUS, PREVIOUS_CONSTANT_YELLOW_STAT
    GL_HEALTHY_START_TIME = sys.maxsize
    GL_STOP_START_TIME = sys.maxsize
    GL_READY_START_TIME = sys.maxsize
    GL_PREV_HEALTHY_STATUS = False
    GL_PREV_STOP_STATUS = False
    PREVIOUS_CONSTANT_YELLOW_STAT = False


def assign_data(data):
    try:
        global GL_CURR_PART_COUNT, GL_CURR_REJECT_COUNT, MACHINE_STATUS
        if data[0] > 65000:
            GL_CURR_PART_COUNT = 0
        else:
            GL_CURR_PART_COUNT = data[0]
        GL_CURR_REJECT_COUNT = data[1]

        # stop_status = data[2]
        # if stop_status is False:
        #     stop_status = 0
        # elif stop_status is True:
        #     stop_status = 1
        #
        # ready_status = data[2]
        # if ready_status is False:
        #     ready_status = 0
        # elif ready_status is True:
        #     ready_status = 1
        #
        # healthy_status = data[2]
        # if healthy_status is False:
        #     healthy_status = 0
        # elif healthy_status is True:
        #     healthy_status = 1
        status_data = modbus_read()

        if status_data is not None:
            healthy_status, ready_status, stop_status = status_data
        else:
            healthy_status, ready_status, stop_status = False, False, False

        MACHINE_STATUS['stop'] = stop_status
        MACHINE_STATUS['ready'] = ready_status
        MACHINE_STATUS['healthy'] = healthy_status
    except Exception as e:
        log.error(f"Error: {e}")


def send_data(t, s):
    try:
        data = db.fetch_data(t, s)
        if data:
            payload = {
                "date": t,
                "shift": s,
                "part_count": data['part_count'],
                "reject_count": data['reject_count']
            }
            log.info(f"Shift data : {payload}")
            response = requests.post(URL_TELE, json=payload, timeout=3)
            response.raise_for_status()
            log.info(f"Production data sent (status:{response.status_code})")
    except Exception as e:
        log.error(f"Error: {e}")


def oee_calculations():
    """oee calculations"""
    global SHIFT_A_TOTAL_PARTS, SHIFT_B_TOTAL_PARTS, SHIFT_C_TOTAL_PARTS, SHIFT_A_CYCLE_PARTS, SHIFT_B_CYCLE_PARTS, SHIFT_C_CYCLE_PARTS
    try:
        current_date, current_shift = db.get_misc_data()
        data = db.fetch_data(current_date, current_shift)

        if data['planned_time'] is not None:
            # for k, v in data.items():
            #     if v is None:
            #         data[k] = 0
            operating_time = data['healthy_time']
            planned_time = data["planned_time"] - data["stop_time"]
            if planned_time < 0:
                planned_time = 0
            actual_production = data["part_count"] + data["reject_count"]
            max_possible_production = int((data["healthy_time"] + data["ready_time"]) * 55)
            availability_planned_time = data['healthy_time'] + data["stop_time"] + data["ready_time"]

            try:
                availability = (operating_time + data["ready_time"]) / availability_planned_time
                availability_percent = round(availability * 100, 2)
            except Exception as e:
                log.error(f"Error: {e}")
                availability = 0
                availability_percent = 0

            # performance
            try:
                performance = (1.2 * actual_production) / ((operating_time + data["ready_time"]) * 60)
                # log.info(f"Performance is {performance}")
                # performance = actual_production / max_possible_production
            except Exception as e:
                log.error(f"Error: {e}")
                performance = 0

            # utilization
            try:
                machine_util = round(performance * 100, 2)
            except Exception as e:
                log.error(f"Error while calculating machine utilization : {e}")
                machine_util = 0

            try:
                quality = data['part_count'] / actual_production
                quality_per = round(quality * 100, 2)
            except Exception as e:
                log.error(f"Error : {e}")
                quality_per = 0
                quality = 0

            # OEE
            try:
                oee = availability_percent * machine_util * quality_per
                oee = round(oee / 10000, 2)
            except Exception as e:
                log.error(f"Error:  {e}")
                oee = 0

            payload_api = data
            payload_api["performance"] = machine_util
            payload_api["availability"] = availability_percent
            payload_api["quality"] = quality_per
            payload_api['oee'] = oee

            loss_time_ = data["stop_time"] + data["ready_time"]
            log.info(f'loss_time for telemetry: {loss_time_}')

            try:
                sync_data = db.get_sync_data()
                if sync_data:
                    for i in sync_data:
                        payload = json.loads(i[0])
                        log.info(f'payload: {payload}')
                        sync_response = requests.post(API, json=payload, timeout=2)
                        sync_response.raise_for_status()
                        log.info(f"Sync data sent : {sync_response.status_code}")
                    db.delete_sync_data()
                    log.info(f'Sync table data deleted')
                else:
                    log.info("Sync data is not available")

                log.info(f"sending production data to API")
                log.info(f"API_payload: {payload_api}")
                response = requests.post(API, json=payload_api, timeout=2)
                response.raise_for_status()
            except Exception as e:
                log.error(f"Error: {e}")
                db.add_sync_data(payload_api)

            try:
                payload = {
                    "day_Oee": oee,
                    "day_quality": quality_per,
                    "machine_util": machine_util,
                    "availability_percent": availability_percent,
                    "loss_time": loss_time_
                }
                log.info(f"sending production data to telemetry")
                log.info(f"Tele_payload: {payload_api}")
                response = requests.post(URL_TELE, json=payload, timeout=2)
                response.raise_for_status()
                log.info(f"Shift Oee sent to telemetry: {response.status_code}")
            except Exception as e:
                log.error(f"Error in sending shift Oee : {e}")
        else:
            log.info(f"Planned time is not available")
    except Exception as e:
        log.error(f"Error: {e}")


def whats_app_status(status):
    payload = {
        "whats_app_status": status
    }
    try:
        response = requests.post(URL_ATTR, json=payload, timeout=3)
        response.raise_for_status()
        log.info(f"Whats app status: {status}: {response.status_code}")
    except Exception as e:
        log.error(f"Error in whats_app_status : {e}")


def send_alarm_status(status):
    payload = {
        "alarm_status": status
    }
    try:
        response = requests.post(URL_ATTR, json=payload, timeout=3)
        response.raise_for_status()
        log.info(f"Alarm generated {status}: {response.status_code}")
    except Exception as e:
        log.error(f"Error in sending alarm : {e}")


def reset_values():
    payload = {
        'stop_status': 0,
        'ready_status': 0,
        'healthy': 0,
        "A_cycle_parts": 0,
        "A_real_parts": 0,
        "B_cycle_parts": 0,
        "B_real_parts": 0,
        "C_cycle_parts": 0,
        "C_real_parts": 0,
        "G_real_parts": 0,
        "G_cycle_parts": 0,
        "part_count": 0,
        "reject_count": 0,
        "day_up_time": 0,
        "healthy_time": 0,
        "loss_time": 0
    }
    try:
        response = requests.post(URL_ATTR, json=payload, timeout=3)
        response.raise_for_status()
        log.info(f"Dashboard reset : {response.status_code}")
    except Exception as e:
        log.error(f"Error in sending reset data to attributes {e}")


def reset_oee():
    try:
        payload = {
            "day_Oee": 0,
            "day_quality": 0,
        }
        response = requests.post(URL_TELE, json=payload, timeout=3)
        response.raise_for_status()
        log.info(f"OEE RESET: {response.status_code}")
    except Exception as e:
        log.error(f"Error while resetting oee: {e}")


def send_data_to_attributes():
    global MACHINE_STATUS, today
    current_date, current_shift = db.get_misc_data()
    # send_data(current_date, current_shift)
    # data_attr = db.fetch_data(current_date, current_shift)
    payload = {
        'stop_status': 0,
        'ready_status': 0,
        'healthy': 0,
        "A_cycle_parts": 0,
        "A_real_parts": 0,
        "B_cycle_parts": 0,
        "B_real_parts": 0,
        "C_cycle_parts": 0,
        "C_real_parts": 0,
        "G_real_parts": 0,
        "G_cycle_parts": 0,
        # "day_count": 0,
        # "day_reject": 0,
        "day_up_time": 0,
    }

    try:
        data_attr = db.fetch_data(current_date, current_shift)
        payload = data_attr
        # current_date, current_shift = db.get_misc_data()
        # payload = db.fetch_data(current_date, current_shift)
        payload['stop_status'] = MACHINE_STATUS['stop']
        payload['ready_status'] = MACHINE_STATUS['ready']
        payload['healthy'] = MACHINE_STATUS['healthy']
        payload['loss_time'] = data_attr["stop_time"] + data_attr["ready_time"]

    except Exception as e:
        log.error(f"Error:{e}")

    try:
        # current_date, current_shift = db.get_misc_data()
        shift_data = db.get_shift_data(current_date, current_shift)
        payload["A_cycle_parts"] = shift_data["A_cycle_time_parts"]
        payload["A_real_parts"] = shift_data["A_real_time_parts"]
        payload["B_cycle_parts"] = shift_data["B_cycle_time_parts"]
        payload["B_real_parts"] = shift_data["B_real_time_parts"]
        payload["C_cycle_parts"] = shift_data["C_cycle_time_parts"]
        payload["C_real_parts"] = shift_data["C_real_time_parts"]
        payload["G_real_parts"] = shift_data["G_real_time_parts"]
        payload["G_cycle_parts"] = shift_data["G_cycle_time_parts"]
    except Exception as e:
        log.error(f"Error: {e}")

    try:
        prod_data = db.get_day_production(today)
        operating_time = prod_data['total_healthy']
        # payload["day_count"] = prod_data["total_part_count"]
        # payload["day_reject"] = prod_data["total_reject_count"]
        payload["day_up_time"] = round(operating_time, 2)
    except Exception as e:
        log.error(f"Error : {e}")

    log.info(f"Payload {payload}")

    try:
        response = requests.post(URL_ATTR, json=payload, timeout=2)
        response.raise_for_status()
        log.info(f"Sending data to attribute: {response.status_code}")
    except Exception as e:
        log.error(f"Error in sending data to attri {e}")


last_sent_oee_payload = 0


def fetch_machine_maintenance_status(sys_date, curr_shift):
    try:
        log.info('Getting Machine Maintenance from server')
        url = f'https://ithingspro.cloud/Hettich/check_maintenance_status/{sys_date}/{curr_shift}/{MACHINE_NAME}/'
        log.info(f"url:{url}")
        req = requests.get(url, headers=HEADERS, timeout=5)
        status = req.json()
        log.info(f"fetched Maintenance status:{status}")
        return status
    except requests.Timeout:
        log.error("[!!] Error: Timeout occurred while fetching the Maintenance status.")
        return None
    except requests.RequestException as e:
        log.error(f"[!!] Error: {e}, unable to fetch the Maintenance status.")
        return None


def post_breakdowns() -> None:
    url = f'https://ithingspro.cloud/api/v1/{access_token}/telemetry'
    log.info(f"(>>>)Sending Breakdown status on {url}")

    payload = {"breakdown_status": breakdown_status}

    log.info(f"Breakdown_Payload: {payload}")

    if SEND_DATA:
        try:
            send_req = requests.post(url, json=payload, headers=HEADERS, timeout=5)
            log.info(f"post_oee_data:{send_req.status_code}, Request status code")
            send_req.raise_for_status()
        except Exception as e:
            log.error(f"[!] Error in sending OEE data {e}")


def post_major_breakdown_whatsapp_message():
    url = f'https://gate.whapi.cloud/messages/text?token=I5QPTnzMDJCE8zJP7Qr4ffbtMq6LVxJ0'

    log.info(f"(>>>)Sending Major Breakdown Message to whatsapp group.")

    message = (
        "🚨 *MAJOR BREAKDOWN ALERT* 🚨\n\n"
        f"The *{MACHINE_NAME}* has been in breakdown for over 15 minutes.⚠️"
    )

    log.info(f"Major Breakdown Message: {message}")

    payload = {
        "to": "120363416810610968@g.us",
        "body": message

    }

    log.info(f" Major Breakdown Payload: {payload}")

    try:
        send_req = requests.post(url, json=payload, headers=HEADERS, timeout=5)
        log.info(f"post_oee_data:{send_req.status_code}, Request status code")
        send_req.raise_for_status()
    except Exception as e:
        log.error(f'Error: {e}, while sending the whatsapp message')


while True:
    try:
        opc_url = f'opc.tcp://{machine_opc_ip}:4840'
        log.info(f"OPC URL: {opc_url} for machine: {config_machine_name}\n")
        node_ids = machine_details[config_machine_name]["opc_node_ids"]
        obj_opc_client = cl_opc_client(opc_url, node_ids)
        if obj_opc_client.connect():
            today = (datetime.today() - timedelta(hours=shift_a_start.hour, minutes=shift_a_start.minute)).strftime(
                "%F")
            curr_date, curr_shift = db.get_misc_data()
            if curr_date != today:
                try:
                    url = f"{URL_GENERAL}/{today}/{MACHINE_NAME}"  # URL_GENERAL.format(date_=today)
                    response = requests.get(url)
                    GENERAL_SHIFT_STATUS = response.json()
                    if GENERAL_SHIFT_STATUS:
                        reset_values()
                        reset_oee()
                except Exception as e:
                    log.error(f"Error: {e}")
                db.update_curr_date(today)
                log.info(f"Date:({curr_date}) >>> updating >>> ({today}) ")
                FL_RESET = True
            if First_time:
                try:
                    url = f"{URL_GENERAL}/{today}/{MACHINE_NAME}"  # URL_GENERAL.format(date_=today)
                    response = requests.get(url)
                    GENERAL_SHIFT_STATUS = response.json()
                    First_time = False
                except Exception as e:
                    log.error(f"Error: while fetching general shift status: {e}")
                    if curr_shift == 'A' or curr_shift == 'B' or curr_shift == 'C':
                        GENERAL_SHIFT_STATUS = False
                    else:
                        GENERAL_SHIFT_STATUS = True
                    First_time = False

            log.info(f"General_shift_status {GENERAL_SHIFT_STATUS}")

            if GENERAL_SHIFT_STATUS:
                shift = get_shift(GENERAL_SHIFT_STATUS)
            else:
                shift = get_shift(GENERAL_SHIFT_STATUS)

            if curr_shift != shift:
                # it will send true whenever shift changes to A
                if shift == 'A' and curr_shift != 'NA':
                    whats_app_status(True)
                elif curr_shift == 'G':
                    whats_app_status(True)
                log.info(f"Shift: {curr_shift} >>> updating >>> {shift}")
                log.info(" ")
                db.update_curr_shift(shift)
                # current_shift_time = time.time()
                FL_RESET = True
            else:
                whats_app_status(False)

            if FL_RESET:
                # reset_counter()
                # reset_plc_counter()
                # current_shift_time = time.time()
                # GL_HEALTHY_START_TIME = sys.maxsize
                # GL_BLINKING_START_TIME = sys.maxsize
                # GL_STOP_START_TIME = sys.maxsize
                # GL_READY_START_TIME = sys.maxsize
                FL_RESET = False
            data = obj_opc_client.read_values()
            log.info(f"Data from OPC: {data}")
            assign_data(data)
            curr_date, curr_shift = db.get_misc_data()
            if curr_shift != 'NA':
                if GL_CURR_PART_COUNT or GL_CURR_REJECT_COUNT:
                    db.add_count_data(curr_date, curr_shift, GL_CURR_PART_COUNT, GL_CURR_REJECT_COUNT)
                    log.info(
                        f"Count updated into database!! {GL_CURR_PART_COUNT} : {GL_CURR_REJECT_COUNT} : {curr_shift}")

                # here tracking prev healthy status so that we can identify whether we have to stop duration or not
                if MACHINE_STATUS['healthy']:
                    if GL_HEALTHY_START_TIME > time.time():
                        log.info(f"+++++HEALTHY duration starts+++++")
                        GL_HEALTHY_START_TIME = time.time()
                        GL_PREV_HEALTHY_STATUS = True
                    log.info(f"time diff {round((time.time() - GL_HEALTHY_START_TIME) / 60, 2)}")
                    healthy_duration = round((time.time() - GL_HEALTHY_START_TIME) / 60, 2)
                    db.add_healthy_time(curr_date, curr_shift, healthy_duration)
                    GL_HEALTHY_START_TIME = time.time()

                else:
                    if GL_PREV_HEALTHY_STATUS:
                        log.info(f"+++++HEALTHY duration stop+++++")
                        # if GL_HEALTHY_START_TIME != sys.maxsize:
                        healthy_duration = int((time.time() - GL_HEALTHY_START_TIME) / 60)
                        db.add_healthy_time(curr_date, curr_shift, healthy_duration)
                        GL_PREV_HEALTHY_STATUS = False
                    GL_HEALTHY_START_TIME = sys.maxsize

                # here tracking prev healthy status so that we can identify whether we have to stop duration or not
                planned_break_status = break_check(curr_shift)

                # '''----Maintenance and breakdowns logics---- '''
                sys_date = datetime.now().date()
                log.info(f'[*]System Date: {sys_date}')

                machine_maintenance_status = fetch_machine_maintenance_status(sys_date, curr_shift)
                log.info(f'[*]machine_maintenance_status: {machine_maintenance_status}')

                log.info(f'[*]breakdown_status: {breakdown_status}')

                if machine_maintenance_status is True:
                    log.info(
                        f'Machine is Under Maintenance, Thus Changing the breakdown_status({breakdown_status}) to false.')
                    breakdown_status = False

                if (time.time() - FL_LAST_SENT) > 60:
                    post_breakdowns()
                    FL_LAST_SENT = time.time()

                log.info(f' ')

                if machine_maintenance_status is not True:
                    log.info(f'Machine is not under Maintenance.')

                    # when machine is stopped
                    if MACHINE_STATUS['stop']:

                        '''if Start stop time is not at initial value (as in NONE in our std. code) so in place of None here sys.maxsize is used
                        and rather than " is not None" "!= sys.maxsize" is used
                        here in the "if" construct if planned break is started then reset the variables and add the Stop time'''

                        if GL_STOP_START_TIME != sys.maxsize and planned_break_status:
                            stop_duration = round((time.time() - GL_STOP_START_TIME) / 60, 2)
                            db.add_stop_time(curr_date, curr_shift, stop_duration)
                            GL_STOP_START_TIME = sys.maxsize
                            GL_PREV_STOP_STATUS = False
                            log.info(f"Stop duration stops because planned break is active")

                        # if GL_STOP_START_TIME is at initial value and planned break status is not active
                        # then initialize the stop start time with time.time()
                        # basically the machine has stopped
                        if GL_STOP_START_TIME > time.time() and not planned_break_status:
                            log.info(f"+++++STOP duration starts+++++")
                            GL_STOP_START_TIME = time.time()
                            GL_PREV_STOP_STATUS = True
                            send_alarm_status(True)

                        if GL_STOP_START_TIME != sys.maxsize:
                            stop_duration = round((time.time() - GL_STOP_START_TIME) / 60, 2)
                            db.add_stop_time(curr_date, curr_shift, stop_duration)
                            GL_STOP_START_TIME = time.time()

                    else:
                        if GL_PREV_STOP_STATUS:
                            log.info(f"+++++STOP duration stop+++++")
                            # if GL_STOP_START_TIME != sys.maxsize:
                            stop_duration = round((time.time() - GL_STOP_START_TIME) / 60, 2)
                            db.add_stop_time(curr_date, curr_shift, stop_duration)
                            GL_PREV_STOP_STATUS = False
                        GL_STOP_START_TIME = sys.maxsize
                        send_alarm_status(False)

                    if MACHINE_STATUS['ready'] and not MACHINE_STATUS['healthy'] and not MACHINE_STATUS['stop']:
                        if GL_READY_START_TIME > time.time():
                            log.info(f"+++++READY duration starts+++++")
                            GL_READY_START_TIME = time.time()
                            PREVIOUS_CONSTANT_YELLOW_STAT = True
                        ready_duration = round((time.time() - GL_READY_START_TIME) / 60, 2)
                        db.add_ready_time(curr_date, curr_shift, ready_duration)
                        GL_READY_START_TIME = time.time()
                        send_alarm_status(False)
                    else:
                        if PREVIOUS_CONSTANT_YELLOW_STAT:
                            log.info(f"+++++READY duration stop+++++")
                            # if GL_READY_START_TIME != sys.maxsize:
                            ready_duration = round((time.time() - GL_READY_START_TIME) / 60, 2)
                            db.add_ready_time(curr_date, curr_shift, ready_duration)
                            PREVIOUS_CONSTANT_YELLOW_STAT = False
                        GL_READY_START_TIME = sys.maxsize

                    # here calculating breakdown if ready blinks for 15 minutes then we will start the breakdown

                    if GL_PREV_READY_STATUS != MACHINE_STATUS["ready"]:
                        blinking = True
                        if GL_BLINKING_START_TIME > time.time():
                            GL_BLINKING_START_TIME = time.time()
                            GL_PREV_READY_STATUS = MACHINE_STATUS['ready']
                            breakdown_status = True

                        if time.time() - GL_BLINKING_START_TIME > 900 and not GL_BREAKDOWN_STATUS:

                            log.info(f'Major Breakdown is going on...')
                            log.info(f'[*]breakdown_status: {breakdown_status}, '
                                     f'prev_breakdown_status: {prev_breakdown_status}')

                            if breakdown_status != prev_breakdown_status:
                                log.info(f'So sending the Major breakdown whatsapp message>>')
                                post_major_breakdown_whatsapp_message()
                                prev_breakdown_status = breakdown_status

                            GL_BREAKDOWN_STATUS = True
                            GL_breakdown_start = datetime.now().time()
                            start_duration = time.time()
                            log.info(f"+++++++++++++++Breakdown Started++++++++++++++++++")
                    else:
                        blinking = False
                        GL_BLINKING_START_TIME = sys.maxsize
                        GL_BREAKDOWN_STATUS = False
                        breakdown_status = False

                    if GL_BREAKDOWN_STATUS:
                        duration = round((time.time() - start_duration) / 60, 2)
                        # db.add_breakdown_data(curr_date, curr_shift, GL_breakdown_start, GL_breakdown_stop, duration)
                        if GL_PREV_READY_STATUS and MACHINE_STATUS['ready']:
                            log.info(f"+++++++++++++++Breakdown Stopped++++++++++++++++++")
                            GL_breakdown_stop = datetime.now().time()
                            # db.update_breakdown_data(curr_date, curr_shift, GL_breakdown_start, GL_breakdown_stop, duration)
                else:
                    log.info(f'Machine is Under Maintenance, Thus Breakdowns are stopped.')

                # adding planned data into database
                # if current_shift_time:

                data = db.fetch_data(curr_date, curr_shift)
                planned_production = data["healthy_time"] + data["ready_time"] + data["stop_time"]
                log.info(f"planned production : {planned_production}")

                # Check if actual planned production time exceeds planned production time
                shift_planned_production = round((get_current_total_time(curr_shift)) / 60, 2)
                log.info(f"Code shift planned production : {planned_production}")
                if planned_production > shift_planned_production:
                    planned_production = shift_planned_production

                db.add_planned_production_time(curr_date, curr_shift, planned_production)
                if SEND_DATA:
                    send_data(curr_date, curr_shift)
                    schedule.run_pending()
                time.sleep(0.5)
            else:
                log.info(f"shift is Not Available")
                time.sleep(0.5)

            if (time.time() - last_sent_oee_payload) > 10:
                oee_calculations()
                send_data_to_attributes()
                last_sent_oee_payload = time.time()

        else:
            log.info(f"Machine is disconnected")
            time.sleep(0.5)

    except Exception as e:
        log.error(f"Error: {e}")
        time.sleep(0.5)

    time.sleep(1)
