"""this is main file of Assembly_1 machine"""
import sys
import requests

from logger import log
from comm import read_s7_data
import time
from database import DBHelper
from shift import get_shift, shift_a_start, shift_b_start, shift_c_start, get_current_total_time, break_check
from datetime import datetime, timedelta
import schedule

GL_breakdown_start = 0
GL_breakdown_stop = 0
MACHINE_STATUS = {
    "ready": None,
    "stop": None,
    "healthy": None
}

FL_RESET = 0
First_time = True
GL_PREVIOUS_SHIFT = ""
PART_COUNT_ING_INIT = True
GL_TOTAL_COUNT = 0
GL_TOTAL_REJECT_COUNT = 0
GL_CURR_PART_COUNT = 0
GL_CURR_REJECT_COUNT = 0

GL_PREV_PART_COUNT = 0
GL_PREV_REJECT_COUNT = 0

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

cPartCountIng = 0
part_count_ing = 0
current_part_count = 0
rejectPartCountIng = 0
reject_part_count_ing = 0
current_reject_count = 0

today = ""
shift = ""
alert = sys.maxsize
GENERAL_SHIFT_STATUS = False


def reset_counter():
    global GL_HEALTHY_START_TIME, GL_STOP_START_TIME, GL_READY_START_TIME, GL_PREV_HEALTHY_STATUS, GL_PREV_STOP_STATUS, PREVIOUS_CONSTANT_YELLOW_STAT
    GL_HEALTHY_START_TIME = sys.maxsize
    GL_STOP_START_TIME = sys.maxsize
    GL_READY_START_TIME = sys.maxsize
    GL_PREV_HEALTHY_STATUS = False
    GL_PREV_STOP_STATUS = False
    PREVIOUS_CONSTANT_YELLOW_STAT = False


def assign_data(data):
    global GL_CURR_PART_COUNT, GL_CURR_REJECT_COUNT, MACHINE_STATUS
    GL_CURR_PART_COUNT = data[0]
    GL_CURR_REJECT_COUNT = data[1]
    MACHINE_STATUS['stop'] = data[3]
    MACHINE_STATUS['ready'] = data[4]
    MACHINE_STATUS['healthy'] = data[2]


ACCESS_TOKEN = "lHR9XG4N9tifdWLsgzzw"
URL_ATTR = f'https://ithingspro.cloud/api/v1/{ACCESS_TOKEN}/attributes'
URL_TELE = f'https://ithingspro.cloud/api/v1/{ACCESS_TOKEN}/telemetry'
API = "https://ithingspro.cloud/Hettich/create_shift_data/"

URL_GENERAL = 'https://ithingspro.cloud/Hettich/check_generalshift_status'
MACHINE_NAME = "HMT Assy-2"

SEND_DATA = True


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
            response = requests.post(URL_TELE, json=payload, timeout=3)
            response.raise_for_status()
            log.info(f"Production data sent (status:{response.status_code})")
    except Exception as e:
        log.error(f"Error: {e}")


# def send_status():
#     global MACHINE_STATUS
#     try:
#         # "stop": d2[0],
#         # "ready": d2[1],
#         # "healthy": d2[2]
#         current_date, current_shift = db.get_misc_data()
#         payload = db.fetch_data(current_date, current_shift)
#         payload['stop_status'] = MACHINE_STATUS['stop']
#         payload['ready_status'] = MACHINE_STATUS['ready']
#         payload['healthy'] = MACHINE_STATUS['healthy']
#         log.info(f"send_data: {payload}")
#         for k, v in payload.items():
#             if v is None:
#                 payload[k] = 0
#         response = requests.post(URL_ATTR, json=payload, timeout=3)
#         response.raise_for_status()
#         log.info(f"status code {response.status_code}")
#     except Exception as e:
#         log.error(f"Error: {e}")


# def send_shift_data():
#     try:
#         current_date, current_shift = db.get_misc_data()
#         shift_data = db.get_shift_data(current_date, current_shift)
#         payload = {
#             "A_cycle_parts": shift_data["A_cycle_time_parts"],
#             "A_real_parts": shift_data["A_real_time_parts"],
#             "B_cycle_parts": shift_data["B_cycle_time_parts"],
#             "B_real_parts": shift_data["B_real_time_parts"],
#             "C_cycle_parts": shift_data["C_cycle_time_parts"],
#             "C_real_parts": shift_data["C_real_time_parts"],
#             "G_real_parts": shift_data["G_real_time_parts"],
#             "G_cycle_parts": shift_data["G_cycle_time_parts"]
#         }
#
#         response = requests.post(URL_ATTR, json=payload, timeout=3)
#         response.raise_for_status()
#         log.info(f"Shift data sent: {response.status_code}")
#     except Exception as e:
#         log.error(f"Error: {e}")


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
            planned_time = data["planned_time"] - data['stop_time']
            if planned_time < 0:
                planned_time = 0
            actual_production = data["part_count"] + data["reject_count"]
            max_possible_production = int(planned_time * 55)
            availability_planned_time = data['healthy_time'] + data["stop_time"] + data["ready_time"]
            # real oee formula
            # try:
            #     quality = data['part_count'] / actual_production
            #     oee = actual_production / max_possible_production
            #     real_oee = oee * quality
            #     real_oee = round(real_oee * 100, 2)
            # except Exception as e:
            #     log.error(f"Error: {e}")
            #     real_oee = 0
            # availability
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
                # performance = actual_production / max_possible_production
            except Exception as e:
                log.error(f"Error: {e}")
                performance = 0

            # machine utilization
            try:
                machine_util = performance * 100
            except Exception as e:
                log.error(f'Error while calculating machine utilization : {e}')
                machine_util = 0

            # Quality
            try:
                quality = data['part_count'] / actual_production
                quality_per = round(quality * 100, 2)
            except Exception as e:
                log.error(f"Error : {e}")
                quality_per = 0
                quality = 0

            # OEE
            try:
                oee = availability * performance * quality
                oee = round(oee * 100, 2)
            except Exception as e:
                log.error(f"Error:  {e}")
                oee = 0

            # payload = {
            #     "performance": round(performance, 2),
            #     "availability": round(availability, 2),
            #     "quality": quality_per,
            #     "oee": round(real_oee * 100, 2),
            # }
            # log.info(f"Oee: {payload}")
            # # sending data
            # response = requests.post(URL_TELE, json=payload, timeout=2)
            # response.raise_for_status()
            # log.info(f"Oee data sent (status:{response.status_code}")
            # {
            #     "date_": "2024-03-12",
            #     "shift": "A",
            #     "machine_name": "string",
            #     "part_count": 0,
            #     "reject_count": 0,
            #     "healthy_time": 0,
            #     "stop_time": 0,
            #     "ready_time": 0,
            #     "planned_time": 0,
            #     "performance": 0,
            #     "availibility": 0,
            #     "quality": 0,
            #     "oee": 0
            # }
            if oee < 100:
                try:
                    payload_api = data
                    payload_api["performance"] = round(performance, 2)
                    payload_api["availability"] = round(availability, 2)
                    payload_api["quality"] = quality_per
                    payload_api['oee'] = oee
                    log.info(f"Api : {payload_api}")
                    response = requests.post(API, json=payload_api, timeout=2)
                    response.raise_for_status()
                    log.info(f"Api data sent")
                except Exception as e:
                    log.error(f"Error: {e}")

                loss_time_ = data["stop_time"] + data["ready_time"]
                log.info(f'loss_time for telemetry: {loss_time_}')

                try:
                    payload = {
                        "day_Oee": oee,
                        "day_quality": quality_per,
                        "machine_util": round(machine_util, 2),
                        "availability_percent": availability_percent,
                        "loss_time": loss_time_
                    }
                    log.info(f"Shift Oee : {payload}")
                    response = requests.post(URL_TELE, json=payload, timeout=2)
                    response.raise_for_status()
                    log.info(f"Shift Oee sent to telemetry: {response.status_code}")
                except Exception as e:
                    log.error(f"Error in sending shift Oee : {e}")
            else:
                log.info(f"Oee is greater than 100")
        else:
            log.info(f"Planned time is not available")
    except Exception as e:
        log.error(f"Error: {e}")


# def send_day_count():
#     global today
#     try:
#         prod_data = db.get_day_production(today)
#         operating_time = prod_data['total_healthy']
#         payload1 = {
#             "day_count": prod_data["total_part_count"],
#             "day_reject": prod_data["total_reject_count"],
#             "day_up_time": round(operating_time, 2)
#         }
#         log.info(f"Day production : {payload1}")
#         response = requests.post(URL_ATTR, json=payload1, timeout=3)
#         response.raise_for_status()
#         log.info(f"Day Count send to attritbutes : {response.status_code}")
#     except Exception as e:
#         log.error(f"Error : {e}")


# def send_day_production():
#     global today
#     try:
#         prod_data = db.get_day_production(today)
#         actual_production = prod_data["total_part_count"] + prod_data["total_reject_count"]
#         operating_time = prod_data['total_healthy']
#         planned_time = prod_data["total_planned"]
#         # try:
#         #     quality = prod_data['total_part_count'] / actual_production
#         #     oee = actual_production / (planned_time * 55)
#         #     real_oee = oee * quality
#         #     real_oee = round(real_oee * 100, 2)
#         # except Exception as e:
#         #     log.error(f"Error: {e}")
#         #     real_oee = 0
#         #
#         # # availability
#         try:
#             availability = operating_time / planned_time
#         except Exception as e:
#             log.error(f"Error : {e}")
#             availability = 0
#
#         # performance
#         try:
#             max_possible_production = int(planned_time * 55)
#             performance = actual_production / max_possible_production
#         except Exception as e:
#             log.error(f"Error: {e}")
#             performance = 0
#
#         # Quality
#         try:
#             quality = prod_data['total_part_count'] / actual_production
#             quality_per = round(quality * 100, 2)
#             log.info(f"quality_per {quality_per}")
#         except Exception as e:
#             log.error(f"Error : {e}")
#             quality_per = 0
#             quality = 0
#
#         try:
#             # OEE
#             oee = availability * performance * quality
#             oee = round(oee * 100, 2)
#         except Exception as e:
#             log.error(f"Error : {e}")
#             oee = 0
#         if oee < 100:
#             payload = {
#                 "day_Oee": oee,
#                 "day_quality": quality_per,
#             }
#             log.info(f"Day production : {payload}")
#             response = requests.post(URL_TELE, json=payload, timeout=3)
#             response.raise_for_status()
#             log.info(f"Day Oee sent to telemetry: {response.status_code}")
#         else:
#             log.info(f"Oee is greater than 100")
#     except Exception as e:
#         log.error(f"Error: {e}")


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
        "day_count": 0,
        "day_reject": 0,
        "day_up_time": 0,
        "healthy_time": 0,
        "part_count": 0,
        "reject_count": 0,
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
        # log.info(f"Day production : {payload}")
        response = requests.post(URL_TELE, json=payload, timeout=3)
        response.raise_for_status()
        log.info(f"OEE RESET: {response.status_code}")
    except Exception as e:
        log.error(f"Error while resetting oee: {e}")


###############################################################################################
def send_data_to_attributes():
    global MACHINE_STATUS, today
    current_date, current_shift = db.get_misc_data()

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
        "day_count": 0,
        "day_reject": 0,
        "day_up_time": 0,
    }

    try:
        # "stop": d2[0],
        # "ready": d2[1],
        # "healthy": d2[2]

        # current_date, current_shift = db.get_misc_data()
        # payload = db.fetch_data(current_date, current_shift)
        data_attr = db.fetch_data(current_date, current_shift)
        payload = data_attr
        payload['stop_status'] = MACHINE_STATUS['stop'] or 0
        payload['ready_status'] = MACHINE_STATUS['ready'] or 0
        payload['healthy'] = MACHINE_STATUS['healthy'] or 0
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
        payload["day_count"] = prod_data["total_part_count"]
        payload["day_reject"] = prod_data["total_reject_count"]
        payload["day_up_time"] = round(operating_time, 2)
    except Exception as e:
        log.error(f"Error : {e}")

    log.info(f"Payload {payload}")

    try:
        response = requests.post(URL_ATTR, json=payload, timeout=3)
        response.raise_for_status()
        log.info(f"Sending data to attribute: {response.status_code}")
    except Exception as e:
        log.error(f"Error in sending data to attri {e}")


schedule.every(5).seconds.do(send_data_to_attributes)
######################################################################################
schedule.every(30).seconds.do(oee_calculations)
# schedule.every(1).minutes.do(send_day_production)
# schedule.every(5).seconds.do(send_status)
# schedule.every(5).seconds.do(send_shift_data)
# schedule.every(5).seconds.do(send_day_count)
while True:
    try:

        today = (datetime.today() - timedelta(hours=shift_a_start.hour, minutes=shift_a_start.minute)).strftime(
            "%F")
        curr_date, curr_shift = db.get_misc_data()

        if curr_date != today:
            try:
                url = f"{URL_GENERAL}/{today}/{MACHINE_NAME}"  # URL_GENERAL.format(date_=today)
                response = requests.get(url)
                GENERAL_SHIFT_STATUS = response.json()
                reset_values()
                reset_oee()
            except Exception as e:
                log.error(f"Error: {e}")
            db.update_curr_date(today)
            log.info(f"Date:({curr_date}) >>> updating >>> ({today}) ")
            FL_RESET = 1
            db.update_fl_status(FL_RESET)
        status = read_s7_data()
        if status is not None:
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
                FL_RESET = 1
                db.update_fl_status(FL_RESET)
            else:
                whats_app_status(False)

            curr_date, curr_shift = db.get_misc_data()
            FL_RESET = db.get_fl_status()
            if FL_RESET:
                reset_counter()
                # current_shift_time = time.time()
                # GL_HEALTHY_START_TIME = sys.maxsize
                # GL_BLINKING_START_TIME = sys.maxsize
                # GL_STOP_START_TIME = sys.maxsize
                # GL_READY_START_TIME = sys.maxsize
                part_count_ing = cPartCountIng  # resetting part count ing to 0 here
                reject_part_count_ing = rejectPartCountIng
                current_part_count = 0
                current_reject_count = 0
                db.add_shift_start_data(curr_date, curr_shift, 0, 0, part_count_ing, reject_part_count_ing)
                FL_RESET = 0
                db.update_fl_status(FL_RESET)

            data = read_s7_data()
            log.info(f"Data from PLC: {data}")
            assign_data(data)
            cPartCountIng = data[0]
            rejectPartCountIng = data[1]

            if PART_COUNT_ING_INIT:
                # here we handled reboot across Same shift
                part_count_ing = db.get_part_count_ing(curr_date, curr_shift)
                reject_part_count_ing = db.get_reject_part_count_ing(curr_date, curr_shift)
                if part_count_ing is None:
                    # here we are handling reboot across different shift
                    part_count_ing = cPartCountIng
                    db.fixing_reset_part_count(curr_date, curr_shift, part_count_ing)
                if reject_part_count_ing is None:
                    reject_part_count_ing = rejectPartCountIng
                    db.fixing_reset_reject_part_count(curr_date, curr_shift, reject_part_count_ing)
                PART_COUNT_ING_INIT = False
            log.info(f"[+] part_count_ing is {part_count_ing}")
            log.info(f"[+] reject_part_count_ing is {reject_part_count_ing}")

            # Updated the Logic to handle multiple resets
            if (cPartCountIng - part_count_ing) < 0 or current_part_count > (cPartCountIng - part_count_ing):
                part_count_ing = -current_part_count
                db.fixing_reset_part_count(curr_date, curr_shift, part_count_ing)
            elif (cPartCountIng - part_count_ing) >= 0:
                current_part_count = cPartCountIng - part_count_ing

            if (rejectPartCountIng - reject_part_count_ing) < 0 or current_reject_count > (
                    rejectPartCountIng - reject_part_count_ing):
                reject_part_count_ing = -current_reject_count
                db.fixing_reset_reject_part_count(curr_date, curr_shift, reject_part_count_ing)
            elif (rejectPartCountIng - reject_part_count_ing) >= 0:
                current_reject_count = rejectPartCountIng - reject_part_count_ing
            GL_CURR_PART_COUNT = current_part_count
            GL_CURR_REJECT_COUNT = current_reject_count

            if GL_CURR_PART_COUNT is not None and curr_shift != 'NA':

                # if code service restarted into gateway then this will become true
                # if First_time:
                #     # if shift == "A":
                #     #     datetime_obj = datetime.combine(datetime.today().date(), shift_a_start)
                #     #     current_shift_time = datetime_obj.timestamp()
                #     # elif shift == "B":
                #     #     datetime_obj = datetime.combine(datetime.today().date(), shift_b_start)
                #     #     current_shift_time = datetime_obj.timestamp()
                #     # elif shift == "C":
                #     #     datetime_obj = datetime.combine(datetime.today().date(), shift_c_start)
                #     #     current_shift_time = datetime_obj.timestamp()
                #     First_time = False

                # curr_date, curr_shift = db.get_misc_data()
                # if GL_CURR_PART_COUNT is not None:
                # if GL_CURR_PART_COUNT != GL_PREV_PART_COUNT:
                #     GL_TOTAL_COUNT += 1
                # elif GL_CURR_REJECT_COUNT != GL_PREV_REJECT_COUNT:
                #     GL_TOTAL_REJECT_COUNT += 1
                db.add_count_data(curr_date, curr_shift, GL_CURR_PART_COUNT, GL_CURR_REJECT_COUNT)
                log.info(f"Count updated into database!! {GL_CURR_PART_COUNT} : {GL_CURR_REJECT_COUNT} : {curr_shift}")

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
                if MACHINE_STATUS['stop']:
                    if GL_STOP_START_TIME != sys.maxsize and planned_break_status:
                        stop_duration = round((time.time() - GL_STOP_START_TIME) / 60, 2)
                        db.add_stop_time(curr_date, curr_shift, stop_duration)
                        GL_STOP_START_TIME = sys.maxsize
                        GL_PREV_STOP_STATUS = False
                        log.info(f"Stop duration stops because planned break is active")
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

                    if time.time() - GL_BLINKING_START_TIME > 900 and not GL_BREAKDOWN_STATUS:
                        GL_BREAKDOWN_STATUS = True
                        GL_breakdown_start = datetime.now().time()
                        start_duration = time.time()
                        log.info(f"+++++++++++++++Breakdown Started++++++++++++++++++")
                else:
                    blinking = False
                    GL_BLINKING_START_TIME = sys.maxsize
                    GL_BREAKDOWN_STATUS = False

                if GL_BREAKDOWN_STATUS:
                    duration = round((time.time() - start_duration) / 60, 2)
                    # db.add_breakdown_data(curr_date, curr_shift, GL_breakdown_start, GL_breakdown_stop, duration)
                    if GL_PREV_READY_STATUS and MACHINE_STATUS['ready']:
                        log.info(f"+++++++++++++++Breakdown Stopped++++++++++++++++++")
                        GL_breakdown_stop = datetime.now().time()
                        # db.update_breakdown_data(curr_date, curr_shift, GL_breakdown_start, GL_breakdown_stop, duration)

                # adding planned data into database
                # if current_shift_time:
                planned_production = round((get_current_total_time(curr_shift)) / 60, 2)
                log.info(f"planned production : {planned_production}")
                db.add_planned_production_time(curr_date, curr_shift, planned_production)
                if SEND_DATA:
                    send_data(curr_date, curr_shift)
                    schedule.run_pending()
                time.sleep(0.5)
            else:
                log.info(f"Part count is None Or shift is Not Available")
                time.sleep(0.5)
        else:
            log.info(f"Machine is disconnected")
            time.sleep(0.5)
    except Exception as e:
        log.error(f"Error: {e}")
        time.sleep(0.5)
