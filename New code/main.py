from ingenious_libs.system_info import send_system_info
from ingenious_libs import logMan
from ingenious_libs.utils import ConfReader
from pyModbusTCP.client import ModbusClient
from database import DBHelper
from conversions import ShiftManager
from alerts import CL_Alerts
from datetime import date, datetime, timedelta
import threading
import requests
import time
import sys
import os

log = logMan.ILogs('HIS_LOGS', 'info', True, True)
num_of_days = 4
log_cleaner = logMan.LogCleaner(num_of_days)

if getattr(sys, 'frozen', False):
    dirname = os.path.dirname(sys.executable)
else:
    dirname = os.path.dirname(os.path.abspath(__file__))

CONFDIR = f"{dirname}/config"
log.info(f"configuration directory name is {CONFDIR}")
if not os.path.isdir(CONFDIR):
    log.info("[-] conf directory doesn't exists")
    try:
        os.mkdir(CONFDIR)
        log.info("[+] Created configuration dir successfully")
    except Exception as e:
        log.error(f"[-] Can't create dir configuration Error: {e}")

machine_config_file = f"{CONFDIR}/cosberg_machines_config_file.csv"
server_config_file = f"{CONFDIR}/server_config_file.csv"

machine_obj = {}
MACHINE_LIST = []
combined_config = []

HEADERS = {'content-type': 'application/json'}
SEND_DATA = True
HOST = None


class CL_MODBUS:
    def __init__(self, logger, plc_ip, start_addr):

        self.log = logger
        self.plc_ip = plc_ip
        self.plc_port = 502
        self.unit_id = 1
        self.TIMEOUT = 5
        self.start_addr = start_addr
        self.reset_reg_addr = 4100
        self.RESET_VALUE = 28000

    def is_connected(self) -> bool:
        try:
            for i in range(3):
                client = ModbusClient(self.plc_ip, self.plc_port, self.unit_id,
                                      auto_open=True, auto_close=True, timeout=self.TIMEOUT)

                data_list = client.read_holding_registers(self.start_addr, 4)

                if data_list is not None:
                    if len(data_list) == 4:
                        self.log.info(f"PLC [{self.plc_ip}] is connected. Holding Registers: {data_list}")
                        return True
            self.log.warn(f"Failed to connect to PLC [{self.plc_ip}]. Connection attempt unsuccessful.")
            return False
        except Exception as e:
            self.log.error(f"[-] Can't connect to the machine {e}")
            return False

    def read_counter(self, holding_addr) -> int:
        try:
            client = ModbusClient(self.plc_ip, self.plc_port, self.unit_id,
                                  auto_open=True, auto_close=True, timeout=self.TIMEOUT)
            data_list = client.read_holding_registers(holding_addr, 2)
            counter = data_list[0]
            return counter
        except Exception as e:
            self.log.error(f"[-] Error {e}")

    def read_status(self, discrete_addr) -> bool:
        try:
            client = ModbusClient(self.plc_ip, self.plc_port, self.unit_id,
                                  auto_open=True, auto_close=True, timeout=self.TIMEOUT)
            self.log.info(f'discrete_addr: {discrete_addr}')
            data_list = client.read_discrete_inputs(discrete_addr, 2)
            self.log.info(f"Status list: {data_list}")
            self.log.info(f" ")
            status = data_list[0]
            return status
        except Exception as e:
            self.log.error(f"[!] Error while reading machine statuses: {e}")

    def write_counter(self, holding_addr) -> bool:
        try:
            client = ModbusClient(self.plc_ip, self.plc_port, self.unit_id,
                                  auto_open=True, auto_close=True, timeout=self.TIMEOUT)
            reset_status = client.write_single_register(self.reset_reg_addr, self.RESET_VALUE)
            self.log.info(f'Reset status: {reset_status}')
            if reset_status is True:
                reset_value = self.read_counter(holding_addr)
                if reset_value == 0:
                    self.log.info('Successfully, Count value RESET')
                    return True
                else:
                    self.log.info('Failed! Count value not RESET')
                    return False
            else:
                self.log.info('Failed! Count value not RESET')
                return False
        except Exception as e:
            self.log.error(f"Unable to reset the counter, ERROR: {e}")
            return False


class CL_MAIN:
    def __init__(self, machine_name_, access_token, plc_ip, part_count_addr, reject_count_addr,
                 red_light_addr, yellow_light_addr, green_light_addr):

        self.machine_name = machine_name_

        self.log = logMan.ILogs(self.machine_name, 'info', True, True,
                                logger_file_name=self.machine_name)

        self.plc_ip = plc_ip
        self.part_count_addr = part_count_addr  # 4114
        self.reject_count_addr = reject_count_addr  # 4112
        self.green_light_addr = green_light_addr  # 2052
        self.yellow_light_addr = yellow_light_addr  # 2051
        self.red_light_addr = red_light_addr  # 2050

        self.access_token = access_token

        self.obj_modbus = CL_MODBUS(self.log, plc_ip, reject_count_addr)
        self.obj_conversions = ShiftManager(self.log)
        self.obj_db = DBHelper(self.machine_name, self.log)
        self.obj_alerts = CL_Alerts(HOST, self.access_token, self.log)

        self.FL_FIRST_START = True
        self.FL_FIRST_CALL = True
        self.FL_SHIFT_START_RESET = False
        self.FL_ST_NOT_FETCHED = False
        self.FL_PB_NOT_FETCHED = False
        self.operating_status = False
        self.idle_status = False
        self.breakdown_status = False
        self.active_machine_status = False
        self.general_shift_status = False
        self.machine_maintenance_status = False
        self.major_breakdown_alert_sent = False
        self.fl_one_time = False
        self.IS_BREAKDOWN = 'BREAKDOWN'
        self.IS_IDLE = 'IDLE'
        self.IS_OPERATING = 'OPERATING'
        self.running_mode = "BUSY"

        self.LAST_CON_ALERT_SENT = time.time()
        self.LAST_CT_ALERT_SENT = time.time()
        self.LAST_BD_ALERT_SENT = time.time()
        self.LAST_OEE_PAYLOAD_SENT = time.time()
        self.last_activity_time = time.time()
        self.CYCLE_START_TIME = time.time()

        self.tele_payload = {}
        self.attr_payload = {}
        self.api_payload = {}

        self.target_count = 0
        self.part_count = 0
        self.reject_count = 0
        self.cycle_time = 0
        self.working_time_so_far = 0
        self.operating_time = 0
        self.idle_time = 0
        self.breakdown_time = 0
        self.prev_part_count = 0

        self.plant_date, self.curr_shift = self.obj_db.get_misc_data()
        self.new_shift = self.obj_conversions.get_current_shift(self.general_shift_status)

    def post_to_api(self) -> None:
        url = f'https://ithingspro.cloud/Hettich/create_shift_data/'
        self.log.info(f"(>>>)Sending data to API")
        self.log.info(f'api_payload: {self.api_payload}')
        if SEND_DATA:
            try:
                send_req = requests.post(url, json=self.api_payload, headers=HEADERS, timeout=5)
                send_req.raise_for_status()
                self.log.info(f"post_to_api:{send_req.status_code}, Request status code")

                # for i in self.obj_db.get_sync_data():
                #     max_ts = max([j['ts'] for j in i])
                #     try:
                #         sync_req = requests.post(url, json=i, headers=HEADERS, timeout=5)
                #         self.log.info(f'post_data_sync_req:{sync_req.status_code}')
                #         sync_req.raise_for_status()
                #         self.log.info(f"Sync_successful of post_to_telemetry{max_ts}")
                #         self.obj_db.clear_sync_data(max_ts)
                #         with open(os.path.join(dirname, f'logs/sync_log.{date.today()}.txt'), 'a') as f:
                #             pname = f'{datetime.now().strftime("%Y-%m-%d %H:%M:%S")} ---- SYNC DONE\n'
                #             f.write(pname)
                #         time.sleep(0.1)
                #     except Exception as e:
                #         self.log.error(f"[!] Error in sending SYNC OEE data {e}")

            except Exception as e:
                self.log.error(f"[!] Error in sending OEE data {e}")
                self.obj_db.add_sync_data(self.api_payload)
        self.log.info(f" ")

    def post_to_telemetry(self) -> None:
        url = f'https://{HOST}/api/v1/{self.access_token}/telemetry'
        self.log.info(f"(>>>)Sending data to telemetry")
        self.log.info(f'tele_payload: {self.tele_payload}')
        if SEND_DATA:
            try:
                send_req = requests.post(url, json=self.tele_payload, headers=HEADERS, timeout=5)
                send_req.raise_for_status()
                self.log.info(f"post_to_telemetry:{send_req.status_code}, Request status code")

                # for i in self.obj_db.get_sync_data():
                #     max_ts = max([j['ts'] for j in i])
                #     try:
                #         sync_req = requests.post(url, json=i, headers=HEADERS, timeout=5)
                #         self.log.info(f'post_data_sync_req:{sync_req.status_code}')
                #         sync_req.raise_for_status()
                #         self.log.info(f"Sync_successful of post_to_telemetry{max_ts}")
                #         self.obj_db.clear_sync_data(max_ts)
                #         with open(os.path.join(dirname, f'logs/sync_log.{date.today()}.txt'), 'a') as f:
                #             pname = f'{datetime.now().strftime("%Y-%m-%d %H:%M:%S")} ---- SYNC DONE\n'
                #             f.write(pname)
                #         time.sleep(0.1)
                #     except Exception as e:
                #         self.log.error(f"[!] Error in sending SYNC OEE data {e}")

            except Exception as e:
                self.log.error(f"[!] Error in sending OEE data {e}")
                # self.obj_db.add_sync_data(self.tele_payload)
        self.log.info(f" ")

    def post_to_attribute(self) -> None:
        url = f'https://{HOST}/api/v1/{self.access_token}/attributes'
        self.log.info(f"(>>>)Sending data to Attributes")
        self.log.info(f'attr_payload: {self.attr_payload}')
        if SEND_DATA:
            try:
                send_req = requests.post(url, json=self.attr_payload, headers=HEADERS, timeout=5)
                send_req.raise_for_status()
                self.log.info(f"post_to_attributes:{send_req.status_code}, Request status code")

                # for i in self.obj_db.get_sync_data():
                #     max_ts = max([j['ts'] for j in i])
                #     try:
                #         sync_req = requests.post(url, json=i, headers=HEADERS, timeout=5)
                #         self.log.info(f'post_data_sync_req:{sync_req.status_code}')
                #         sync_req.raise_for_status()
                #         self.log.info(f"Sync_successful of post_to_telemetry{max_ts}")
                #         self.obj_db.clear_sync_data(max_ts)
                #         with open(os.path.join(dirname, f'logs/sync_log.{date.today()}.txt'), 'a') as f:
                #             pname = f'{datetime.now().strftime("%Y-%m-%d %H:%M:%S")} ---- SYNC DONE\n'
                #             f.write(pname)
                #         time.sleep(0.1)
                #     except Exception as e:
                #         self.log.error(f"[!] Error in sending SYNC OEE data {e}")

            except Exception as e:
                self.log.error(f"[!] Error in sending OEE data {e}")
                # self.obj_db.add_sync_data(self.attr_payload)
        self.log.info(f" ")

    def post_breakdown_alert_to_whatsapp_grp(self):
        url = f'https://gate.whapi.cloud/messages/text?token=I5QPTnzMDJCE8zJP7Qr4ffbtMq6LVxJ0'

        self.log.info(f"(>>>)Sending Major Breakdown Message to whatsapp group.")

        message = (
            "\U0001F6A8 *MAJOR BREAKDOWN ALERT* \U0001F6A8\n\n"
            f"The *{self.machine_name}* has been in breakdown for over 15 minutes. \u26A0\uFE0F"
        )

        log.info(f"Major Breakdown Message: {message}")

        payload = {
            "to": "120363416810610968@g.us",
            "body": message

        }

        self.log.info(f" Major Breakdown Payload: {payload}")
        try:
            send_req = requests.post(url, json=payload, headers=HEADERS, timeout=5)
            log.info(f"post_oee_data:{send_req.status_code}, Request status code")
            send_req.raise_for_status()
        except Exception as e:
            self.log.error(f'Error: {e}, while sending the whatsapp message')

    def fetch_general_shift_status(self):
        try:
            self.log.info('Getting General Shift status from server')
            url = f'https://ithingspro.cloud/Hettich/check_generalshift_status/{self.plant_date}/{self.machine_name}'
            self.log.info(f"url:{url}")
            req = requests.get(url, headers=HEADERS, timeout=5)
            status = req.json()
            self.log.info(f"fetched General Shift status:{status}")
            if status == {'detail': 'Not Found'}:
                status = False
            return status
        except requests.Timeout:
            self.log.error("[!!] Error: Timeout occurred while fetching the General Shift status.")
            return None
        except requests.RequestException as e:
            self.log.error(f"[!!] Error: {e}, unable to fetch the General Shift status.")
            return None

    def fetch_machine_maintenance_status(self, sys_date, curr_shift):
        try:
            self.log.info('Getting Machine Maintenance from server')
            url = f'https://ithingspro.cloud/Hettich/check_maintenance_status/{sys_date}/{curr_shift}/{self.machine_name}/'
            self.log.info(f"url:{url}")
            req = requests.get(url, headers=HEADERS, timeout=5)
            status = req.json()
            self.log.info(f"fetched Machine Maintenance status:{status}")
            return status
        except requests.Timeout:
            self.log.error("[!!] Error: Timeout occurred while fetching the Maintenance status.")
            return None
        except requests.RequestException as e:
            self.log.error(f"[!!] Error: {e}, unable to fetch the Maintenance status.")
            return None

    def calculate_operating_time(self, green_light_status: bool) -> None:
        try:
            self.log.info("Starting Operating time calculation")
            prev_time_update, prev_status = self.obj_db.getCurrStatus(self.IS_OPERATING)
            new_time_update, new_status = self.obj_db.getNewStatus(self.IS_OPERATING)

            if green_light_status:
                operating_status_now = 1
            else:
                operating_status_now = 0

            if operating_status_now != new_status:
                self.obj_db.updateNewStatus(self.IS_OPERATING, operating_status_now)

            new_time_update, new_status = self.obj_db.getNewStatus(self.IS_OPERATING)

            self.log.info(f"Updated operating status -> prev: {prev_status}, new: {new_status}")

            if prev_status != new_status:
                if new_status == 1 and (datetime.now() - new_time_update).total_seconds() > 3:
                    self.obj_db.add_start_time(self.plant_date, self.curr_shift, True, self.IS_OPERATING)
                    self.obj_db.updateCurrStatus(self.IS_OPERATING, new_status)
                    self.operating_status = True

                elif new_status == 0 and (datetime.now() - new_time_update).total_seconds() > 3:
                    self.obj_db.add_stop_time(self.IS_OPERATING)
                    self.obj_db.updateCurrStatus(self.IS_OPERATING, new_status)
                    self.operating_status = False
        except Exception as e:
            self.log.error(f"[!] Error: {e}, while calculating operating time")
        self.log.info(f" ")

    def calculate_idle_time(self, yellow_light_status: bool) -> None:
        try:
            self.log.info("Starting Idle time calculation")
            prev_time_update, prev_status = self.obj_db.getCurrStatus(self.IS_IDLE)
            new_time_update, new_status = self.obj_db.getNewStatus(self.IS_IDLE)

            if yellow_light_status:
                idle_status_now = 1
            else:
                idle_status_now = 0

            if self.machine_maintenance_status:
                if self.idle_status:
                    self.log.info("Maintenance active -- stopping existing Idle time")
                    self.obj_db.add_stop_time(self.IS_IDLE)
                    self.obj_db.updateCurrStatus(self.IS_IDLE, 1)
                    self.idle_status = False
                else:
                    self.log.info("Maintenance active -- skipping Idle time start")
                return

            if idle_status_now != new_status:
                self.obj_db.updateNewStatus(self.IS_IDLE, idle_status_now)

            new_time_update, new_status = self.obj_db.getNewStatus(self.IS_IDLE)

            self.log.info(f"after checking machine status, updated these status..")
            self.log.info(f" prev_status:{prev_status}, new_status:{new_status}")

            if prev_status != new_status:
                if new_status == 0 and (datetime.now() - new_time_update).total_seconds() > 3:
                    self.obj_db.add_start_time(self.plant_date, self.curr_shift, True, self.IS_IDLE)
                    self.obj_db.updateCurrStatus(self.IS_IDLE, new_status)
                    self.idle_status = True

                elif new_status == 1 and (datetime.now() - new_time_update).total_seconds() > 3:
                    self.obj_db.add_stop_time(self.IS_IDLE)
                    self.obj_db.updateCurrStatus(self.IS_IDLE, new_status)
                    self.idle_status = False
                else:
                    pass
        except Exception as e:
            self.log.error(f"[!] Error: {e}, while calculating idle time ")
        self.log.info(f" ")

    def calculate_breakdown_time(self, red_light_status: bool) -> None:
        try:
            self.log.info("Starting breakdown time calculation")

            prev_time_update, prev_status = self.obj_db.getCurrStatus(self.IS_BREAKDOWN)
            new_time_update, new_status = self.obj_db.getNewStatus(self.IS_BREAKDOWN)

            if red_light_status:
                status_now = 1
            else:
                status_now = 0

            if self.machine_maintenance_status:
                if self.breakdown_status:
                    self.log.info("Maintenance active -- stopping existing breakdown")
                    self.obj_db.add_stop_time(self.IS_BREAKDOWN)
                    self.obj_db.updateCurrStatus(self.IS_BREAKDOWN, 1)
                    self.breakdown_status = False
                else:
                    self.log.info("Maintenance active -- skipping breakdown start")
                return

            if status_now != new_status:  # here updating new status in db
                self.obj_db.updateNewStatus(self.IS_BREAKDOWN, status_now)

            new_time_update, new_status = self.obj_db.getNewStatus(self.IS_BREAKDOWN)

            if prev_status != new_status:
                if new_status == 0 and (datetime.now() - new_time_update).total_seconds() > 10:
                    self.obj_db.add_start_time(self.plant_date, self.curr_shift, True, self.IS_BREAKDOWN)
                    self.obj_db.updateCurrStatus(self.IS_BREAKDOWN, new_status)
                    self.breakdown_status = True

                elif new_status == 1 and (datetime.now() - new_time_update).total_seconds() > 10:
                    self.obj_db.add_stop_time(self.IS_BREAKDOWN)
                    self.obj_db.updateCurrStatus(self.IS_BREAKDOWN, new_status)
                    self.breakdown_status = False
                else:
                    pass
        except Exception as e:
            self.log.error(f"[!] Error: {e}, while calculating breakdown time ")

        self.log.info(f" ")

    def calculations(self) -> None:
        try:
            self.operating_time = self.obj_db.get_total_duration(self.plant_date, self.curr_shift, self.IS_OPERATING)
            self.idle_time = self.obj_db.get_total_duration(self.plant_date, self.curr_shift, self.IS_IDLE)
            self.breakdown_time = self.obj_db.get_total_duration(self.plant_date, self.curr_shift, self.IS_BREAKDOWN)

            loss_time = self.idle_time + self.breakdown_time

            cycle_time = 1.33
            self.working_time_so_far, planned_breaks_dur = self.obj_conversions.get_working_time_so_far(self.curr_shift)

            planned_breaks_dur = planned_breaks_dur.total_seconds()

            self.target_count = round((self.working_time_so_far / cycle_time))
        except ArithmeticError as AE:
            self.log.error(AE)
            self.operating_time = 0
            self.idle_time = 0
            loss_time = 0
            planned_breaks_dur = 0
        except Exception as e:
            self.log.error(f'Error: {e}, while calculating operating_time')
            self.operating_time = 0
            self.idle_time = 0
            loss_time = 0
            planned_breaks_dur = 0

        try:
            performance = self.part_count / self.target_count
            self.log.info(f"[x]**performance: {performance}")
        except ArithmeticError as AE:
            performance = 0
            self.log.error(AE)

        try:
            quality = 1
            if self.part_count != 0:
                quality = (self.part_count - self.reject_count) / self.part_count
                self.log.info(f"[x]**quality: {quality}")
        except ArithmeticError as AE:
            quality = 0
            self.log.error(AE)

        try:
            availability = self.operating_time / self.working_time_so_far
            self.log.info(f"[x]**availability: {availability}")
        except ArithmeticError as AE:
            availability = 0
            self.log.error(AE)

        try:
            OEE = (availability * performance * quality)
            self.log.info(f"[*]OEE(A x E x Q): {OEE * 100}")
        except ArithmeticError as AE:
            OEE = 0
            self.log.error(AE)

        try:
            if self.operating_time > self.working_time_so_far:
                self.operating_time = self.working_time_so_far

            if self.idle_time > self.working_time_so_far:
                self.idle_time = self.working_time_so_far

            if self.breakdown_time > self.working_time_so_far:
                self.breakdown_time = self.working_time_so_far

            if loss_time > self.working_time_so_far:
                loss_time = self.working_time_so_far

            self.tele_payload['date'] = self.plant_date
            self.tele_payload["shift"] = self.curr_shift
            self.tele_payload["part_count"] = self.part_count
            self.tele_payload['reject_count'] = self.reject_count
            self.tele_payload['loss_time'] = round(loss_time / 60, 2)
            self.tele_payload['ready_time'] = round(self.idle_time / 60, 2)
            self.tele_payload['machine_util'] = round(performance * 100, 2)
            self.tele_payload['day_quality'] = round(quality * 100, 2)
            self.tele_payload['availability_percent'] = round(availability * 100, 2)
            self.tele_payload['day_Oee'] = round(OEE * 100, 2)
            self.tele_payload['breakdown_status'] = self.breakdown_status

            self.api_payload['date_'] = self.plant_date
            self.api_payload['shift'] = self.curr_shift
            self.api_payload['machine_name'] = self.machine_name
            self.api_payload['part_count'] = self.part_count
            self.api_payload['reject_count'] = self.reject_count
            self.api_payload['healthy_time'] = round(self.operating_time / 60, 2)
            self.api_payload['stop_time'] = round(loss_time / 60, 2)
            self.api_payload['ready_time'] = round(self.idle_time / 60, 2)
            self.api_payload['planned_time'] = round(self.working_time_so_far / 60, 2)
            self.api_payload['performance'] = round(performance * 100, 2)
            self.api_payload['availability'] = round(availability * 100, 2)
            self.api_payload['quality'] = round(quality * 100, 2)
            self.api_payload['oee'] = round(OEE * 100, 2)

            self.attr_payload['healthy_time'] = round(self.operating_time / 60, 2)
            self.attr_payload['ready_time'] = round(self.idle_time / 60, 2)
            self.attr_payload['loss_time'] = round(loss_time / 60, 2)
            self.attr_payload['A_cycle_parts'] = self.obj_db.get_target_count(self.plant_date, 'A')
            self.attr_payload['A_real_parts'] = self.obj_db.get_part_count(self.plant_date, 'A')
            self.attr_payload['B_cycle_parts'] = self.obj_db.get_target_count(self.plant_date, 'B')
            self.attr_payload['B_real_parts'] = self.obj_db.get_part_count(self.plant_date, 'B')
            self.attr_payload['C_cycle_parts'] = self.obj_db.get_target_count(self.plant_date, 'C')
            self.attr_payload['C_real_parts'] = self.obj_db.get_part_count(self.plant_date, 'C')
            self.attr_payload['G_cycle_parts'] = self.obj_db.get_target_count(self.plant_date, 'G')
            self.attr_payload['G_real_parts'] = self.obj_db.get_part_count(self.plant_date, 'G')
            self.attr_payload['machine_name'] = self.machine_name
            self.attr_payload['part_count'] = self.part_count
            self.attr_payload['reject_count'] = self.reject_count
            self.attr_payload['healthy_time'] = round(self.operating_time / 60, 2)
            self.attr_payload['ready_time'] = round(self.idle_time / 60, 2)
            self.attr_payload['planned_time'] = round(self.working_time_so_far / 60, 2)
            self.attr_payload['performance'] = round(performance * 100, 2)
            self.attr_payload['availability'] = round(availability * 100, 2)
            self.attr_payload['quality'] = round(quality * 100, 2)
            self.attr_payload['oee'] = round(OEE * 100, 2)

            self.log.info(f"")
            self.log.info(f"====== Calculated Parameters ========= ")
            self.log.info(f"[+]target_count: {self.target_count}")
            self.log.info(f"[+]planned_time: {round(self.working_time_so_far / 60, 2)}")
            self.log.info(f"[+]planned_breaks: {round(planned_breaks_dur / 60, 2)}")
            self.log.info(f"[+]operating_time: {round(self.operating_time / 60, 2)}")
            self.log.info(f"[+]idle_time: {round(self.idle_time / 60, 2)}")
            self.log.info(f"[+]breakdown_time: {round(self.breakdown_time / 60, 2)}")
            self.log.info(f"[+]performance: {round(performance * 100, 2)}")
            self.log.info(f"[+]availability: {round(availability * 100, 2)}")
            self.log.info(f"[+]quality: {round(quality * 100, 2)}")
            self.log.info(f"[+]oee: {round(OEE * 100, 2)}")
            self.log.info(f"===========================================")
            self.log.info(f"")

            self.log.info(f"OEE: {self.tele_payload}")
        except Exception as AE:
            self.log.error(f"[!]{AE}")

    def reset_on_date_shift_changed(self) -> None:
        try:
            # getting date&shift for checking.
            self.plant_date, self.curr_shift = self.obj_db.get_misc_data()

            '''Subtract shift-A start time from today's date to change the date at 6:30 AM instead of midnight.'''
            newDate = (datetime.today() - timedelta(hours=self.obj_conversions.shift_a_start.hour,
                                                    minutes=self.obj_conversions.shift_a_start.minute)).strftime("%F")

            self.general_shift_status = self.fetch_general_shift_status()

            self.new_shift = self.obj_conversions.get_current_shift(self.general_shift_status)
            self.log.info(f'curr_shift: {self.curr_shift}, new_shift: {self.new_shift}')

            if self.plant_date != newDate:
                self.log.info(f"Date:({self.plant_date}) >>> updating >>> ({newDate}) ")
                log_cleaner.clean()
                self.obj_db.update_curr_date(newDate)
                self.FL_SHIFT_START_RESET = True

            if self.curr_shift != self.new_shift:
                self.log.info(f"Shift: {self.curr_shift} >>> updating >>> {self.new_shift}")
                self.FL_SHIFT_START_RESET = True

            if self.FL_SHIFT_START_RESET:
                self.calculations()
                self.post_to_telemetry()

                # After posting prev shift data updated date&shift after shift or date changed.
                self.obj_db.update_curr_shift(self.new_shift)
                self.plant_date, self.curr_shift = self.obj_db.get_misc_data()
                self.log.info(f'[#]curr_date:{self.plant_date}, curr_shift:{self.curr_shift}')

                self.obj_db.add_stop_time(self.IS_OPERATING)
                self.obj_db.add_stop_time(self.IS_IDLE)
                self.obj_db.add_stop_time(self.IS_BREAKDOWN)

                # Resetting the machine running status
                self.log.info(f'Resetting machine running status in db')
                self.obj_db.updateCurrStatus(self.IS_OPERATING, 1)
                self.obj_db.updateNewStatus(self.IS_OPERATING, 1)
                self.obj_db.updateCurrStatus(self.IS_IDLE, 1)
                self.obj_db.updateNewStatus(self.IS_IDLE, 1)
                self.obj_db.updateCurrStatus(self.IS_BREAKDOWN, 1)
                self.obj_db.updateNewStatus(self.IS_BREAKDOWN, 1)

                self.breakdown_status = False

                self.obj_modbus.write_counter(self.part_count_addr)
                self.obj_modbus.write_counter(self.reject_count_addr)
                self.part_count = 0
                self.prev_part_count = 0
                self.obj_db.add_production_data(self.plant_date, self.curr_shift, self.target_count, self.part_count,
                                                self.reject_count, self.operating_time, self.idle_time,
                                                self.breakdown_time)
                self.FL_SHIFT_START_RESET = False

        except Exception as e:
            self.log.error(f'Error:{e}, while reset on date shift changed')

    def main(self) -> None:
        try:
            while 1:
                is_connected = self.obj_modbus.is_connected()
                self.log.info(f'is_connected: {is_connected}')
                try:
                    if is_connected:
                        break
                except Exception as e:
                    self.log.error(f"Connection failed., {e}")
                    time.sleep(2)

                if (time.time() - self.LAST_CON_ALERT_SENT) > 120:
                    self.log.info("Sending-Disconnection-Alert-------")
                    self.obj_alerts.alert_disconnected(True)
                    self.fl_one_time = True
                    self.LAST_CON_ALERT_SENT = time.time()

            if (time.time() - self.LAST_CON_ALERT_SENT) > 120:
                self.log.info("Sending-Connected-Alert...")
                self.obj_alerts.alert_disconnected(False)
                self.LAST_CON_ALERT_SENT = time.time()

            self.reset_on_date_shift_changed()

            if is_connected and self.curr_shift != 'NA':
                self.log.info(f"-------*{self.machine_name} [{self.plc_ip}] CONNECTION IS ACTIVE*----------")
                self.plant_date, self.curr_shift = self.obj_db.get_misc_data()

                self.part_count = self.obj_modbus.read_counter(self.part_count_addr)
                self.reject_count = self.obj_modbus.read_counter(self.reject_count_addr)
                operating_status = self.obj_modbus.read_status(self.green_light_addr)
                idle_status = self.obj_modbus.read_status(self.yellow_light_addr)
                breakdown_status = self.obj_modbus.read_status(self.red_light_addr)

                if self.part_count < 0 or self.part_count > 65000:
                    self.part_count = 0
                if self.reject_count < 0 or self.reject_count > 65000:
                    self.reject_count = 0

                sys_date = datetime.now().date()
                self.log.info(f'[*]System Date: {sys_date}')
                self.machine_maintenance_status = self.fetch_machine_maintenance_status(sys_date, self.curr_shift)

                if operating_status is not None:
                    self.calculate_operating_time(operating_status)
                    self.attr_payload['healthy_status'] = operating_status

                if idle_status is not None:
                    self.calculate_idle_time(idle_status)
                    self.attr_payload['ready_status'] = idle_status

                if breakdown_status is not None:
                    self.calculate_breakdown_time(breakdown_status)
                    self.attr_payload['stop_status'] = breakdown_status

                self.obj_db.add_production_data(self.plant_date, self.curr_shift, self.target_count, self.part_count,
                                                self.reject_count, self.operating_time, self.idle_time,
                                                self.breakdown_time)

                self.calling_breakdown_funcs()

                if (time.time() - self.LAST_OEE_PAYLOAD_SENT) > 60:
                    self.calculations()
                    self.post_to_api()
                    self.post_to_telemetry()
                    self.post_to_attribute()
                    send_system_info(HOST, self.access_token)
                    self.LAST_OEE_PAYLOAD_SENT = time.time()

                self.log.info(' ')
                self.log.info(f"[+] part_count: {self.part_count}")
                self.log.info(f"[+] reject_count: {self.reject_count}")
                self.log.info(f"[+] green-light/operating status: {operating_status}")
                self.log.info(f"[+] yellow-light/idle status: {idle_status}")
                self.log.info(f"[+] red-light/breakdown status: {breakdown_status}")
                self.log.info(f"[+]target_count: {self.target_count}")
                self.log.info(f"[+]planned_time: {round(self.working_time_so_far / 60, 2)}")
                self.log.info(f"[+]operating_time: {round(self.operating_time / 60, 2)}")
                self.log.info(f"[+]idle_time: {round(self.idle_time / 60, 2)}")
                self.log.info(f"[+]breakdown_time: {round(self.breakdown_time / 60, 2)}")
                self.log.info(' ')

        except Exception as e:
            self.log.error(f"[!]Error:{e}, in main code.")

        self.log.info('-' * 80)
        self.log.info(' ')

    def calling_breakdown_funcs(self) -> None:
        try:
            ongoing_breakdown_duration = self.obj_db.get_current_duration(self.plant_date, self.curr_shift,
                                                                          self.IS_BREAKDOWN)
            self.log.info(f"ongoing_breakdown_duration: {ongoing_breakdown_duration}")

            if ongoing_breakdown_duration > 900:
                bk_stat = True
                if not self.major_breakdown_alert_sent:
                    self.log.warn(f'Major Breakdown is going on')
                    self.post_breakdown_alert_to_whatsapp_grp()
                    self.major_breakdown_alert_sent = True
            else:
                bk_stat = False
                self.major_breakdown_alert_sent = False

            if (time.time() - self.LAST_BD_ALERT_SENT) > 120:
                self.obj_alerts.alert_major_bkdown(bk_stat)
                self.LAST_BD_ALERT_SENT = time.time()
        except Exception as e:
            self.log.error(f"[-] Can't calculate Breakdown Error: {e}")


if __name__ == "__main__":
    try:
        confHandler = ConfReader()
        if not os.path.exists(machine_config_file):
            confHandler.create_empty_csv(machine_config_file, ['machine_name',
                                                               'access_token',
                                                               'plc_ip',
                                                               'part_count_addr',
                                                               'reject_count_addr',
                                                               'red_light_addr',
                                                               'yellow_light_addr',
                                                               'green_light_addr'])
        if not os.path.exists(server_config_file):
            confHandler.create_empty_csv(server_config_file, ['HOST'])

        MACHINE_LIST = confHandler.parse_conf_csv(machine_config_file)
        SERVER_INFO = confHandler.parse_conf_csv(server_config_file)
        log.info(f'MACHINE_LIST: {MACHINE_LIST}')
        log.info(f'SERVER_INFO: {SERVER_INFO}')
        if SERVER_INFO:
            HOST = SERVER_INFO[0]['HOST']
            log.info(f"HOST: {HOST}")

        machine_obj = {}
        for machine in MACHINE_LIST:
            try:
                log.info(f'Operating with machine: {machine}')
                machine_obj[machine['machine_name']] = CL_MAIN(machine['machine_name'],
                                                               machine['access_token'],
                                                               machine['plc_ip'],
                                                               int(machine['part_count_addr']),
                                                               int(machine['reject_count_addr']),
                                                               int(machine['red_light_addr']),
                                                               int(machine['yellow_light_addr']),
                                                               int(machine['green_light_addr']))
                if machine_obj.get(machine['machine_name']):
                    log.info(f"[+] Init Successful for {machine['machine_name']}...")
                else:
                    log.error(f"[-] Init Failure for {machine['machine_name']}...")
            except Exception as e:
                log.error(f"[-] Error {e} While Initializing {machine['machine_name']}")

        thread_dict = {}
        while True:
            try:
                for machine_name, obj in machine_obj.items():
                    if thread_dict.get(machine_name) is None:
                        log.info(
                            f'[+] Thread is not configured for machine {machine_name}, configuring and starting it')
                        thread_dict[machine_name] = threading.Thread(target=obj.main)
                        thread_dict[machine_name].start()
                    else:
                        thread_obj = thread_dict[machine_name]
                        if not thread_obj.is_alive():
                            thread_dict[machine_name].join()
                            thread_dict[machine_name] = threading.Thread(target=obj.main)
                            thread_dict[machine_name].start()
                    time.sleep(0.8)
            except Exception as e:
                log.error(f"Error while threading [{e}]")
                time.sleep(3)
                log.info(f"Stopping threads...")
                for machine_name, obj in machine_obj.items():
                    if thread_dict.get(machine_name) is not None:
                        thread_dict.get(machine_name).join()
            time.sleep(1.5)
    except Exception as e:
        log.debug(f'[*] Error: {e}, while calling main.')
