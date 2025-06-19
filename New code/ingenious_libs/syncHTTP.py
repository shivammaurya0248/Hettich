import logging
import os
import sys
import ast
import time
import math
import sqlite3
import datetime
import requests
from .logMan import ILogs, default_dir

ilog = ILogs('main', 'info', False)


def check_data_dir(dirname):
    # checking and creating logs directory here
    if not os.path.isdir(dirname):
        ilog.info(f"[-] logs directory doesn't exists {dirname}")
        try:
            os.mkdir(dirname)
            ilog.info("[+] Created logs dir successfully")
        except Exception as e:
            ilog.info(f"[-] Can't create dir logs Error: {e}")


class DBHelper:
    """
    It is a DBHelper Class for syncing the payload with server

    :IMPORTANT: CLASS NOT FOR EXTERNAL USE
    """
    def __init__(self, database_name, data_path=default_dir):

        self.db_path = os.path.join(data_path, 'data')
        check_data_dir(self.db_path)

        self.connection = sqlite3.connect(os.path.join(f"{self.db_path}/{database_name}.db"))
        self.cursor = self.connection.cursor()
        self.cursor.execute("""CREATE TABLE IF NOT EXISTS sync_data(
        start_payload_time DATETIME DEFAULT (datetime('now', 'localtime')),
        stop_payload_time DATETIME DEFAULT (datetime('now', 'localtime')),
        start_payload STRING,
        stop_payload STRING,
        machine_id STRING,
        hour_ INTEGER,
        date_ DATETIME
        )""")

    # region Syncronization functions
    def current_time_is_ok(self):
        try:
            """
            Here We are checking if current time is less than previous save time
            if current_time is less than previous save time then return False
            else return True
            and if there is nothing in db then return True
            """
            self.cursor.execute("""SELECT start_payload_time FROM sync_data ORDER BY oid DESC LIMIT 1""")
            last_saved_time = self.cursor.fetchone()
            if last_saved_time is None:
                ilog.debug("[+] Nothing in DB Assuming time OK returning True")
                return True

            if (temp_con := datetime.datetime.now() > datetime.datetime.strptime(last_saved_time[0],
                                                                                 '%Y-%m-%d %H:%M:%S')):
                ilog.debug(f"[+] Current Time {datetime.datetime.now().isoformat(' ')} > last_saved_time {last_saved_time[0]} : [{temp_con}]")
                return True

            ilog.debug(f"[+] Current Time {datetime.datetime.now().isoformat(' ')} > last_saved_time {last_saved_time[0]} : [{temp_con}]")
        except Exception as e:
            ilog.error(f"[+] Error Getting last sync_time {e}")
            return True

        return False

    def add_sync_data(self, payload, machine_id, hour_, date_):
        try:
            if not self.current_time_is_ok():
                raise Exception(": Current Time is Less than the last Saved Time :")

            # hour_ = datetime.datetime.now().hour
            ts = int(time.time() * 1000)
            new_payload = dict()
            for i in payload.items():
                if math.isnan(i[1]):
                    data = 'nan'
                else:
                    data = i[1]
                new_payload[i[0]] = data
            ilog.info(new_payload)

            new_payload = {
                "ts": ts,
                "values": new_payload
            }
            self.cursor.execute("""SELECT * FROM sync_data WHERE date_=? AND hour_=? AND machine_id=?""",
                                (date_, hour_, machine_id))
            data = self.cursor.fetchone()
            if data is None:
                self.cursor.execute('''INSERT INTO sync_data(start_payload, stop_payload, machine_id, hour_, date_) 
                                    VALUES (?,?,?,?,?)''',
                                    (str(new_payload), str(new_payload), machine_id, hour_, date_))
            else:
                self.cursor.execute(
                    """UPDATE sync_data set stop_payload=?, stop_payload_time=(datetime('now', 'localtime')) 
                    where machine_id=? and hour_=? and date_=?""",
                    (str(new_payload), machine_id, hour_, date_))

            ilog.info('Successful Sync Payload Added to the database')
            self.connection.commit()
        except Exception as e:
            ilog.error(f'ERROR {e} Sync Data not added to the database')

    def get_sync_data(self):
        try:
            payload_size_limit = 20
            sync_payload = list()
            self.cursor.execute('''SELECT machine_id FROM sync_data group by machine_id''')
            machine_ids = self.cursor.fetchall()
            ilog.info(machine_ids)
            if machine_ids is not None:
                for at in machine_ids:
                    if at is not None:
                        self.cursor.execute('''
                        SELECT start_payload, stop_payload, date_, hour_, machine_id FROM sync_data 
                        where machine_id=? order by date_, hour_ ASC
                        ''',
                                            (at[0],))
                        # data = self.cursor.fetchall()
                        data = self.cursor.fetchmany(payload_size_limit)
                        ilog.debug(data)
                        if len(data):
                            sync_payload = [{
                                'payload': [ast.literal_eval(item[0]), ast.literal_eval(item[1])],
                                'date_': item[2],
                                'hour_': item[3],
                                'machine_id': item[4]
                            } for item in data]
                return sync_payload
            return []
        except Exception as e:
            ilog.error(f'ERROR {e} No Sync Data available')
            return []

    def clear_sync_data(self, date_, hour_, machine_id):
        try:
            # deleting the payload where ts is less than or equal to ts
            self.cursor.execute("""DELETE FROM sync_data WHERE date_=? and hour_=? and machine_id=?""",
                                (date_, hour_, machine_id))
            self.connection.commit()
            ilog.info(f"Successful, Cleared Sync payload from the database for {date_}, {hour_}, {machine_id}")
            return True
        except Exception as e:
            ilog.error(f'Error in clear_sync_data {e} No sync Data to clear')
            return False

    # endregion


class SyncHTTP(DBHelper):
    """
    Class for sending Timeseries Data to the server

    class init params are:
        host = url_of_your_server
        port = port
        sync_flag = want to sync data or not
        timeout = default 2

    :parameter for post_data:
        payload, machine_id, access_token
        don't use '$' sign in machine_id else I will have to update it and that will take time
        so be generous and kind enough to not use $ in machine_name

        Author: Shivam Maurya
        organisation: Ingenious Techzoid
    """
    def __init__(self, host, port=8080, sync_flag=True, timeout=2):
        super().__init__("sync")
        self.headers = {'Content-Type': 'Application/Json'}
        self.host = host
        self.port = port
        self.sync_flag = sync_flag
        self.timeout = timeout

    def post_data(self, payload, machine_id, access_token):
        """posting OEE DATA to the SERVER"""
        url = f'http://{self.host}:{self.port}/api/v1/{access_token}/telemetry'
        ilog.info("[+] Sending data to server")
        try:
            send_req = requests.post(url, json=payload, headers=self.headers, timeout=self.timeout)
            ilog.info(send_req.status_code)
            send_req.raise_for_status()
            if self.sync_flag:
                sync_data = self.get_sync_data()
                ilog.debug(sync_data)
                for row in sync_data:
                    payload_sync = row.get('payload')
                    date_sync = row.get('date_')
                    hour_sync = row.get('hour_')
                    machine_data_sync = row.get('machine_id')
                    machine_id_sync, access_token_sync = machine_data_sync.split('$')
                    ilog.info(f"[+] ----- {machine_id_sync} - {date_sync} - {hour_sync}")
                    ilog.debug(payload_sync)
                    try:
                        url = f'http://{self.host}:{self.port}/api/v1/{access_token_sync}/telemetry'
                        ilog.info(url)
                        sync_req = requests.post(url, json=payload_sync, headers=self.headers, timeout=self.timeout)
                        ilog.info(f"[*] Sync status code {sync_req.status_code}")
                        ilog.debug(sync_req.text)
                        sync_req.raise_for_status()
                        ilog.info(f"[+] clearing sync for -> {machine_id_sync} - {date_sync} - {hour_sync}")
                        self.clear_sync_data(date_sync, hour_sync, machine_data_sync)
                    except Exception as e:
                        ilog.error(f"[-] Error in sending SYNC Cycle time data {e}")
                    break
                else:
                    ilog.info("(^-^) No data to sync")
        except Exception as e:
            date_ = datetime.datetime.now().strftime("%Y-%m-%d")
            hour_ = datetime.datetime.now().hour
            ilog.error(f"[-] Error in sending Cycle time data {e}")
            if self.sync_flag:
                self.add_sync_data(payload, f"{machine_id}${access_token}", hour_, date_)


