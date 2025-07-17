from conversions import ShiftManager
import os
import sqlite3
import sys
import ast
import time
from datetime import timedelta
import datetime


class DBHelper:
    def __init__(self, db_name, logger):

        if getattr(sys, 'frozen', False):
            dirname = os.path.dirname(sys.executable)
        else:
            dirname = os.path.dirname(os.path.abspath(__file__))

        self.log = logger

        if not os.path.isdir(f"{dirname}/data/"):
            self.log.info("[-] data directory doesn't exists")
            try:
                os.mkdir(f"{dirname}/data/")
                self.log.info("[+] Created data dir successfully")
            except Exception as e:
                self.log.error(f"[-] Can't create data dir Error: {e}")

        self.db_name = f"{dirname}/data/{db_name}.db"

        self.connection = sqlite3.connect(self.db_name, check_same_thread=False)
        self.c = self.connection.cursor()

        try:
            self.c.execute("CREATE TABLE IF NOT EXISTS Production_Data("
                           "date_ DATE, "
                           "time_ DATETIME, "
                           "shift VARCHAR(2), "
                           "target_count INTEGER, "
                           "part_count INTEGER, "
                           "reject_count INTEGER, "
                           "operating_time INTEGER, "
                           "idle_time INTEGER, "
                           "breakdown_time INTEGER)")

            for table in ['OPERATING', 'IDLE', 'BREAKDOWN']:
                self.c.execute(f'''CREATE TABLE IF NOT EXISTS {table}(
                                   id INTEGER NOT NULL,
                                   date_ DATE NOT NULL,
                                   shift VARCHAR(1) NOT NULL,
                                   time_ DATETIME, 
                                   startTime DATETIME NOT NULL, 
                                   stopTime DATETIME,
                                   duration INTEGER,
                                   PRIMARY KEY (id AUTOINCREMENT))''')

            self.c.execute("CREATE TABLE IF NOT EXISTS mcStatus("
                           "id INTEGER, "
                           "timeUpdated DATETIME, "
                           "currStatus BOOLEAN DEFAULT 0, "
                           "timeLastChanged DATETIME, "
                           "newStatus BOOLEAN DEFAULT 0, "
                           "parameter STRING)")

            self.c.execute('''CREATE TABLE IF NOT EXISTS misc(
                                id INTEGER NOT NULL DEFAULT 1,
                                current_date_ DATE NOT NULL DEFAULT (date('now','localtime')),
                                current_shift VARCHAR(1) NOT NULL, 
                                current_hour INTEGER)''')

            self.c.execute("CREATE TABLE IF NOT EXISTS sync_data("
                           "ts INTEGER, "
                           "payload STRING)")

        except Exception as e:
            self.log.error(f"Error creating table: {e}")

    # region Production data Management Code
    def add_production_data(self, date_, shift, target_count, part_count, reject_count, operating_time, idle_time,
                            breakdown_time):
        try:
            time_ = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            self.c.execute("SELECT part_count FROM Production_Data WHERE date_=? AND shift=?", (date_, shift))
            data = self.c.fetchall()

            if len(data) == 0:
                self.c.execute(
                    "INSERT INTO Production_Data(date_, shift, time_, target_count, part_count, reject_count, operating_time, idle_time, breakdown_time) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (date_, shift, time_, target_count, part_count, reject_count, operating_time, idle_time,
                     breakdown_time)
                )
                out = 'INSERT'
            else:
                self.c.execute(
                    "UPDATE Production_Data SET time_=?, target_count=?, part_count=?, reject_count=?, operating_time=?, idle_time=?, breakdown_time=? "
                    "WHERE date_=? AND shift=?",
                    (time_, target_count, part_count, reject_count, operating_time, idle_time, breakdown_time, date_,
                     shift)
                )
                out = 'UPDATE'

            self.connection.commit()
            self.log.info(f'add_product_data output: {out}')
            return out
        except Exception as e:
            self.log.error(f'Error occurred while adding product data: {e}')

    def get_target_count(self, date_: str, shift: str) -> int:
        try:
            self.c.execute("SELECT target_count FROM Production_Data "
                           "WHERE date_=? AND shift=? ", (date_, shift))
            data = self.c.fetchone()
            if data:
                return data[0]
            else:
                return 0
        except Exception as e:
            self.log.error(f"Error fetching part count {e}")
            return 0

    def get_part_count(self, date_: str, shift: str) -> int:
        try:
            self.c.execute("SELECT part_count FROM Production_Data "
                           "WHERE date_=? AND shift=? ", (date_, shift))
            data = self.c.fetchone()
            if data:
                return data[0]
            else:
                return 0
        except Exception as e:
            self.log.error(f"Error fetching part count {e}")
            return 0

    def get_reject_count(self, date_: str, shift: str) -> int:
        try:
            self.c.execute("SELECT reject_count FROM Production_Data "
                           "WHERE date_=? AND shift=? ", (date_, shift))
            data = self.c.fetchone()
            if data:
                return data[0]
            else:
                return 0
        except Exception as e:
            self.log.error(f"Error fetching part count {e}")
            return 0

    def get_prev_part_count(self, date_, shift):
        try:
            self.c.execute("""SELECT part_count FROM Production_Data 
                            WHERE date_ = ? and shift = ? 
                            ORDER BY time_ DESC LIMIT 1
                            """, (date_, shift))
            total_count = self.c.fetchone()
            if total_count:
                return total_count[0]
            else:
                return 0
        except Exception as e:
            self.log.error(f"Error in get_prev_part_count {e}")
            return 0


    def add_start_time(self, date_, shift, status, table_name):
        try:
            self.add_stop_time(table_name)
            if status:
                now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                self.c.execute(f'''INSERT INTO {table_name}(date_, shift, time_, startTime)
                                   VALUES (?,?,?,?)''', (date_, shift, now, now))
                self.connection.commit()
                print(f'Successful: {table_name} time added to the database')
        except Exception as e:
            print(f'Error: {e}, Could not add start time to {table_name}')

    def add_stop_time(self, table_name):
        try:
            now = datetime.datetime.now()
            now_str = now.strftime("%Y-%m-%d %H:%M:%S")
            self.c.execute(f'''SELECT startTime, id FROM {table_name} WHERE stopTime IS NULL''')
            for start_time, id_ in self.c.fetchall():
                start_dt = datetime.datetime.strptime(start_time, "%Y-%m-%d %H:%M:%S")
                if now > start_dt:
                    duration = (now - start_dt).seconds
                    self.c.execute(f'''UPDATE {table_name} SET time_=?, stopTime=?, duration=?
                                       WHERE id=? AND stopTime IS NULL''',
                                   (now_str, now_str, duration, id_))
                else:
                    fallback = start_dt + datetime.timedelta(seconds=10)
                    self.c.execute(f'''UPDATE {table_name} SET time_=?, stopTime=?, duration=?
                                       WHERE id=? AND stopTime IS NULL''',
                                   (now_str, fallback, 10, id_))
            self.connection.commit()
        except Exception as e:
            print(f'Error: {e}, Could not stop time in {table_name}')

    def add_duration_to_null_blocks(self, table_name):
        try:
            now = datetime.datetime.now()
            now_str = now.strftime("%Y-%m-%d %H:%M:%S")
            self.c.execute(f'''SELECT startTime, id FROM {table_name} WHERE stopTime IS NULL''')
            for start_time, id_ in self.c.fetchall():
                start_dt = datetime.datetime.strptime(start_time, "%Y-%m-%d %H:%M:%S")
                duration = (now - start_dt).seconds
                self.c.execute(f'''UPDATE {table_name} SET time_=?, duration=? 
                                   WHERE stopTime IS NULL AND id=?''',
                               (now_str, duration, id_))
            self.connection.commit()
        except Exception as e:
            print(f'Error: {e}, Could not update durations in {table_name}')

    def get_current_duration(self, date_, shift, table_name):
        try:
            self.add_duration_to_null_blocks(table_name)
            self.c.execute(f'''SELECT duration FROM {table_name}
                              WHERE date_=? AND shift=? ORDER BY id DESC LIMIT 1''',
                           (date_, shift))
            b_time = self.c.fetchone()
            if b_time is not None:
                return b_time[0]
            else:
                return 0
        except Exception as e:
            self.log.error(f'Error: {e}, Could not get daily breakdown.')
            return 0

    def get_total_duration(self, date_, shift, table_name):
        try:
            self.add_duration_to_null_blocks(table_name)
            self.c.execute(f'''SELECT SUM(duration) FROM {table_name}
                               WHERE date_=? AND shift=?''', (date_, shift))
            result = self.c.fetchone()
            return result[0] if result and result[0] else 0
        except Exception as e:
            print(f'Error: {e}, Could not fetch total duration from {table_name}')
            return 0

    def get_duration_count(self, today, shift, table_name):
        try:
            self.c.execute(f'''SELECT COUNT(id) FROM {table_name}
                               WHERE date_=? AND shift=?''', (today, shift))
            result = self.c.fetchone()
            return result[0] if result else 0
        except Exception as e:
            print(f'Error: {e}, Could not fetch idle count from {table_name}')
            return 0

    def add_misc_data(self):
        try:
            shift = ShiftManager(self.log).get_current_shift(False)
            hour_ = datetime.datetime.now().strftime("%H")
            date_ = (datetime.datetime.now() - timedelta(hours=6, minutes=30)).strftime("%F")
            self.c.execute("""INSERT INTO misc(id, current_shift, current_date_, current_hour)
                                VALUES (?,?,?,?)""",
                           (1, shift, date_, hour_))
            self.connection.commit()
            self.log.info("Successful: Misc data added to the database.")
        except Exception as e:
            self.log.error(f'Error {e} Could not add Misc data to the Database')

    def get_curr_shift(self):
        self.c.execute('''SELECT * FROM misc''')
        check = self.c.fetchone()
        if check is None:
            self.add_misc_data()
        self.c.execute('''SELECT current_shift FROM misc WHERE id=1''')
        try:
            data = self.c.fetchone()[0]
        except:
            data = 'N'
        return data

    def get_curr_date(self):
        self.c.execute('''SELECT * FROM misc''')
        check = self.c.fetchone()
        if check is None:
            self.add_misc_data()
        self.c.execute('''SELECT current_date_ FROM misc WHERE id=1''')
        try:
            data = self.c.fetchone()[0]
        except:
            data = 'N'
        return data

    def get_misc_data(self):
        self.c.execute('''SELECT * FROM misc''')
        check = self.c.fetchone()
        if check is None:
            self.add_misc_data()
        self.c.execute('''SELECT current_date_,current_shift FROM misc WHERE id=1''')
        try:
            data = self.c.fetchone()
            prevD = data[0]
            prevS = data[1]
            return prevD, prevS
        except Exception as e:
            self.log.error(f'ERROR: fetching misc data {e}')
            return 'N', 'N', 0

    def update_curr_date(self, today):
        self.c.execute('''SELECT * FROM misc''')
        check = self.c.fetchone()
        if check is None:
            self.add_misc_data()
        self.c.execute("UPDATE misc SET current_date_=?", (today,))
        self.connection.commit()
        # self.log.info(f'Successful: Date updated successfully in database.')

    def update_curr_shift(self, shift):
        self.c.execute('''SELECT * FROM misc''')
        check = self.c.fetchone()
        if check is None:
            self.add_misc_data()
        self.c.execute("UPDATE misc SET current_shift=?", (shift,))
        self.connection.commit()
        self.log.info(f'Successful: Shift updated successfully in database.')

    def update_curr_hour(self, hour_):
        self.c.execute('''SELECT * FROM misc''')
        check = self.c.fetchone()
        if check is None:
            self.add_misc_data()
        self.c.execute("UPDATE misc SET current_hour=?", (hour_,))
        self.connection.commit()
        self.log.info(f'Successful: Hour updated successfully in database.')

    def addStatusData(self, parameter):
        try:
            self.c.execute('''INSERT INTO mcStatus(parameter, id, timeUpdated, currStatus, timeLastChanged, newStatus)
                               VALUES(?, ?, datetime(CURRENT_TIMESTAMP, 'localtime'), ?, 
                                      datetime(CURRENT_TIMESTAMP, 'localtime'), ?)''',
                           (parameter, 1, 1, 1))
            self.connection.commit()
            self.log.info(f'Successful: Status Data inserted for parameter "{parameter}" in the database.')
        except Exception as e:
            self.log.error(f'ERROR: {e} Could not add status data for parameter "{parameter}" to the database.')

    def updateCurrStatus(self, parameter, status):
        self.c.execute('''SELECT * FROM mcStatus WHERE parameter=?''', (parameter,))
        check = self.c.fetchone()
        if check is None:
            self.addStatusData(parameter)
        try:
            self.c.execute('''UPDATE mcStatus SET timeUpdated=datetime(CURRENT_TIMESTAMP, 'localtime'), 
                                currStatus=? WHERE parameter=?''',
                           (status, parameter))
            self.log.debug(f'Successfully updated current status for parameter "{parameter}" in the database.')
            self.connection.commit()
        except Exception as e:
            self.log.error(f'Error: {e}, Current status for parameter "{parameter}" NOT Updated')

    def updateNewStatus(self, parameter, status):
        self.c.execute('''SELECT * FROM mcStatus WHERE parameter=?''', (parameter,))
        check = self.c.fetchone()
        if check is None:
            self.addStatusData(parameter)
        try:
            self.c.execute('''UPDATE mcStatus SET timeLastChanged=datetime(CURRENT_TIMESTAMP, 'localtime'), 
                                newStatus=? WHERE parameter=?''',
                           (status, parameter))
            self.log.debug(f'Successfully updated new status for parameter "{parameter}" in the database.')
            self.connection.commit()
        except Exception as e:
            self.log.error(f'Error: {e}, New status for parameter "{parameter}" NOT Updated')

    def getCurrStatus(self, parameter):
        self.c.execute('''SELECT * FROM mcStatus WHERE parameter=?''', (parameter,))
        check = self.c.fetchone()
        if check is None:
            self.addStatusData(parameter)
        self.c.execute('''SELECT timeUpdated, currStatus FROM mcStatus WHERE parameter=? LIMIT 1''', (parameter,))
        try:
            timeUpdated, currStatus = self.c.fetchone()
            timeUpdated = datetime.datetime.strptime(timeUpdated, "%Y-%m-%d %H:%M:%S")
        except:
            timeUpdated, currStatus = [None, None]
        return timeUpdated, currStatus

    def getNewStatus(self, parameter):
        self.c.execute('''SELECT * FROM mcStatus WHERE parameter=?''', (parameter,))
        check = self.c.fetchone()
        if check is None:
            self.addStatusData(parameter)
        self.c.execute('''SELECT timeLastChanged, newStatus FROM mcStatus WHERE parameter=? LIMIT 1''', (parameter,))
        try:
            timeLastChanged, newStatus = self.c.fetchone()
            timeLastChanged = datetime.datetime.strptime(timeLastChanged, "%Y-%m-%d %H:%M:%S")
        except:
            timeLastChanged, newStatus = [None, None]
        return timeLastChanged, newStatus

    def add_sync_data(self, payload):
        try:
            ts = int(time.time() * 1000)
            self.c.execute('''INSERT INTO sync_data(ts, payload) VALUES (?,?)''', (ts, str(payload)))
            self.log.info('Successful Sync Payload Added to the database')
            self.connection.commit()
        except Exception as e:
            self.log.error(f'ERROR {e} Sync Data not added to the database')

    def get_sync_data(self):
        try:
            self.c.execute('''SELECT * FROM sync_data order by ts ASC LIMIT 200''')
            data = self.c.fetchall()
            if len(data):
                data_payload = [{"ts": int(item[0]),
                                 "values": ast.literal_eval(item[1])}
                                for item in data]
                # splitting data_payload in list of lists of 100 items those 100 items are objects
                return [data_payload[i:i + 100] for i in range(0, len(data_payload), 100)]
            else:
                return []
        except Exception as e:
            self.log.error(f'ERROR {e} No Sync Data available')
            return []

    def clear_sync_data(self, ts):
        try:
            # deleting the payload where ts is less than or equal to ts
            self.c.execute("""DELETE FROM sync_data WHERE ts<=?""", (ts,))
            self.connection.commit()
            self.log.info(f"Successful, Cleared Sync payload from the database for {ts}")
            return True
        except Exception as e:
            self.log.error(f'Error in clear_sync_data {e} No sync Data to clear')
