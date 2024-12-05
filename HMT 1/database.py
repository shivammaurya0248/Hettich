import sqlite3
from logger import log
from datetime import datetime, timedelta
from shift import get_shift
import math


class DBHelper:
    def __init__(self):
        self.conn = sqlite3.connect("hettich.db")
        self.c = self.conn.cursor()
        self.c.execute("CREATE TABLE IF NOT EXISTS CountData("
                       "date_ DATE, "
                       "shift VARCHAR(2), "
                       "time_ DATETIME, "
                       "count INTEGER, "
                       "reject_count INTEGER)")

        self.c.execute('''CREATE TABLE IF NOT EXISTS misc(id INTEGER NOT NULL DEFAULT "1",
                                 current_date_ DATE NOT NULL DEFAULT (date('now','localtime')),
                                 current_shift VARCHAR(1) NOT NULL, current_hour INTEGER)''')

        self.c.execute("""CREATE TABLE IF NOT EXISTS
                          breakdown_data(
                              date_ STRING,
                              shift VARCHAR(2),
                              start_time STRING,
                              stop_time STRING,
                              duration FLOAT)
                          """)
        self.c.execute("""CREATE TABLE IF NOT EXISTS 
                                  up_time(
                                      date_ STRING,
                                      shift VARCHAR(2),
                                      healthy_duration FLOAT,
                                      stop_duration FLOAT,
                                      ready_duration FLOAT,
                                      planned_production_time FLOAT
                                      )
                                  """)

    def add_count_data(self, today, shift, count, rejection):
        try:
            time_ = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            self.c.execute("SELECT count FROM CountData "
                           "WHERE date_=? AND shift=?",
                           (today, shift))
            data = self.c.fetchone()
            if not data:
                # If no matching record is found, insert a new record
                self.c.execute(
                    "INSERT INTO CountData(date_, shift, time_, count,reject_count)"
                    "VALUES (?,?,?,?,?)",
                    (today, shift, time_, count, rejection))
                log.info(f'Successfully added data to the database.')
            else:
                self.c.execute("UPDATE CountData SET time_=?, count=?, reject_count = ?"
                               "WHERE date_=? AND shift = ?",
                               (time_, count, rejection, today, shift))
                log.info(f'Successfully updated data in the database.')

            self.conn.commit()
        except Exception as e:
            log.error(f'Error: {e}, Could not add GC data to the database.')

    def get_misc_data(self):
        self.c.execute('''SELECT * FROM misc''')
        check = self.c.fetchone()
        if check is None:
            self.add_misc_data()
        self.c.execute('''SELECT current_date_,current_shift FROM misc WHERE id=1''')
        try:
            data = self.c.fetchone()
            prev_day = data[0]
            prev_shift = data[1]
            return prev_day, prev_shift
        except Exception as e:
            log.error(f'ERROR: fetching misc data {e}')
            return 'N', 'N', 0

    def add_misc_data(self):
        try:
            shift = get_shift(False)
            hour_ = datetime.now().strftime("%H")
            date_ = (datetime.now() - timedelta(hours=7, minutes=30)).strftime("%F")
            self.c.execute("""INSERT INTO misc(id, current_shift, current_date_, current_hour)
                                  VALUES (?,?,?,?)""",
                           (1, shift, date_, hour_))
            self.conn.commit()
            log.info("Successful: Misc data added to the database.")
        except Exception as e:
            log.error(f'Error {e} Could not add Misc data to the Database')

    def update_curr_date(self, today):
        try:
            self.c.execute('''SELECT * FROM misc''')
            check = self.c.fetchone()
            if check is None:
                self.add_misc_data()
            self.c.execute("UPDATE misc SET current_date_=?", (today,))
            self.conn.commit()
            log.info(f'Successful: Date updated successfully in database.')
        except Exception as e:
            log.error(f"Error: {e}")

    def update_curr_shift(self, shift):
        try:
            self.c.execute('''SELECT * FROM misc''')
            check = self.c.fetchone()
            if check is None:
                self.add_misc_data()
            self.c.execute("UPDATE misc SET current_shift=?", (shift,))
            self.conn.commit()
            log.info(f'Successful: Shift updated successfully in database.')
        except Exception as e:
            log.error(f"Error: {e}")

    def get_count_data(self, today, shift):
        try:
            self.c.execute("SELECT count,reject_count FROM CountData "
                           "WHERE date_=? AND shift=?",
                           (today, shift))
            data = self.c.fetchone()
            if data:
                return data
            else:
                return None
        except Exception as e:
            log.error(f"Error : {e}")

    def add_breakdown_data(self, today, shift, breakdown_start, breakdown_stop, duration):
        self.c.execute("""SELECT * FROM breakdown_data WHERE date_=? AND shift=? """, (today, shift))
        data = self.c.fetchone()
        if data:
            self.c.execute("""UPDATE breakdown_data SET duration=? WHERE date_=? AND shift = ?""",
                           (duration, today, shift))
            log.info(f"Breakdown_duration updated into database")
            self.conn.commit()
        else:
            try:
                breakdown_start = breakdown_start.strftime("%H:%M:%S")
                breakdown_stop = breakdown_stop.strftime("%H:%M:%S")
                self.c.execute(
                    """INSERT INTO breakdown_data(date_,shift,start_time,stop_time,duration) VALUES (?,?,?,?,?)""",
                    (today, shift, breakdown_start, breakdown_stop, duration))
                self.conn.commit()
            except Exception as e:
                log.error(f"Error: {e}")

    def update_breakdown_data(self, today, shift, breakdown_start, breakdown_stop, duration):
        try:
            breakdown_stop = breakdown_stop.strftime("%H:%M:%S")
            self.c.execute("""UPDATE breakdown_data SET stop_time=?,duration=? WHERE date_=? AND shift=? """,
                           (breakdown_stop, duration, today, shift))
            self.conn.commit()
        except Exception as e:
            log.error(f"Error: {e}")

    def add_healthy_time(self, today, shift, healthy_time):
        try:

            self.c.execute("""SELECT healthy_duration FROM up_time WHERE date_=? AND shift=? """, (today, shift))
            healthy_data = self.c.fetchone()
            if healthy_data:
                if healthy_data[0] is None:
                    total_duration = 0 + healthy_time
                else:
                    total_duration = healthy_data[0] + healthy_time
                self.c.execute("""UPDATE up_time SET healthy_duration=? WHERE date_=? AND shift = ?""",
                               (round(total_duration, 2), today, shift))
                self.conn.commit()
                log.info(f"Healthy_duration UPDATED into database")
            else:
                self.c.execute("""INSERT INTO up_time(date_,shift,healthy_duration) VALUES (
                ?,?,?)""", (today, shift, healthy_time))
                self.conn.commit()
                log.info(f"healthy_duration ADDED into database")
        except Exception as e:
            log.error(f"Error: {e}")

    def add_stop_time(self, today, shift, stop_time):
        try:

            self.c.execute("""SELECT stop_duration FROM up_time WHERE date_=? AND shift=? """, (today, shift))
            stop_data = self.c.fetchone()
            if stop_data:
                if stop_data[0] is None:
                    total_duration = 0 + stop_time
                else:
                    total_duration = stop_data[0] + stop_time
                self.c.execute("""UPDATE up_time SET stop_duration=? WHERE date_=? AND shift = ?""",
                               (round(total_duration, 2), today, shift))
                self.conn.commit()
                log.info(f"stop_duration UPDATED into database")
            else:
                self.c.execute("""INSERT INTO up_time(date_,shift,stop_duration) VALUES (
                        ?,?,?)""", (today, shift, stop_time))
                self.conn.commit()
                log.info(f"stop_duration ADDED into database")
        except Exception as e:
            log.error(f"Error: {e}")

    def add_ready_time(self, today, shift, ready_time):
        try:
            self.c.execute("""SELECT ready_duration FROM up_time WHERE date_=? AND shift=? """, (today, shift))
            ready_data = self.c.fetchone()
            if ready_data:
                if ready_data[0] is None:
                    total_duration = 0 + ready_time
                else:
                    total_duration = ready_data[0] + ready_time
                self.c.execute("""UPDATE up_time SET ready_duration=? WHERE date_=? AND shift = ?""",
                               (round(total_duration, 2), today, shift))
                self.conn.commit()
                log.info(f"ready_duration UPDATED into database")
            else:
                self.c.execute("""INSERT INTO up_time(date_,shift,ready_duration) VALUES (
                        ?,?,?)""", (today, shift, ready_time))
                self.conn.commit()
                log.info(f"ready_duration ADDED into database")
        except Exception as e:
            log.error(f"Error: {e}")

    def fetch_data(self, today, shift):
        data = {
            "date_": today,
            "shift": shift,
            "machine_name": "HMT Assy-01",
            "part_count": 0,
            "reject_count": 0,
            "healthy_time": 0,
            "stop_time": 0,
            "ready_time": 0,
            "planned_time": 0
        }
        try:
            self.c.execute("SELECT count,reject_count FROM CountData "
                           "WHERE date_=? AND shift=?",
                           (today, shift))

            count_data = self.c.fetchone()
            if count_data:
                data["part_count"] = count_data[0] or 0
                data["reject_count"] = count_data[1] or 0

            self.c.execute("""SELECT healthy_duration,stop_duration,ready_duration,planned_production_time FROM 
            up_time WHERE date_ = ? AND shift = ?""", (today, shift))
            duration_data = self.c.fetchone()
            if duration_data:
                data["healthy_time"] = duration_data[0] or 0
                data["stop_time"] = duration_data[1] or 0
                data["ready_time"] = duration_data[2] or 0
                data["planned_time"] = duration_data[3] or 0
            return data
        except Exception as e:
            log.info(f"Error: {e}")
            return None

    def add_planned_production_time(self, today, shift, planned_time):
        self.c.execute("""SELECT planned_production_time FROM up_time WHERE date_=? AND shift=? """, (today, shift))
        data = self.c.fetchone()
        if data:
            self.c.execute("""UPDATE up_time SET planned_production_time=? WHERE date_=? AND shift = ?""",
                           (planned_time, today, shift))
            log.info(f"Planned Production time UPDATED into database")
            self.conn.commit()
        else:
            try:
                self.c.execute(
                    """INSERT INTO up_time(date_,shift,planned_production_time) VALUES (?,?,?)""",
                    (today, shift, round(planned_time, 2)))
                log.info(f"Planned Production time ADDED into database")
                self.conn.commit()
            except Exception as e:
                log.error(f"Error: {e}")

    def get_day_production(self, today):
        payload = {
            "total_part_count": 0,
            "total_reject_count": 0,
            "total_healthy": 0,
            "total_stop": 0,
            "total_ready": 0,
            "total_planned": 0
        }
        try:
            self.c.execute("""SELECT count,reject_count FROM CountData 
                               WHERE date_=?""", (today,))
            data = self.c.fetchall()
            payload["total_part_count"] = sum(t[0] or 0 for t in data)
            payload["total_reject_count"] = sum(t[1] or 0 for t in data)
            self.c.execute("""SELECT healthy_duration,stop_duration,ready_duration,planned_production_time FROM 
                        up_time WHERE date_ = ?""", (today,))
            duration_data = self.c.fetchall()
            payload["total_healthy"] = sum(t[0] or 0 for t in duration_data)
            payload["total_stop"] = sum(t[1] or 0 for t in duration_data)
            payload["total_ready"] = sum(t[2] or 0 for t in duration_data)
            payload["total_planned"] = sum(t[3] or 0 for t in duration_data)
            return payload
        except Exception as e:
            log.error(f"Error: {e}")

    def get_shift_data(self, date, shift):
        payload = {
            "A_real_time_parts": 0,
            "B_real_time_parts": 0,
            "C_real_time_parts": 0,
            "G_real_time_parts": 0,
            "A_cycle_time_parts": 0,
            "B_cycle_time_parts": 0,
            "C_cycle_time_parts": 0,
            "G_cycle_time_parts": 0,
        }
        if shift == 'G':
            self.c.execute('SELECT COALESCE(count, 0), COALESCE(reject_count, 0) FROM CountData WHERE date_=? AND '
                           'shift = ?',
                           (date, shift))
            data = self.c.fetchone()
            if data:
                payload["G_real_time_parts"] = data[0] + data[1]

            self.c.execute("""SELECT COALESCE(planned_production_time,0),COALESCE(stop_duration,0) FROM up_time WHERE 
            date_ = ? AND shift=?""", (date, shift))
            data = self.c.fetchone()
            #log.info(f"Planned and Stop : {data}")
            parts = math.floor(data[0]) * 55
            #log.info(f"cycle time parts (g) : {parts}")
            if data:
                if data[1] > data[0]:
                    payload["G_cycle_time_parts"] = 0
                else:
                    payload["G_cycle_time_parts"] = int(data[0] * 55)

        else:
            self.c.execute("""SELECT count , reject_count FROM CountData WHERE date_ =?""", (date,))
            data = self.c.fetchall()
            if data:
                count = 0
                for t in data:
                    count += 1
                    if count == 1:
                        payload["A_real_time_parts"] = sum(v or 0 for v in t)
                    if count == 2:
                        payload["B_real_time_parts"] = sum(v or 0 for v in t)
                    if count == 3:
                        payload["C_real_time_parts"] = sum(v or 0 for v in t)

            self.c.execute("""SELECT planned_production_time,stop_duration FROM up_time WHERE date_ = ?""", (date,))

            data = self.c.fetchall()
            if data:
                count = 0
                for t in data:
                    count += 1
                    if count == 1:
                        if t[0] is None:
                            payload["A_cycle_time_parts"] = 0
                        elif t[1] is None:
                            payload["A_cycle_time_parts"] = int(t[0] * 55)
                        else:
                            payload["A_cycle_time_parts"] = int(t[0] * 55)
                        # if t[1] is not None and t[0] is not None and t[1] > t[0]:
                        #     payload["A_cycle_time_parts"] = 0
                    if count == 2:
                        if t[0] is None:
                            payload["B_cycle_time_parts"] = 0
                        elif t[1] is None:
                            payload["B_cycle_time_parts"] = int(t[0] * 55)
                        else:
                            payload["B_cycle_time_parts"] = int((t[0]) * 55)
                        # if t[1] is not None and t[0] is not None and t[1] > t[0]:
                        #     payload["B_cycle_time_parts"] = 0
                    if count == 3:
                        if t[0] is None:
                            payload["C_cycle_time_parts"] = 0
                        elif t[1] is None:
                            payload["C_cycle_time_parts"] = int(t[0] * 55)
                        else:
                            payload["C_cycle_time_parts"] = int((t[0]) * 55)
                        # if t[1] is not None and t[0] is not None and t[1] > t[0]:
                        #     payload["C_cycle_time_parts"] = 0
        return payload

    def get_part_count_ing(self, date_, shift):  # getting the part_count_ing
        try:
            self.c.execute("""
            SELECT partCountIng FROM CountData WHERE date_ =? and shift =? """, (date_, shift))
            part_count_ing = self.c.fetchone()
            if part_count_ing:
                return part_count_ing[0]
            else:
                return None
        except Exception as e:
            log.error(f"Error in get_part_count_ing {e}")
            return None

    def fixing_reset_part_count(self, today, shift, partCountIng):
        try:
            time_ = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            self.c.execute("SELECT count FROM CountData "
                           "WHERE date_=? AND shift=?", (today, shift))
            data = self.c.fetchall()
            out = ''
            if len(data) != 0:
                self.c.execute("UPDATE CountData SET time_=?, partCountIng=? WHERE date_=? AND shift = ?",
                               (time_, partCountIng, today, shift))
                log.info('Successful FIXED part count ING in the database.')
                out = 'UPDATE'
            self.conn.commit()
            return out
        except Exception as e:
            log.error(f'Error Could not update partCountIng to the database. {e}')

    def get_reject_part_count_ing(self, date_, shift):  # getting the part_count_ing
        try:
            self.c.execute("""
            SELECT reject_part_count_ing FROM CountData WHERE date_ =? and shift =? """, (date_, shift))
            reject_part_count_ing = self.c.fetchone()
            if reject_part_count_ing:
                return reject_part_count_ing[0]
            else:
                return None
        except Exception as e:
            log.error(f"Error in get_part_count_ing {e}")
            return None

    def fixing_reset_reject_part_count(self, today, shift, partCountIng):
        try:
            time_ = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            self.c.execute("SELECT reject_count FROM CountData "
                           "WHERE date_=? AND shift=?", (today, shift))
            data = self.c.fetchall()
            out = ''
            if len(data) != 0:
                self.c.execute("UPDATE CountData SET time_=?, reject_part_count_ing=? WHERE date_=? AND shift = ?",
                               (time_, partCountIng, today, shift))
                log.info('Successful FIXED reject part count ING in the database.')
                out = 'UPDATE'
            self.conn.commit()
            return out
        except Exception as e:
            log.error(f'Error Could not update reject_part_count_ing to the database. {e}')

    def add_shift_start_data(self, today, shift, count, rejection, cpart, creject):
        try:
            time_ = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            self.c.execute("SELECT count FROM CountData "
                           "WHERE date_=? AND shift=?",
                           (today, shift))
            data = self.c.fetchone()
            if not data:
                # If no matching record is found, insert a new record
                self.c.execute(
                    "INSERT INTO CountData(date_, shift, time_, count,reject_count,partCountIng,reject_part_count_ing)"
                    "VALUES (?,?,?,?,?,?,?)",
                    (today, shift, time_, count, rejection, cpart, creject))
                log.info(f'Successfully added shift start data to the database.')
            # else:
            #     self.c.execute("UPDATE CountData SET time_=?, count=?, reject_count = ?"
            #                    "WHERE date_=? AND shift = ?",
            #                    (time_, count, rejection, today, shift))
            #     log.info(f'Successfully updated data in the database.')

            self.conn.commit()
        except Exception as e:
            log.error(f'Error: {e}, Could not add shift start data to the database.')
