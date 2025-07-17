import os
import sqlite3
import sys
from datetime import timedelta
import datetime


class DBHelper:
    def __init__(self, db_name):

        if getattr(sys, 'frozen', False):
            dirname = os.path.dirname(sys.executable)
        else:
            dirname = os.path.dirname(os.path.abspath(__file__))

        if not os.path.isdir(f"{dirname}/data/"):
            print("[-] data directory doesn't exists")
            try:
                os.mkdir(f"{dirname}/data/")
                print("[+] Created data dir successfully")
            except Exception as e:
                print(f"[-] Can't create data dir Error: {e}")

        self.db_name = f"{dirname}/data/{db_name}.db"

        self.connection = sqlite3.connect(self.db_name, check_same_thread=False)
        self.c = self.connection.cursor()

    def table_creation(self, table_name):
        try:
            self.c.execute(f'''CREATE TABLE IF NOT EXISTS {table_name}(
                                                   id INTEGER NOT NULL,
                                                   date_ DATE NOT NULL,
                                                   shift VARCHAR(1) NOT NULL,
                                                   time_ DATETIME, 
                                                   startTime DATETIME NOT NULL, 
                                                   stopTime DATETIME,
                                                   duration INTEGER,
                                                   PRIMARY KEY (id AUTOINCREMENT))''')

        except Exception as e:
            print(f"Error creating table: {e}")

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

    def get_idle_count(self, today, shift, table_name):
        try:
            self.c.execute(f'''SELECT COUNT(id) FROM {table_name}
                               WHERE date_=? AND shift=?''', (today, shift))
            result = self.c.fetchone()
            return result[0] if result else 0
        except Exception as e:
            print(f'Error: {e}, Could not fetch idle count from {table_name}')
            return 0

    def get_current_duration(self, date_, shift, table_name):
        try:
            self.add_duration_to_null_blocks(table_name)
            # self.add_stop_time(table_name)
            self.c.execute(f'''SELECT duration FROM {table_name}
                              WHERE date_=? AND shift=? ORDER BY id DESC LIMIT 1''',
                           (date_, shift))
            b_time = self.c.fetchone()
            if b_time is not None:
                return b_time[0]
            else:
                return 0
        except Exception as e:
            print(f'Error: {e}, Could not get daily breakdown.')
            return 0


def main():
    param_list = ['operating', 'idle', 'breakdown']
    db_name = 'test6'
    tb_name = param_list[0]
    plant_date = '2025-06-13'  # datetime.datetime.now().date()
    shift = 'A'
    db = DBHelper(db_name)

    # table creation
    print('creating tables')
    for tb in param_list:
        db.table_creation(tb)

    pr_status = True


    # db.add_start_time(plant_date, shift, pr_status, tb_name)
    db.add_stop_time(tb_name)
    # db.add_duration_to_null_blocks(tb_name)

    # get_curr_dur = db.get_current_duration(plant_date, shift, tb_name)
    # print(f'get_current_duration: {get_curr_dur}')

    # total_dur = db.get_total_duration(plant_date, shift, tb_name)
    # print(f'total_dur: {total_dur}')

    # total_count = db.get_idle_count(plant_date, shift, tb_name)
    # print(f'total_count: {total_count}')


if __name__ == '__main__':
    main()
