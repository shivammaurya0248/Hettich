import logging
import logging.handlers
from logging.handlers import TimedRotatingFileHandler
import os
import sys
import datetime
import schedule
import time

if getattr(sys, 'frozen', False):
    dirname = os.path.dirname(sys.executable)
else:
    dirname = os.path.dirname(os.path.abspath(__file__))

default_dir = './'
shift_end_time = datetime.time(hour=8, minute=29, second=59)


def check_log_dir(dirpath):
    # checking and creating logs directory here
    if not os.path.isdir(dirpath):
        # if not os.path.isdir(os.path.join(os.path.dirname(dirname), 'logs')):
        print("[-] logs directory doesn't exists")
        try:
            os.mkdir(dirpath)
            # os.mkdir(os.path.join(os.path.dirname(dirname), 'logs'))
            print("[+] Created logs dir successfully")
        except Exception as e:
            print(f"[-] Can't create dir logs Error: {e}")


class ILogs:
    def __init__(self, logger_name, log_level, is_main_file=False, save_to_file_flag=True,
                 logger_file_name='app_log', log_dir=default_dir):
        global shift_end_time
        if log_level == 'debug':
            self.log_level = logging.DEBUG
        elif log_level == 'info':
            self.log_level = logging.INFO
        elif log_level == 'warning':
            self.log_level = logging.WARNING
        elif log_level == 'error':
            self.log_level = logging.ERROR

        self.log_name = logger_name
        self.log_dir = log_dir

        self.FORMAT = ('%(asctime)-15s %(levelname)-8s %(name)s %(module)-15s:%(lineno)-8s %(message)s')

        self.logFormatter = logging.Formatter(self.FORMAT)
        self.log = logging.getLogger(self.log_name)

        if is_main_file:
            check_log_dir(os.path.join(log_dir, 'logs'))

            if save_to_file_flag:
                self.logger_file_name = logger_file_name
                # shift_end_time = datetime.time(hour=17, minute=0)
                now = datetime.datetime.now()
                today_end = datetime.datetime.combine(now.date(), shift_end_time)
                if now > today_end:
                    next_rotation_time = today_end + datetime.timedelta(days=1)
                else:
                    next_rotation_time = today_end

                self.fileHandler = TimedRotatingFileHandler(os.path.join(self.log_dir, f'logs/{self.logger_file_name}'),
                                                            when='midnight', interval=1,
                                                            backupCount=0,  # Keep only the current file
                                                            atTime=next_rotation_time.time())
                self.fileHandler.setFormatter(self.logFormatter)
                self.fileHandler.suffix = "%Y-%m-%d.log"
                self.log.addHandler(self.fileHandler)

                self.consoleHandler = logging.StreamHandler()
                self.consoleHandler.setFormatter(self.logFormatter)
                self.log.addHandler(self.consoleHandler)
        self.log.setLevel(self.log_level)
        self.info = self.log.info
        self.error = self.log.error
        self.debug = self.log.debug
        self.warn = self.log.warning


class LogCleaner:
    def __init__(self, number_of_days, log_dir=default_dir, interval_minute=60, schedule_self=False):
        self.log_dir = os.path.join(log_dir, 'logs')
        check_log_dir(self.log_dir)
        self.number_of_days = number_of_days
        self.ilog = ILogs('log_cleaner', 'info', True, True, 'log_cleaner')
        if schedule_self:
            schedule.every(interval_minute).minutes.do(self.clean)

    def clean(self):
        try:
            if os.path.isdir(self.log_dir):
                current_date = datetime.datetime.now()
                list_files = os.listdir(self.log_dir)
                self.ilog.info("[*] fetching files")
                self.ilog.debug(list_files)
                if list_files:
                    for i in list_files:
                        temp_list = i.split('.')
                        if len(temp_list) < 3:
                            self.ilog.debug(f"[+] new file {i}")
                            continue

                        date_of_log_creation_str = temp_list[1]
                        date_of_log_creation = datetime.datetime.strptime(date_of_log_creation_str, "%Y-%m-%d")
                        self.ilog.debug(f"[+] date_of_log_creation {date_of_log_creation}")
                        if (current_date - date_of_log_creation).days > self.number_of_days:
                            self.ilog.debug(f"[+] {i} deleted!")
                            os.remove(os.path.join(self.log_dir, i))
                        else:
                            self.ilog.debug(f"[+] Not deleting {i}")
                else:
                    self.ilog.debug(f"[-] No file found")
            else:
                self.ilog.warn(f"[-] {self.log_dir} not found")
        except Exception as e:
            self.ilog.error(f"[-] Error while Deleting the logs {e}")
