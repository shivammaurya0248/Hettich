import os
from .logMan import ILogs

dirname = os.path.dirname(__name__)
ilog_main = ILogs('main', 'info', False)


def run_if_time_synced(function, timesync_flag_file='./conf/time_sync_flag.txt'):
    """
    Decorator Function to not run the program if the time is not synced in the server
    and only run if time is synced from custom NTP

    :param function:
    :return:
    """
    def inner():
        try:
            with open(timesync_flag_file, 'r') as f:
                f.seek(0)
                data = f.read()
                ilog_main.info(f"[+] Time synced : [{data}]")
                if data == '1':
                    function()
        except Exception as e:
            ilog_main.error(f"[+] Unable to check time sync {e}")

    return inner
