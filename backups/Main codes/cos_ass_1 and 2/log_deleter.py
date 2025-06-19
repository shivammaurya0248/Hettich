import os
import datetime
import schedule
import time

logs_dir = "./logs"
number_of_days = 7


def main():
    try:
        if os.path.isdir(logs_dir):
            current_date = datetime.datetime.now()
            list_files = os.listdir(logs_dir)
            print("[+] got files")
            print(list_files)
            if list_files:
                if len(list_files) > number_of_days:

                    for i in list_files:
                        # Skipping current day's log
                        if i == "app_log":
                            continue

                        if "app_log." in i:
                            date_of_log_creation_str = i.split(".")[1]
                        # elif "email_log_" in i:
                        #     date_of_log_creation_str = i.split("_")[2].split(".")[0]
                        # elif "emp_login_" in i:
                        #     date_of_log_creation_str = i.split("_")[2].split(".")[0]
                        # elif "restart." in i:
                        #     date_of_log_creation_str = i.split(".")[1]
                        # elif "network." in i:
                        #     date_of_log_creation_str = i.split(".")[1]
                        elif "network_" in i:
                            date_of_log_creation_str = i.split("_")[1].split(".")[0]
                        # elif "sync_log." in i:
                        #     date_of_log_creation_str = i.split(".")[1]
                        # elif "po_log_" in i:
                        #     date_of_log_creation_str = i.split("_")[2].split(".")[0]
                        # elif "upload_log_" in i:
                        #     date_of_log_creation_str = i.split("_")[2].split(".")[0]
                        else:
                            print("[-] No log to be deleted")
                            continue
                        date_of_log_creation = datetime.datetime.strptime(date_of_log_creation_str, "%Y-%m-%d")
                        print(f"date_of_log_creation {date_of_log_creation}")
                        if (current_date - date_of_log_creation).days > number_of_days:
                            os.remove(os.path.join(logs_dir, i))
                            print(f"[+] Deleted file successfully {i}")
                else:
                    print("[+] Got only 7 or less than 7 log files")
        else:
            print(f"[-] No such file {logs_dir}")
    except Exception as e:
        print(f"Error while Deleting the logs {e}")


schedule.every(30).minutes.do(main)

try:
    while True:
        schedule.run_pending()
        time.sleep(10)
except Exception as e:
    print(f"Error while running schedule {e}")
    time.sleep(10)
