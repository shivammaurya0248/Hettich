import datetime
from logger import log

LINE_ID = "LINE-1"
# SHIFT TIMINGS
shift_a_start = datetime.time(7, 0, 0, 0)
shift_b_start = datetime.time(15, 30, 0, 0)
shift_c_start = datetime.time(0, 0, 0, 0)
shift_a_end = datetime.time(15, 30, 0, 0)
shift_b_end = datetime.time(23, 59, 59, 0)
shift_c_end = datetime.time(7, 0, 0, 0)

shift_g_start = datetime.time(9, 0, 0, 0)
shift_g_end = datetime.time(17, 30, 0, 0)


# FETCHING SHIFT

def get_shift(status):
    global shift_a_start, shift_b_start, shift_c_start, shift_a_end, shift_b_end, shift_c_end, shift_g_start, shift_g_end
    now = datetime.datetime.now().time()
    # new_day = datetime.time(23, 59, 59, 999)
    # new_one = datetime.time(0, 0, 0, 0)
    if status:
        if shift_g_start <= now < shift_g_end:
            return 'G'
        else:
            return 'NA'
    else:
        if shift_b_start <= now < shift_b_end:
            return 'B'
        elif shift_a_start <= now < shift_a_end:
            return 'A'
        elif shift_c_start <= now < shift_c_end:
            return 'C'
        else:
            return 'C'


def get_current_total_time(current_shift):
    """
    This function returns the total working time from the start of the shift
    :param: current_shift:
    :return total_working_time:
    """
    no_break_set = False
    planned_break_dict = {
        "LINE-1": {
            "A": {
                # "moving_1": [datetime.datetime.now().replace(hour=6, minute=30), 5],
                "tea_1": [datetime.datetime.now().replace(hour=10, minute=00), 10],
                "lunch": [datetime.datetime.now().replace(hour=11, minute=30), 30],
                "tea_2": [datetime.datetime.now().replace(hour=14, minute=00), 10],
            },
            "B": {
                # "moving_1": [datetime.datetime.now().replace(hour=15, minute=0), 5],
                "tea_1": [datetime.datetime.now().replace(hour=16, minute=00), 10],
                "lunch": [datetime.datetime.now().replace(hour=19, minute=30), 30],
                "tea_2": [datetime.datetime.now().replace(hour=22, minute=00), 10],
            },
            "C": {
                # "moving_1": [datetime.datetime.now().replace(hour=23, minute=30), 5],
                "tea_1": [datetime.datetime.now().replace(hour=1, minute=00), 10],
                #"lunch": [datetime.datetime.now().replace(hour=4, minute=00), 30],
                "tea_2": [datetime.datetime.now().replace(hour=4, minute=00), 10]
            },
            "G": {
                # "moving_1": [datetime.datetime.now().replace(hour=23, minute=30), 5],
                "tea_1": [datetime.datetime.now().replace(hour=10, minute=00), 10],
                "lunch": [datetime.datetime.now().replace(hour=12, minute=30), 30],
                "tea_2": [datetime.datetime.now().replace(hour=14, minute=00), 10]
            },
        }
    }
    try:
        current_day = datetime.datetime.now()
        if current_shift == 'A':
            shift_start = datetime.datetime.now().replace(hour=shift_a_start.hour, minute=shift_a_start.minute,
                                                          second=0)
        elif current_shift == 'B':
            shift_start = datetime.datetime.now().replace(hour=shift_b_start.hour, minute=shift_b_start.minute,
                                                          second=0)
        elif current_shift == 'G':
            shift_start = datetime.datetime.now().replace(hour=shift_g_start.hour, minute=shift_g_start.minute,
                                                          second=0)
        elif current_shift == 'C':
            # if shift c starts at or after 0 then use the shift_start as it is
            if shift_c_start.hour >= 0 and (shift_c_start.hour < shift_a_start.hour):
                shift_start = datetime.datetime.now().replace(hour=shift_c_start.hour,
                                                              minute=shift_c_start.minute,
                                                              second=0)
            else:
                # if shift c starts before midnight then  if current_day.hour is greater than shift a start then
                # set shift_start to shift_start as it is because other wise it will minus it from the previous date
                if current_day.hour >= shift_c_start.hour:
                    shift_start = datetime.datetime.now().replace(hour=shift_c_start.hour,
                                                                  minute=shift_c_start.minute,
                                                                  second=0)
                    # we are using this so that we can stop break time from subtracting from current time otherwise
                    # then it will subtract the breaktime from it and will return data accordingly
                    no_break_set = True
                # And if now time has increased to more than midnight then it will be setting shift_start to previous
                # day otherwise it will give 0 as output
                else:
                    # here we are subtracting 1 from day otherwise the shift start will become greater than current time
                    shift_start = datetime.datetime.now().replace(hour=shift_c_start.hour, minute=shift_c_start.minute,
                                                                  second=0) - datetime.timedelta(hours=24)
                    no_break_set = False
        else:
            shift_start = datetime.datetime.now().replace(hour=0, minute=0, second=0)

        time_to_subtract_for_current_break = datetime.timedelta(minutes=0)
        total_break_time = datetime.timedelta(minutes=0)
        current_time = datetime.datetime.now()
        break_time_dict = planned_break_dict[LINE_ID][current_shift]  # Getting current shift's break times
        for break_time in break_time_dict:
            planned_break_start_time = break_time_dict[break_time][0]
            planned_break_minutes = break_time_dict[break_time][1]
            planned_break_stop_time = planned_break_start_time + datetime.timedelta(minutes=planned_break_minutes)
            current_break_time = planned_break_stop_time - planned_break_start_time
            if current_time > planned_break_stop_time:
                total_break_time += current_break_time
            if (current_time > planned_break_start_time) and (current_time < planned_break_stop_time):
                time_to_subtract_for_current_break = current_time - planned_break_start_time
                total_break_time += time_to_subtract_for_current_break

        if no_break_set:
            return (current_time - shift_start).seconds

        log.info(f"[+] Total break time is {total_break_time}")
        usable_time = current_time - total_break_time
        if usable_time < shift_start:
            return 0
        total_working_time = usable_time - shift_start
        log.info(f"[+] Total Working time is {total_working_time}")
        log.info(f"[+] Total Working time in seconds is {total_working_time.seconds}")
        return total_working_time.seconds
    except Exception as e:
        log.error(f'[-] Error while calculating break time{e}')
        return 0


def break_check(current_shift):
    planned_break_dict = {
        "LINE-1": {
            "A": {
                # "moving_1": [datetime.datetime.now().replace(hour=6, minute=30), 5],
                "tea_1": [datetime.datetime.now().replace(hour=10, minute=00), 10],
                "lunch": [datetime.datetime.now().replace(hour=11, minute=30), 30],
                "tea_2": [datetime.datetime.now().replace(hour=14, minute=00), 10],
            },
            "B": {
                # "moving_1": [datetime.datetime.now().replace(hour=15, minute=0), 5],
                "tea_1": [datetime.datetime.now().replace(hour=16, minute=00), 10],
                "lunch": [datetime.datetime.now().replace(hour=19, minute=30), 30],
                "tea_2": [datetime.datetime.now().replace(hour=22, minute=00), 10],
            },
            "C": {
                # "moving_1": [datetime.datetime.now().replace(hour=23, minute=30), 5],
                "tea_1": [datetime.datetime.now().replace(hour=1, minute=00), 10],
                # "lunch": [datetime.datetime.now().replace(hour=4, minute=00), 30],
                "tea_2": [datetime.datetime.now().replace(hour=4, minute=00), 10]
            },
            "G": {
                # "moving_1": [datetime.datetime.now().replace(hour=23, minute=30), 5],
                "tea_1": [datetime.datetime.now().replace(hour=10, minute=00), 10],
                "lunch": [datetime.datetime.now().replace(hour=12, minute=30), 30],
                "tea_2": [datetime.datetime.now().replace(hour=14, minute=00), 10]
            },
        }
    }
    current_time = datetime.datetime.now()
    break_time_dict = planned_break_dict[LINE_ID][current_shift]

    for key, [planned_break, minutes] in break_time_dict.items():
        if (current_time - planned_break).total_seconds() < minutes * 60 and current_time > planned_break:
            return True
    return False
