import datetime


class ShiftManager:

    def __init__(self, logger):

        self.log = logger
        self.shift_a_start = self.shift_c_end = datetime.time(7, 0, 0)
        self.shift_b_start = self.shift_a_end = datetime.time(15, 30, 0)
        self.shift_b_end = datetime.time(23, 59, 59)
        self.shift_c_start = datetime.time(0, 0, 0)

        self.shift_g_start = datetime.time(9, 0, 0)
        self.shift_g_end = datetime.time(17, 30, 0)

        self.SHIFT_TIME_FORMAT = "%H:%M:%S"
        self.BREAKS_TIME_FORMAT = "%Y-%m-%dT%H:%M"

        self.planned_breaks_dict = {
            "A": {
                "tea_1": [datetime.datetime.now().replace(hour=10, minute=0), 10],
                "lunch": [datetime.datetime.now().replace(hour=11, minute=30), 30],
                "tea_2": [datetime.datetime.now().replace(hour=14, minute=0), 10],
            },
            "B": {
                "tea_1": [datetime.datetime.now().replace(hour=16, minute=0), 10],
                "lunch": [datetime.datetime.now().replace(hour=19, minute=30), 30],
                "tea_2": [datetime.datetime.now().replace(hour=22, minute=0), 10],
            },
            "C": {
                "tea_1": [datetime.datetime.now().replace(hour=1, minute=0), 0],
                "tea_2": [datetime.datetime.now().replace(hour=4, minute=0), 0],
            },
            "G": {
                "tea_1": [datetime.datetime.now().replace(hour=10, minute=0), 10],
                "lunch": [datetime.datetime.now().replace(hour=12, minute=30), 30],
                "tea_2": [datetime.datetime.now().replace(hour=14, minute=0), 10],
            },
        }

    def get_current_shift(self, general_shift_status) -> str:
        try:
            now = datetime.datetime.now().time()

            if general_shift_status:
                if self.shift_g_start <= now < self.shift_g_end:
                    return 'B'
                else:
                    return 'NA'
            else:
                if self.shift_b_start <= now < self.shift_b_end:
                    return 'B'
                elif self.shift_a_start <= now < self.shift_a_end:
                    return 'A'
                elif self.shift_c_start <= now < self.shift_c_end:
                    return 'C'
                else:
                    return 'C'
        except Exception as e:
            self.log.error(f"[!] Error: {e}, while deciding")

    def break_check(self) -> bool:
        try:
            current_time = datetime.datetime.now()
            if self.planned_breaks_dict is None and self.planned_breaks_dict == {}:
                return False

            for key, (planned_break, minutes) in self.planned_breaks_dict.items():
                planned_break = datetime.datetime.strptime(planned_break, self.BREAKS_TIME_FORMAT)

                if (current_time - planned_break).total_seconds() < minutes * 60 and current_time > planned_break:
                    return True
            return False
        except Exception as e:
            self.log.error(f'[!]Error {e}, error while checking break_check')
            return False

    def get_working_time_so_far(self, current_shift: str) -> (int, int):
        try:
            no_break_set = False
            current_day = datetime.datetime.now()
            if current_shift == 'A':
                shift_start = current_day.replace(hour=self.shift_a_start.hour,
                                                  minute=self.shift_a_start.minute, second=0)
            elif current_shift == 'B':
                shift_start = current_day.replace(hour=self.shift_b_start.hour,
                                                  minute=self.shift_b_start.minute, second=0)

            elif current_shift == 'G':
                shift_start = current_day.replace(hour=self.shift_g_start.hour,
                                                  minute=self.shift_g_start.minute, second=0)

            elif current_shift == 'C':
                if self.shift_c_start.hour >= 0 and (self.shift_c_start.hour < self.shift_a_start.hour):
                    shift_start = datetime.datetime.now().replace(hour=self.shift_c_start.hour,
                                                                  minute=self.shift_c_start.minute,
                                                                  second=0)
                else:
                    if current_day.hour >= self.shift_c_start.hour:
                        shift_start = datetime.datetime.now().replace(hour=self.shift_c_start.hour,
                                                                      minute=self.shift_c_start.minute,
                                                                      second=0)
                        no_break_set = True
                    else:
                        shift_start = datetime.datetime.now().replace(hour=self.shift_c_start.hour,
                                                                      minute=self.shift_c_start.minute,
                                                                      second=0) - datetime.timedelta(hours=24)
                        no_break_set = False

            else:
                shift_start = current_day.replace(hour=0, minute=0, second=0)

            total_break_time = datetime.timedelta(minutes=0)
            current_time = datetime.datetime.now()

            # if planned_breaks_dict is None and planned_breaks_dict == {}:
            #     return 0

            if not self.planned_breaks_dict:
                usable_time = current_time - shift_start
                if usable_time.total_seconds() < 0:
                    return 0, datetime.timedelta(0)  # No working time if current_time is before shift_start
                return usable_time.seconds, datetime.timedelta(minutes=0)

            for break_name, break_data in self.planned_breaks_dict.get(current_shift, {}).items():
                planned_break_start_time, planned_break_minutes = break_data

                today = current_time.date()
                adjusted_break_start_time = datetime.datetime(today.year, today.month, today.day,
                                                              planned_break_start_time.hour,
                                                              planned_break_start_time.minute)
                planned_break_stop_time = adjusted_break_start_time + datetime.timedelta(minutes=planned_break_minutes)
                current_break_time = planned_break_stop_time - adjusted_break_start_time

                if current_time > planned_break_stop_time:
                    total_break_time += current_break_time
                elif (current_time > adjusted_break_start_time) and (current_time < planned_break_stop_time):
                    time_to_subtract_for_current_break = current_time - adjusted_break_start_time
                    total_break_time += time_to_subtract_for_current_break

            self.log.info(f"[+]curr_total_break_time: {total_break_time, type(total_break_time)}")

            if no_break_set:
                return (current_time - shift_start).seconds, total_break_time

            usable_time = current_time - total_break_time

            if usable_time < shift_start:
                return 0, datetime.timedelta(0)

            total_working_time = usable_time - shift_start
            self.log.info(f"[+]curr_break_time: {total_break_time}")
            self.log.info(f"[+]curr_working_time: {total_working_time}")

            return total_working_time.seconds, total_break_time
        except Exception as e:
            self.log.error(f'[-] Error while calculating break time: {e}')
            return 0, datetime.timedelta(0)

    def get_total_shift_working_time(self, current_shift: str) -> (int, int):
        try:
            if current_shift == 'A':
                shift_start = datetime.datetime.now().replace(hour=self.shift_a_start.hour,
                                                              minute=self.shift_a_start.minute, second=0)
                shift_end = datetime.datetime.now().replace(hour=self.shift_a_end.hour,
                                                            minute=self.shift_a_end.minute, second=0)
            elif current_shift == 'B':
                shift_start = datetime.datetime.now().replace(hour=self.shift_b_start.hour,
                                                              minute=self.shift_b_start.minute, second=0)
                shift_end = datetime.datetime.now().replace(hour=self.shift_b_end.hour,
                                                            minute=self.shift_b_end.minute, second=0)
            elif current_shift == 'G':
                shift_start = datetime.datetime.now().replace(hour=self.shift_g_start.hour,
                                                              minute=self.shift_g_start.minute, second=0)
                shift_end = datetime.datetime.now().replace(hour=self.shift_g_end.hour,
                                                            minute=self.shift_g_end.minute, second=0)

            elif current_shift == 'C':
                shift_start = datetime.datetime.now().replace(hour=self.shift_c_start.hour,
                                                              minute=self.shift_c_start.minute, second=0)
                shift_end = datetime.datetime.now().replace(hour=self.shift_c_end.hour,
                                                            minute=self.shift_c_end.minute, second=0)
            else:
                shift_start = datetime.datetime.now().replace(hour=0, minute=0, second=0)
                shift_end = datetime.datetime.now().replace(hour=0, minute=0, second=0)

            current_time = datetime.datetime.now()
            total_break_time = datetime.timedelta(minutes=0)

            if self.planned_breaks_dict is None and self.planned_breaks_dict == {}:
                return 0

            for break_name, (planned_break_start_time, planned_break_minutes) in self.planned_breaks_dict.items():
                planned_break_start_time = datetime.datetime.strptime(planned_break_start_time,
                                                                      self.BREAKS_TIME_FORMAT)
                today = current_time.date()
                adjusted_break_start_time = datetime.datetime(today.year, today.month, today.day,
                                                              planned_break_start_time.hour,
                                                              planned_break_start_time.minute)
                planned_break_stop_time = adjusted_break_start_time + datetime.timedelta(minutes=planned_break_minutes)
                total_break_time += (planned_break_stop_time - adjusted_break_start_time)

            total_shift_time = shift_end - shift_start
            total_working_time = total_shift_time - total_break_time

            self.log.info(f'[+]total_break_time: {total_break_time}')
            self.log.info(f'[+]total_working_time_after_breaks: {total_working_time}')

            return total_working_time.seconds, total_shift_time.seconds
        except Exception as e:
            self.log.error(f'[-] Error while calculating break time: {e}')
            return 0
