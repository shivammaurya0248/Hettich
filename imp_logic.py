import time
import sys

# Number of lights
num_lights = 4

# Initialize variables
start_times = [sys.maxsize] * num_lights
prev_statuses = [False] * num_lights
durations = [0] * num_lights  # List to store summed duration for each light


def update_light_status(light_statuses):
    global start_times, prev_statuses, durations
    current_time = time.time()
    for i in range(num_lights):
        if light_statuses[i]:  # Light is on
            if start_times[i] == sys.maxsize:
                print(f"+++++LIGHT {i + 1} duration starts+++++")
                start_times[i] = current_time
                prev_statuses[i] = True

            duration = round((current_time - start_times[i]) / 60, 2)
            print(f"Light {i + 1} time diff {duration} minutes")
            durations[i] += duration
            start_times[i] = current_time

        else:  # Light is off
            if prev_statuses[i]:
                print(f"+++++LIGHT {i + 1} duration stop+++++")
                duration = round((current_time - start_times[i]) / 60, 2)
                durations[i] += duration
                print(f"Stored duration for Light {i + 1}: {duration} minutes")
                prev_statuses[i] = False
            start_times[i] = sys.maxsize


def print_durations():
    global durations
    for i in range(num_lights):
        print(f"Total duration for Light {i + 1}: {durations[i]} minutes")


# Example usage
num_lights = 4
#light_calculator = LightDurationCalculator(num_lights)

# Simulate PLC signal updates
light_signals_list = [
    [True, False, True, False],  # Example initial state
    [True, True, False, False],  # Example after some time
    [False, False, True, True],  # Example after some more time
    [True, True, True, True],  # Example after some more time
    [False, False, False, False]  # Example after some more time
]

try:
    while True:
        for signals in light_signals_list:
            update_light_status(signals)
            print_durations()
            time.sleep(1)  # Simulate 1-second interval

except KeyboardInterrupt:
    print("Test interrupted. Printing total durations stored so far:")
    print_durations()
