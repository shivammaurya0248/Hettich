import time
from opcua import Client
from logger import log

opc_url = f"opc.tcp://192.3.1.7:4840"
client = Client(opc_url)

log.info(f"Connecting to OPC server at {opc_url}")


def connect(retries=4, delay=2):
    for attempt in range(1, retries + 1):
        try:
            log.info(f"Attempting to connect (Attempt {attempt}/{retries})...")
            client.connect()
            log.info(f"Connected to OPC server at {opc_url}")
            return True
        except Exception as e:
            log.info(f"Connection attempt {attempt} failed: {e}")
            time.sleep(delay)

    log.error("Failed to connect to the OPC server after multiple attempts.")
    return False


def read_values():
    """Read values from OPC UA server."""
    try:
        # Node IDs (use exact full names)
        pezzi_ok_node = 'ns=3;s="COP_DB_L"."Totali"."Pezzi_Ok"'
        pezzi_ko_node = 'ns=3;s="COP_DB_L"."Totali"."Pezzi_Ko"'
        led_red_node = 'ns=3;s="ledRed"'
        led_yellow_node = 'ns=3;s="ledYellow"'
        led_green_node = 'ns=3;s="ledGreen"'

        pezzi_ok = client.get_node(pezzi_ok_node)
        pezzi_ko = client.get_node(pezzi_ko_node)
        led_red = client.get_node(led_red_node)
        led_yellow = client.get_node(led_yellow_node)
        led_green = client.get_node(led_green_node)

        try:
            value_ok = pezzi_ok.get_value()
            value_ng = pezzi_ko.get_value()
            value_red = led_red.get_value()
            value_yellow = led_yellow.get_value()
            value_green = led_green.get_value()
            return [value_ng, value_ok, value_red, value_yellow, value_green]
        except KeyboardInterrupt:
            log.info("\nLoop stopped by user.")
        except Exception as e:
            log.info(f"Error during read: {e}")

    except Exception as e:
        log.info(f"An error occurred: {e}")

    finally:
        try:
            client.disconnect()
            log.info("Disconnected from OPC server.")
        except Exception as e:
            log.info(f"Error while disconnecting: {e}")

    return [None, None, None, None, None]

# if __name__ == "__main__":
#     got_data = read_values()
#     log.info(f"Got Data: {got_data}")
#     time.sleep(1)
