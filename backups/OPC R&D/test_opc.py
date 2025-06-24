import time
from opcua import Client

# Machine details
opc_url = "opc.tcp://192.3.1.7:4840"

# Create OPC UA client
client = Client(opc_url)

try:
    # Connect to the server
    client.connect()
    print(f"Connected to OPC server at {opc_url}")

    # Node IDs (use exact full names)
    pezzi_ok_node = 'ns=3;s="COP_DB_L"."Totali"."Pezzi_Ok"'
    pezzi_ko_node = 'ns=3;s="COP_DB_L"."Totali"."Pezzi_Ko"'
    led_red_node = 'ns=3;s="ledRed"'
    led_yellow_node = 'ns=3;s="ledYellow"'
    led_green_node = 'ns=3;s="ledGreen"'

    # Access nodes
    pezzi_ok = client.get_node(pezzi_ok_node)
    pezzi_ko = client.get_node(pezzi_ko_node)
    led_red = client.get_node(led_red_node)
    led_yellow = client.get_node(led_yellow_node)
    led_green = client.get_node(led_green_node)

    print("Reading data in a loop (Press Ctrl+C to stop)...")
    while True:
        try:
            # Read values
            value_ok = pezzi_ok.get_value()
            value_ko = pezzi_ko.get_value()
            value_red = led_red.get_value()
            value_yellow = led_yellow.get_value()
            value_green = led_green.get_value()

            # Print the values
            print(f"Pezzi OK: {value_ok}, Pezzi KO: {value_ko}")
            print(f"LED Red: {value_red}, LED Yellow: {value_yellow}, LED Green: {value_green}")

            # Wait for 2 seconds
            time.sleep(2)
        except KeyboardInterrupt:
            print("\nLoop stopped by user.")
            break
        except Exception as e:
            print(f"Error during read: {e}")
            break

except Exception as e:
    print(f"An error occurred: {e}")

finally:
    try:
        client.disconnect()
        print("Disconnected from OPC server.")
    except Exception as e:
        print(f"Error while disconnecting: {e}")
