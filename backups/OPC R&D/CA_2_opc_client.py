from opcua import Client

# OPC-UA server URL
client = Client("opc.tcp://192.3.1.15:4840")  # Replace with your server's IP and port

try:
    client.connect()
    print("Connected to OPC-UA server")

    # Node ID for Pezzi_Ok
    node_id = 'ns=3;s="COP_DB_L"."Totali"."Pezzi_Ok"'
    pezzi_ko_node = 'ns=3;s="COP_DB_L"."Totali"."Pezzi_Ko"'
    led_red_node = 'ns=3;s="ledRed"'
    led_yellow_node = 'ns=3;s="ledYellow"'
    led_green_node = 'ns=3;s="ledGreen"'

    pezzi_ok_node = client.get_node(node_id)
    pezzi_ko = client.get_node(pezzi_ko_node)
    led_red = client.get_node(led_red_node)
    led_yellow = client.get_node(led_yellow_node)
    led_green = client.get_node(led_green_node)

    print(f"Pezzi_Ok Value: {pezzi_ok_node.get_value()}")
    print(f"Pezzi_Ko Value: {pezzi_ko.get_value()}")
    print(f"Led Red Value: {led_red.get_value()}")
    print(f"Led Yellow Value: {led_yellow.get_value()}")
    print(f"Led Green Value: {led_green.get_value()}")

finally:
    client.disconnect()
