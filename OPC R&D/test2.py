from opcua import Client
import time
from logger import log


class OpcClient:
    def __init__(self, opc_url):

        self.opc_url = opc_url
        log.info(f"OPC URL: {self.opc_url}")
        self.client = Client(opc_url)
        self.connected = False

        self.node_1 = 'ns=3;s="COP_DB_L"."Totali"."Pezzi_Ok"'
        self.node_2 = 'ns=3;s="COP_DB_L"."Totali"."Pezzi_Ko"'
        self.node_3 = 'ns=3;s="ledRed"'
        self.node_4 = 'ns=3;s="ledYellow"'
        self.node_5 = 'ns=3;s="ledGreen"'

    def is_connected(self, retries=4, delay=2):
        for attempt in range(1, retries + 1):
            try:
                log.info(f"Attempting to connect (Attempt {attempt}/{retries})...")
                self.client.connect()
                log.info(f"Connected to OPC server at {self.opc_url}")
                return True
            except Exception as e:
                log.info(f"Connection attempt {attempt} failed: {e}")
                time.sleep(delay)
        log.error("Failed to connect to the OPC server after multiple attempts.")
        self.disconnect()
        return False


    def disconnect(self):
        try:
            if self.connected:
                self.client.disconnect()
                log.info("Disconnected from OPC server.")
            else:
                log.info("Client was not connected.")
        except Exception as e:
            log.error(f"Error while disconnecting: {e}")

    def read_values(self):
        try:
            # if not self.is_connected():
            #     log.info("Client is not connected to the OPC server.")
            #     return [None, None, None, None, None]
            # self.client.disconnect()
            # self.client.connect()
            # log.info(f"Connected to OPC server at {opc_url}")
            ok_count = self.client.get_node(self.node_1)
            ng_count = self.client.get_node(self.node_2)
            red_led = self.client.get_node(self.node_3)
            yellow_led = self.client.get_node(self.node_4)
            green_led = self.client.get_node(self.node_5)

            value_ok = ok_count.get_value()
            value_ng = ng_count.get_value()
            value_red = red_led.get_value()
            value_yellow = yellow_led.get_value()
            value_green = green_led.get_value()

            return [value_ng, value_ok, value_red, value_yellow, value_green]
        except Exception as e:
            log.error(f"Error reading values: {e}")
            return [None, None, None, None, None]


if __name__ == "__main__":
    opc_url_ = "opc.tcp://192.3.1.7:4840"
    opc_client = OpcClient(opc_url_)
    log.info(f'opc_conn: {opc_client.is_connected()}')
    try:
        while True:
            data = opc_client.read_values()
            log.info(f'got data: {data}')
            time.sleep(2)
    finally:
        opc_client.disconnect()
