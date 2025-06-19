from opcua import Client
import time
from logger import log
import logging


# Suppress unwanted logs from `opcua` library
logging.getLogger("opcua").setLevel(logging.WARNING)
logging.getLogger("connection").setLevel(logging.WARNING)


class cl_opc_client:
    def __init__(self, opc_url, node_dict):
        log.info(f"OPC URL: {opc_url}")
        self.client = Client(opc_url)
        self.connected = False
        self.node_dict = node_dict  # Dictionary containing node names and IDs

    def connect(self, retries=4, delay=2):
        for attempt in range(1, retries + 1):
            try:
                log.info(f"Attempting to connect (Attempt {attempt}/{retries})...")
                self.client.connect()
                self.connected = True
                log.info(f"Connected to OPC server at {self.client.server_url}")
                time.sleep(1)
                return True
            except Exception as e:
                log.warning(f"Connection attempt {attempt} failed: {e}")
                time.sleep(delay)

        self.connected = False
        log.error("Failed to connect to the OPC server after multiple attempts.")
        return False

    def is_connected(self):
        self.connect()
        return self.connected

    def disconnect(self):
        try:
            if self.connected:
                self.client.disconnect()
                log.info("Disconnected from OPC server.")
            else:
                log.info("Client was not connected.")
        except Exception as e:
            log.error(f"Error while disconnecting: {e}")
        finally:
            self.connected = False

    def read_values(self):
        try:
            if not self.is_connected():
                log.warning("Client is not connected to the OPC server.")
                return [None] * len(self.node_dict)

            values = []
            for node_name, node_id in self.node_dict.items():
                if node_id is None:
                    values.append(None)
                    continue

                try:
                    node = self.client.get_node(node_id)
                    values.append(node.get_value())
                except Exception as e:
                    log.error(f"Error reading node '{node_name}': {e}")
                    values.append(None)
                time.sleep(1)

            return values
        except Exception as e:
            log.error(f"Error reading values: {e}")
            return [None] * len(self.node_dict)