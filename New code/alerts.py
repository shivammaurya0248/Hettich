import requests


class CL_Alerts:
    def __init__(self, host, access_token, logger):
        self.log = logger
        self.host = host
        self.access_token = access_token
        self.SEND_DATA = True
        self.HEADERS = {'content-type': 'application/json'}

    def alert_cycle_time(self, status: bool, cycle_time_with_alert: float) -> None:
        url = f'https://{self.host}/api/v1/{self.access_token}/attributes'
        payload = {
            "AlertCycleTimeDisturb": status,
            "Cycletime_with_alert": cycle_time_with_alert
                   }
        if self.SEND_DATA:
            try:
                request_response = requests.post(url, json=payload, headers=self.HEADERS, timeout=2)
                self.log.info(request_response.status_code)
                if request_response.status_code == 200:
                    self.log.info("f[+] Attributes Reset successful")
                else:
                    self.log.error("[-] Attributes Reset failed")
            except Exception as e:
                self.log.error(f"{e} error sending alerts")

    def alert_disconnected(self, status: bool) -> None:
        url = f'https://{self.host}/api/v1/{self.access_token}/attributes'
        payload = {
            "AlertDisconnected": status
                   }
        self.log.info(f"AlertDisconnected:{str(payload)}")
        if self.SEND_DATA:
            try:
                request_response = requests.post(url, json=payload, headers=self.HEADERS, timeout=2)
                self.log.info(request_response.status_code)
                if request_response.status_code == 200:
                    self.log.info("f[+] Attributes Reset successful")
                else:
                    self.log.error("[-] Attributes Reset failed")
            except Exception as e:
                self.log.error(f"{e}")


    def alert_major_bkdown(self, status: bool) -> None:
        url = f'https://{self.host}/api/v1/{self.access_token}/attributes'
        payload = {
            "AlertMajorBreakDown": status
                   }
        self.log.info(f'major_breakdown_payload: {payload}')
        if self.SEND_DATA:
            try:
                request_response = requests.post(url, json=payload, headers=self.HEADERS, timeout=2)
                self.log.info(request_response.status_code)
                if request_response.status_code == 200:
                    self.log.info("f[+] Attributes Reset successful")
                else:
                    self.log.error("[-] Attributes Reset failed")
            except Exception as e:
                self.log.error(f"{e}")


    def reset_alert_check(self) -> None:
        url = f'http://{self.host}:8080/api/v1/{self.access_token}/attributes?sharedKeys=alert_check'
        if self.SEND_DATA:
            self.log.info("(>>>) Post Reset alert check")
            try:
                alert_check = True
                payload = {
                    'alert_check': alert_check,
                }
                request_response = requests.post(url, json=payload, headers=self.HEADERS, timeout=2)
                self.log.info(request_response.status_code)
                if request_response.status_code == 200:
                    self.log.info("f[+] alert_check reset in Shared attributes ")
                else:
                    self.log.error("[-] alert_check unable to reset in Shared attributes")
            except Exception as e:
                self.log.error(f"{e}")
