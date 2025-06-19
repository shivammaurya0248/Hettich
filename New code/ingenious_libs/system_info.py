import os
import re
import sys
import logging
import requests
import subprocess

log = logging.getLogger(__name__)


class ClSystemInfo:
    def __init__(self):
        self.version = '1.0.0'

    def interfaces(self):
        try:
            regex = r"\d: (\w*)....inet (\d*\.\d*.\d*.\d*\/\d*)"
            payload = dict()
            with subprocess.Popen(['ip', '-4', '-o', 'addr', 'show'], stdout=subprocess.PIPE) as proc:
                for Line in proc.stdout:
                    match = re.match(regex, Line.decode('utf-8'), re.MULTILINE)
                    log.info(match.groups())
                    data = match.groups()
                    if data:
                        interface = data[0]
                        ip = data[1]
                        if interface == 'lo':
                            continue
                        else:
                            if interface in payload:
                                if type(payload[interface]) == list:
                                    payload[interface].append(ip)
                                else:
                                    payload[interface] = [payload.get(interface), ip]
                                continue
                            payload[interface] = ip
            if payload:
                return payload
            else:
                return {}
        except:
            log.error(f'[+] No interface found')
            return {}

    def cpu_temp(self):
        sys_temp_dir = '/sys/class/thermal/'
        temperature = {'cpu_temp': 0}
        try:
            temperature_dirs = os.listdir(sys_temp_dir)
            for dir in temperature_dirs:
                if 'thermal' in dir:
                    filename = f"{sys_temp_dir}{dir}/temp"
                    try:
                        with open(filename, "r") as f:
                            temp_in_milli_celcius = f.readline()
                            if temp_in_milli_celcius:
                                temperature['cpu_temp'] = int(temp_in_milli_celcius) / 1000
                                if temperature['cpu_temp'] < 0:
                                    temperature = 0
                                return temperature
                    except:
                        pass
            return temperature
        except Exception as e:
            log.error(f"[-] Error fetching CPU temp {e}")
            return {}

    def drive_space(self):
        payload = dict()
        try:
            data_list = []
            with subprocess.Popen(['df', '/', '-h'], stdout=subprocess.PIPE) as proc:
                for line in proc.stdout:
                    data_list.append(str(line).split(' '))
                for i, data in enumerate(data_list):
                    data_list[i] = [j.replace("b'", '').replace("\\n'", '') for j in data if j != '']
                data_list[0].pop()
                for i, data in enumerate(data_list[0]):
                    payload[f"drive_{data}"] = data_list[1][i]
        except Exception as e:
            log.info(f"Failed to get root fs size: {e}")
        return payload


def send_system_info(host, access_token):
    try:
        url = f'http://{host}:8080/api/v1/{access_token}/attributes'
        headers = {'Content-Type': 'application/json'}

        payload = dict()

        ob_sys_info = ClSystemInfo()
        payload.update(ob_sys_info.interfaces())
        payload.update(ob_sys_info.cpu_temp())
        payload.update(ob_sys_info.drive_space())
        log.info(f"{payload}")
        if payload:
            try:
                log.info("[*] Sending System Info to the server...")
                req = requests.post(url, json=payload, headers=headers, timeout=2)
                req.raise_for_status()
                log.info(f'[+] SysInfo Sent successfully {payload}')
            except Exception as e:
                log.error(f"[+] Can't Send data to the server Error : {e}")
        else:
            log.error("[+] No data to send")
    except Exception as e:
        log.error(f"[+] Error getting system info : {e}")
