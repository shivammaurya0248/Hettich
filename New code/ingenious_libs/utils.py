import ast
import struct
from pandas import read_csv, DataFrame
from .logMan import ILogs


class ConfReader:
    """
    This class Defines multiple parsers
    parse_conf_equal
        This parses the files that have content like:-
        temp=1
        dir='/logs/'

    parse_conf_json
        This parses the files that have contents like
        {
            'temp':1,
            'dir:'/logs',
        }

    parse_conf_csv
        This Parses CSV files like
        machine_id, id, data
        marc1, 1, 'asdf'
        marc2, 2, 'qwer'

    All parsers returns dictionary except parse_conf_csv it returns list of dictionaries

    :Author: Shivam maurya
    organisation: Ingenious Techzoid
    """

    def __init__(self):
        self.ilog = ILogs('main', 'info', False)

    def parse_conf_equal(self, filename: str) -> dict:
        """
        parse_conf_equal
            This parses the files that have content like:-
                temp=1
                dir='/logs/'
        :param filename:
        :return:
        """
        json_obj = {}
        try:
            with open(filename, 'r') as f:
                for i in f.readlines():
                    data = i.split('=')
                    if len(data) < 2:
                        continue
                    key, value = data
                    key = key.strip()
                    value = value.strip()
                    if key[0] == '#':
                        continue
                    json_obj[key] = value
        except Exception as e:
            self.ilog.error(f"[-] Unable to read conf {e}")

        return json_obj

    def parse_conf_json(self, filename):
        """
        parse_conf_json
            This parses the files that have contents like
                {
                    'temp':1,
                    'dir:'/logs',
                }
        :param filename:
        :return:
        """
        json_obj = {}
        try:
            with open(filename, 'r') as f:
                data = f.readlines()
                if data:
                    sum = ''
                    for i in data:
                        sum += i.replace("\n", '').strip()
                    json_obj = ast.literal_eval(sum)
        except Exception as e:
            self.ilog.error(f"[-] Unable to read conf {e}")

        return json_obj

    def parse_conf_csv(self, filename):
        """
        It is a generic csv reader and it give a list of objects (dicts basically)
        with the header name as keys

        :param filename:
        :return:
        """
        list_of_objects = []
        try:
            print(filename)
            data = read_csv(filename)
            keys = data.keys()
            for row_index in range(len(data)):
                list_of_objects.append(
                    dict(
                        zip(
                            keys,
                            [i for i in data.loc[row_index]]
                        )
                    )
                )
        except Exception as e:
            self.ilog.error(f"[-] Unable to read config csv {filename} {e}")

        return list_of_objects

    def create_empty_csv(self, file_path, headers):
        """
        Create an empty CSV file with specified headers using pandas.

        Parameters:
        - file_path (str): The path to the CSV file.

        Returns:
        - None
        """

        # Create an empty DataFrame with the specified headers
        df = DataFrame(columns=headers)

        # Write the DataFrame to a CSV file
        df.to_csv(file_path, index=False)