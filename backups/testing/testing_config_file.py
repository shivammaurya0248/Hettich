import os


def manage_ip_config(default_machine_name=None):
    try:
        file_path = "machine_config"

        if not os.path.exists(file_path):
            with open(file_path, "w") as file:
                file.write(f"Machine_NAME={default_machine_name}\n")
            print(f"File '{file_path}' created with default configurations.")
            return default_machine_name
        else:
            config = {}
            with open(file_path, "r") as file:
                for line_ in file:
                    key, value = line_.strip().split("=", 1)
                    config[key] = value if value != "None" else None

            machine_name = config.get("Machine_NAME", default_machine_name)
            print(f"Configurations from file: Machine_NAME={machine_name}")
            return machine_name
    except Exception as e:
        print(f"Error: {e}, while managing HVIR configuration file.")
        return None


config_machine_name = manage_ip_config()
print(config_machine_name, type(config_machine_name))
