import datetime
from datetime import timedelta
import threading


class TimeManagementSystem:
    """
    Generic time management system that can handle multiple parameters
    (idle, breakdown, maintenance, etc.) with separate tables
    """

    def __init__(self, connection, cursor, logger):
        self.connection = connection
        self.c = cursor
        self.log = logger

        # Thread safety
        self._lock = threading.Lock()

        # Define your parameters and their corresponding table names
        self.parameters = {
            'idle': 'idle_time_data',
            'breakdown': 'breakdown_time_data',
            'maintenance': 'maintenance_time_data'
        }

        # Create tables for all parameters
        self._create_tables()

    def _create_tables(self):
        """Create tables for all parameters"""
        for param, table_name in self.parameters.items():
            self.c.execute(f'''CREATE TABLE IF NOT EXISTS {table_name}(
                               machine VARCHAR(20) NOT NULL,
                               {param}_id INTEGER NOT NULL,
                               date_ DATE NOT NULL,
                               shift VARCHAR(1) NOT NULL,
                               time_ DATETIME, 
                               startTime DATETIME NOT NULL, 
                               stopTime DATETIME,
                               duration INTEGER,
                               PRIMARY KEY ({param}_id AUTOINCREMENT))''')
        self.connection.commit()

    def add_duration(self, mc_name, parameter='idle'):
        """Generic method to add duration for any parameter"""
        with self._lock:
            try:
                if parameter not in self.parameters:
                    raise ValueError(f"Parameter '{parameter}' not supported")

                table_name = self.parameters[parameter]
                id_column = f"{parameter}_id"

                time_ = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                self.c.execute(
                    f'''SELECT startTime, {id_column} FROM {table_name} 
                        WHERE stopTime IS NULL AND machine=?''',
                    (mc_name,))
                start_list = self.c.fetchall()
                self.log.debug(f'start_list for {parameter}:{start_list}')

                for items in start_list:
                    startTime = items[0]
                    record_id = items[1]
                    duration = (datetime.datetime.now() - datetime.datetime.strptime(startTime,
                                                                                     "%Y-%m-%d %H:%M:%S")).seconds
                    self.c.execute(f'''UPDATE {table_name} SET time_=?, duration=?
                                       WHERE stopTime is NULL AND {id_column}=?''',
                                   (time_, duration, record_id))
                self.connection.commit()
            except Exception as e:
                self.log.error(f'Error: {str(e)}, Could not add {parameter} duration to the database.')

    def add_start_time(self, mc_name, prev_d, prev_s, status, parameter='idle'):
        """Generic method to add start time for any parameter"""
        with self._lock:
            try:
                if parameter not in self.parameters:
                    raise ValueError(f"Parameter '{parameter}' not supported")

                table_name = self.parameters[parameter]

                self.add_stop_time(mc_name, parameter)
                time_ = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

                if status:
                    self.c.execute(f"INSERT INTO {table_name}(date_, shift, time_, startTime, machine)"
                                   "VALUES (?,?,?,?,?)", (prev_d, prev_s, time_, time_, mc_name))
                    self.connection.commit()
                    self.log.info(f'Successful: {parameter} time is added successfully to the database for {mc_name}')
            except Exception as e:
                self.log.error(f'Error: {e}, Could not add {parameter} time to the database for {mc_name}')

    def add_stop_time(self, mc_name, parameter='idle'):
        """Generic method to add stop time for any parameter"""
        # Note: This method is called from add_start_time which already has the lock
        try:
            if parameter not in self.parameters:
                raise ValueError(f"Parameter '{parameter}' not supported")

            table_name = self.parameters[parameter]
            id_column = f"{parameter}_id"

            time_ = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            self.c.execute(
                f'''SELECT startTime, {id_column} FROM {table_name} 
                    WHERE stopTime IS NULL and machine = ?''',
                (mc_name,))
            start_list = self.c.fetchall()
            self.log.debug(start_list)

            for items in start_list:
                startTime = items[0]
                record_id = items[1]
                if datetime.datetime.now() > datetime.datetime.strptime(startTime, "%Y-%m-%d %H:%M:%S"):
                    duration = (datetime.datetime.now() - datetime.datetime.strptime(startTime,
                                                                                     "%Y-%m-%d %H:%M:%S")).seconds
                    self.c.execute(f'''UPDATE {table_name} SET time_=?, stopTime=?, duration=?
                                       WHERE stopTime is NULL AND {id_column}=?''',
                                   (time_, time_, duration, record_id))
                else:
                    stop_time = datetime.datetime.strptime(startTime, "%Y-%m-%d %H:%M:%S") + timedelta(seconds=10)
                    self.c.execute(f'''UPDATE {table_name} SET time_=?, stopTime=?, duration=?
                                       WHERE stopTime is NULL AND {id_column}=?''',
                                   (time_, stop_time, 10, record_id))
            self.connection.commit()
        except Exception as e:
            self.log.error(f'Error: {e}, Could not stop {parameter} time in the database for {mc_name}')

    def get_total_duration(self, mc_name, prev_d, prev_s, parameter='idle'):
        """Generic method to get total duration for any parameter"""
        with self._lock:
            try:
                if parameter not in self.parameters:
                    raise ValueError(f"Parameter '{parameter}' not supported")

                table_name = self.parameters[parameter]

                self.c.execute(f'''SELECT SUM(duration) FROM {table_name}
                                  WHERE machine=? AND date_=? AND shift=? GROUP BY shift''',
                               (mc_name, prev_d, prev_s))
                try:
                    duration_time = self.c.fetchone()
                except:
                    duration_time = [0]

                if duration_time is None:
                    return 0
                if type(duration_time) is tuple:
                    return duration_time[0]
                else:
                    return 0
            except Exception as e:
                self.log.error(f'Error: {e}, Could not get daily {parameter} time for {mc_name}')
                return 0

    def get_current_duration(self, mc_name, prev_d, prev_s, parameter='idle'):
        """Generic method to get current duration for any parameter"""
        with self._lock:
            try:
                if parameter not in self.parameters:
                    raise ValueError(f"Parameter '{parameter}' not supported")

                table_name = self.parameters[parameter]
                id_column = f"{parameter}_id"

                self.c.execute(f'''SELECT duration FROM {table_name}
                                  WHERE machine=? AND date_=? AND shift=? ORDER BY {id_column} DESC LIMIT 1''',
                               (mc_name, prev_d, prev_s))
                duration_time = self.c.fetchone()
                if duration_time is not None:
                    return duration_time[0]
                else:
                    return 0
            except Exception as e:
                self.log.error(f'Error: {e}, Could not get daily {parameter} time for {mc_name}')
                return 0

    def get_count(self, today, shift, parameter='idle'):
        """Generic method to get count for any parameter"""
        with self._lock:
            try:
                if parameter not in self.parameters:
                    raise ValueError(f"Parameter '{parameter}' not supported")

                table_name = self.parameters[parameter]
                id_column = f"{parameter}_id"

                self.c.execute(f"SELECT count({id_column}) FROM {table_name} WHERE date_=? AND shift=?",
                               (today, shift))
                count = self.c.fetchone()
                if count:
                    return count[0]
                else:
                    return 0
            except Exception as e:
                self.log.error(f"[-] Unable to fetch {parameter}_count Error: {e}")
                return 0

    # Convenience methods for specific parameters
    def add_idle_duration(self, mc_name):
        return self.add_duration(mc_name, 'idle')

    def add_breakdown_duration(self, mc_name):
        return self.add_duration(mc_name, 'breakdown')

    def add_maintenance_duration(self, mc_name):
        return self.add_duration(mc_name, 'maintenance')

    def add_idle_start_time(self, mc_name, prev_d, prev_s, idle_status):
        return self.add_start_time(mc_name, prev_d, prev_s, idle_status, 'idle')

    def add_breakdown_start_time(self, mc_name, prev_d, prev_s, breakdown_status):
        return self.add_start_time(mc_name, prev_d, prev_s, breakdown_status, 'breakdown')

    def add_maintenance_start_time(self, mc_name, prev_d, prev_s, maintenance_status):
        return self.add_start_time(mc_name, prev_d, prev_s, maintenance_status, 'maintenance')

    def add_idle_stop_time(self, mc_name):
        return self.add_stop_time(mc_name, 'idle')

    def add_breakdown_stop_time(self, mc_name):
        return self.add_stop_time(mc_name, 'breakdown')

    def add_maintenance_stop_time(self, mc_name):
        return self.add_stop_time(mc_name, 'maintenance')

    def get_total_idle_duration(self, mc_name, prev_d, prev_s):
        return self.get_total_duration(mc_name, prev_d, prev_s, 'idle')

    def get_total_breakdown_duration(self, mc_name, prev_d, prev_s):
        return self.get_total_duration(mc_name, prev_d, prev_s, 'breakdown')

    def get_total_maintenance_duration(self, mc_name, prev_d, prev_s):
        return self.get_total_duration(mc_name, prev_d, prev_s, 'maintenance')

    def get_current_idle_duration(self, mc_name, prev_d, prev_s):
        return self.get_current_duration(mc_name, prev_d, prev_s, 'idle')

    def get_current_breakdown_duration(self, mc_name, prev_d, prev_s):
        return self.get_current_duration(mc_name, prev_d, prev_s, 'breakdown')

    def get_current_maintenance_duration(self, mc_name, prev_d, prev_s):
        return self.get_current_duration(mc_name, prev_d, prev_s, 'maintenance')

    def get_idle_count(self, today, shift):
        return self.get_count(today, shift, 'idle')

    def get_breakdown_count(self, today, shift):
        return self.get_count(today, shift, 'breakdown')

    def get_maintenance_count(self, today, shift):
        return self.get_count(today, shift, 'maintenance')


class ThreadSafeTimeManagementSystem(TimeManagementSystem):
    """Thread-safe version that creates separate connections per thread"""

    def __init__(self, db_path, logger):
        self.db_path = db_path
        self.log = logger
        self._thread_local = threading.local()

        # Define parameters
        self.parameters = {
            'idle': 'idle_time_data',
            'breakdown': 'breakdown_time_data',
            'maintenance': 'maintenance_time_data'
        }

        # Create tables using main connection
        self._create_initial_tables()

    def _create_initial_tables(self):
        """Create tables using main connection"""
        import sqlite3
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        for param, table_name in self.parameters.items():
            cursor.execute(f'''CREATE TABLE IF NOT EXISTS {table_name}(
                               machine VARCHAR(20) NOT NULL,
                               {param}_id INTEGER NOT NULL,
                               date_ DATE NOT NULL,
                               shift VARCHAR(1) NOT NULL,
                               time_ DATETIME, 
                               startTime DATETIME NOT NULL, 
                               stopTime DATETIME,
                               duration INTEGER,
                               PRIMARY KEY ({param}_id AUTOINCREMENT))''')
        conn.commit()
        conn.close()

    def _get_connection(self):
        """Get thread-local database connection"""
        if not hasattr(self._thread_local, 'connection'):
            import sqlite3
            self._thread_local.connection = sqlite3.connect(self.db_path)
            self._thread_local.cursor = self._thread_local.connection.cursor()
        return self._thread_local.connection, self._thread_local.cursor

    def add_duration(self, mc_name, parameter='idle'):
        """Thread-safe version of add_duration"""
        try:
            if parameter not in self.parameters:
                raise ValueError(f"Parameter '{parameter}' not supported")

            conn, cursor = self._get_connection()
            table_name = self.parameters[parameter]
            id_column = f"{parameter}_id"

            time_ = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            cursor.execute(
                f'''SELECT startTime, {id_column} FROM {table_name} 
                    WHERE stopTime IS NULL AND machine=?''',
                (mc_name,))
            start_list = cursor.fetchall()
            self.log.debug(f'start_list for {parameter}:{start_list}')

            for items in start_list:
                startTime = items[0]
                record_id = items[1]
                duration = (datetime.datetime.now() - datetime.datetime.strptime(startTime,
                                                                                 "%Y-%m-%d %H:%M:%S")).seconds
                cursor.execute(f'''UPDATE {table_name} SET time_=?, duration=?
                                   WHERE stopTime is NULL AND {id_column}=?''',
                               (time_, duration, record_id))
            conn.commit()
        except Exception as e:
            self.log.error(f'Error: {str(e)}, Could not add {parameter} duration to the database.')

    def add_start_time(self, mc_name, prev_d, prev_s, status, parameter='idle'):
        """Thread-safe version of add_start_time"""
        try:
            if parameter not in self.parameters:
                raise ValueError(f"Parameter '{parameter}' not supported")

            conn, cursor = self._get_connection()
            table_name = self.parameters[parameter]

            self.add_stop_time(mc_name, parameter)
            time_ = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            if status:
                cursor.execute(f"INSERT INTO {table_name}(date_, shift, time_, startTime, machine)"
                               "VALUES (?,?,?,?,?)", (prev_d, prev_s, time_, time_, mc_name))
                conn.commit()
                self.log.info(f'Successful: {parameter} time is added successfully to the database for {mc_name}')
        except Exception as e:
            self.log.error(f'Error: {e}, Could not add {parameter} time to the database for {mc_name}')

    def add_stop_time(self, mc_name, parameter='idle'):
        """Thread-safe version of add_stop_time"""
        try:
            if parameter not in self.parameters:
                raise ValueError(f"Parameter '{parameter}' not supported")

            conn, cursor = self._get_connection()
            table_name = self.parameters[parameter]
            id_column = f"{parameter}_id"

            time_ = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            cursor.execute(
                f'''SELECT startTime, {id_column} FROM {table_name} 
                    WHERE stopTime IS NULL and machine = ?''',
                (mc_name,))
            start_list = cursor.fetchall()
            self.log.debug(start_list)

            for items in start_list:
                startTime = items[0]
                record_id = items[1]
                if datetime.datetime.now() > datetime.datetime.strptime(startTime, "%Y-%m-%d %H:%M:%S"):
                    duration = (datetime.datetime.now() - datetime.datetime.strptime(startTime,
                                                                                     "%Y-%m-%d %H:%M:%S")).seconds
                    cursor.execute(f'''UPDATE {table_name} SET time_=?, stopTime=?, duration=?
                                       WHERE stopTime is NULL AND {id_column}=?''',
                                   (time_, time_, duration, record_id))
                else:
                    stop_time = datetime.datetime.strptime(startTime, "%Y-%m-%d %H:%M:%S") + timedelta(seconds=10)
                    cursor.execute(f'''UPDATE {table_name} SET time_=?, stopTime=?, duration=?
                                       WHERE stopTime is NULL AND {id_column}=?''',
                                   (time_, stop_time, 10, record_id))
            conn.commit()
        except Exception as e:
            self.log.error(f'Error: {e}, Could not stop {parameter} time in the database for {mc_name}')

    def get_total_duration(self, mc_name, prev_d, prev_s, parameter='idle'):
        """Thread-safe version of get_total_duration"""
        try:
            if parameter not in self.parameters:
                raise ValueError(f"Parameter '{parameter}' not supported")

            conn, cursor = self._get_connection()
            table_name = self.parameters[parameter]

            cursor.execute(f'''SELECT SUM(duration) FROM {table_name}
                              WHERE machine=? AND date_=? AND shift=? GROUP BY shift''',
                           (mc_name, prev_d, prev_s))
            try:
                duration_time = cursor.fetchone()
            except:
                duration_time = [0]

            if duration_time is None:
                return 0
            if type(duration_time) is tuple:
                return duration_time[0]
            else:
                return 0
        except Exception as e:
            self.log.error(f'Error: {e}, Could not get daily {parameter} time for {mc_name}')
            return 0

    def get_current_duration(self, mc_name, prev_d, prev_s, parameter='idle'):
        """Thread-safe version of get_current_duration"""
        try:
            if parameter not in self.parameters:
                raise ValueError(f"Parameter '{parameter}' not supported")

            conn, cursor = self._get_connection()
            table_name = self.parameters[parameter]
            id_column = f"{parameter}_id"

            cursor.execute(f'''SELECT duration FROM {table_name}
                              WHERE machine=? AND date_=? AND shift=? ORDER BY {id_column} DESC LIMIT 1''',
                           (mc_name, prev_d, prev_s))
            duration_time = cursor.fetchone()
            if duration_time is not None:
                return duration_time[0]
            else:
                return 0
        except Exception as e:
            self.log.error(f'Error: {e}, Could not get daily {parameter} time for {mc_name}')
            return 0

    def get_count(self, today, shift, parameter='idle'):
        """Thread-safe version of get_count"""
        try:
            if parameter not in self.parameters:
                raise ValueError(f"Parameter '{parameter}' not supported")

            conn, cursor = self._get_connection()
            table_name = self.parameters[parameter]
            id_column = f"{parameter}_id"

            cursor.execute(f"SELECT count({id_column}) FROM {table_name} WHERE date_=? AND shift=?",
                           (today, shift))
            count = cursor.fetchone()
            if count:
                return count[0]
            else:
                return 0
        except Exception as e:
            self.log.error(f"[-] Unable to fetch {parameter}_count Error: {e}")
            return 0

    # Add convenience methods for the thread-safe version
    def add_idle_duration(self, mc_name):
        return self.add_duration(mc_name, 'idle')

    def add_breakdown_duration(self, mc_name):
        return self.add_duration(mc_name, 'breakdown')

    def add_maintenance_duration(self, mc_name):
        return self.add_duration(mc_name, 'maintenance')

    def add_idle_start_time(self, mc_name, prev_d, prev_s, idle_status):
        return self.add_start_time(mc_name, prev_d, prev_s, idle_status, 'idle')

    def add_breakdown_start_time(self, mc_name, prev_d, prev_s, breakdown_status):
        return self.add_start_time(mc_name, prev_d, prev_s, breakdown_status, 'breakdown')

    def add_maintenance_start_time(self, mc_name, prev_d, prev_s, maintenance_status):
        return self.add_start_time(mc_name, prev_d, prev_s, maintenance_status, 'maintenance')

    def add_idle_stop_time(self, mc_name):
        return self.add_stop_time(mc_name, 'idle')

    def add_breakdown_stop_time(self, mc_name):
        return self.add_stop_time(mc_name, 'breakdown')

    def add_maintenance_stop_time(self, mc_name):
        return self.add_stop_time(mc_name, 'maintenance')

    def get_total_idle_duration(self, mc_name, prev_d, prev_s):
        return self.get_total_duration(mc_name, prev_d, prev_s, 'idle')

    def get_total_breakdown_duration(self, mc_name, prev_d, prev_s):
        return self.get_total_duration(mc_name, prev_d, prev_s, 'breakdown')

    def get_total_maintenance_duration(self, mc_name, prev_d, prev_s):
        return self.get_total_duration(mc_name, prev_d, prev_s, 'maintenance')

    def get_current_idle_duration(self, mc_name, prev_d, prev_s):
        return self.get_current_duration(mc_name, prev_d, prev_s, 'idle')

    def get_current_breakdown_duration(self, mc_name, prev_d, prev_s):
        return self.get_current_duration(mc_name, prev_d, prev_s, 'breakdown')

    def get_current_maintenance_duration(self, mc_name, prev_d, prev_s):
        return self.get_current_duration(mc_name, prev_d, prev_s, 'maintenance')

    def get_idle_count(self, today, shift):
        return self.get_count(today, shift, 'idle')

    def get_breakdown_count(self, today, shift):
        return self.get_count(today, shift, 'breakdown')

    def get_maintenance_count(self, today, shift):
        return self.get_count(today, shift, 'maintenance')


# =============================================================================
# SIMULATION AND TESTING CODE
# =============================================================================

import sqlite3
import logging
import time
import random
from threading import Thread


class MockLogger:
    """Mock logger for testing"""

    def debug(self, msg): print(f"[DEBUG] {msg}")

    def info(self, msg): print(f"[INFO] {msg}")

    def error(self, msg): print(f"[ERROR] {msg}")


class TimeManagementSimulation:
    """Simulation class to test the TimeManagementSystem"""

    def __init__(self):
        # Setup database
        self.connection = sqlite3.connect(':memory:')  # In-memory database for testing
        self.cursor = self.connection.cursor()
        self.logger = MockLogger()

        # Initialize the time management system
        self.time_manager = TimeManagementSystem(self.connection, self.cursor, self.logger)

        # Test data
        self.machines = ['Machine_A', 'Machine_B', 'Machine_C']
        self.parameters = ['idle', 'breakdown', 'maintenance']
        self.date = datetime.datetime.now().strftime("%Y-%m-%d")
        self.shift = 'A'

    def simulate_machine_events(self, machine_name, parameter, duration_seconds=5):
        """Simulate a machine event (idle/breakdown/maintenance)"""
        print(f"\n--- Simulating {parameter} event for {machine_name} ---")

        # Start the event
        print(f"Starting {parameter} for {machine_name}")
        self.time_manager.add_start_time(machine_name, self.date, self.shift, True, parameter)

        # Simulate some time passing
        print(f"Waiting {duration_seconds} seconds...")
        time.sleep(duration_seconds)

        # Update duration during the event
        self.time_manager.add_duration(machine_name, parameter)
        current_duration = self.time_manager.get_current_duration(machine_name, self.date, self.shift, parameter)
        print(f"Current {parameter} duration: {current_duration} seconds")

        # Stop the event
        print(f"Stopping {parameter} for {machine_name}")
        self.time_manager.add_stop_time(machine_name, parameter)

        # Get final stats
        total_duration = self.time_manager.get_total_duration(machine_name, self.date, self.shift, parameter)
        count = self.time_manager.get_count(self.date, self.shift, parameter)

        print(f"Total {parameter} duration for {machine_name}: {total_duration} seconds")
        print(f"Total {parameter} events today: {count}")

        return total_duration

    def test_basic_functionality(self):
        """Test basic functionality of the system"""
        print("=" * 60)
        print("TESTING BASIC FUNCTIONALITY")
        print("=" * 60)

        machine = self.machines[0]

        # Test each parameter
        for param in self.parameters:
            print(f"\n{'=' * 40}")
            print(f"Testing {param.upper()} functionality")
            print(f"{'=' * 40}")

            # Test the event
            duration = self.simulate_machine_events(machine, param, 3)

            assert duration > 0, f"{param} duration should be greater than 0"
            print(f"✓ {param} test passed!")

    def test_convenience_methods(self):
        """Test the convenience methods (backward compatibility)"""
        print("\n" + "=" * 60)
        print("TESTING CONVENIENCE METHODS")
        print("=" * 60)

        machine = self.machines[1]

        # Test idle convenience methods
        print("\n--- Testing Idle Convenience Methods ---")
        self.time_manager.add_idle_start_time(machine, self.date, self.shift, True)
        time.sleep(2)
        self.time_manager.add_idle_duration(machine)
        self.time_manager.add_idle_stop_time(machine)
        idle_total = self.time_manager.get_total_idle_duration(machine, self.date, self.shift)
        idle_count = self.time_manager.get_idle_count(self.date, self.shift)

        print(f"Idle duration: {idle_total} seconds, Count: {idle_count}")
        assert idle_total > 0, "Idle duration should be greater than 0"

        # Test breakdown convenience methods
        print("\n--- Testing Breakdown Convenience Methods ---")
        self.time_manager.add_breakdown_start_time(machine, self.date, self.shift, True)
        time.sleep(2)
        self.time_manager.add_breakdown_duration(machine)
        self.time_manager.add_breakdown_stop_time(machine)
        breakdown_total = self.time_manager.get_total_breakdown_duration(machine, self.date, self.shift)
        breakdown_count = self.time_manager.get_breakdown_count(self.date, self.shift)

        print(f"Breakdown duration: {breakdown_total} seconds, Count: {breakdown_count}")
        assert breakdown_total > 0, "Breakdown duration should be greater than 0"

        print("✓ Convenience methods test passed!")

    def test_multiple_machines_parallel(self):
        """Test multiple machines with parallel events"""
        print("\n" + "=" * 60)
        print("TESTING MULTIPLE MACHINES (PARALLEL)")
        print("=" * 60)

        threads = []
        results = {}

        def machine_worker(machine, param):
            duration = self.simulate_machine_events(machine, param, random.randint(2, 5))
            results[f"{machine}_{param}"] = duration

        # Start parallel events
        for machine in self.machines:
            for param in self.parameters:
                thread = Thread(target=machine_worker, args=(machine, param))
                threads.append(thread)
                thread.start()

        # Wait for all threads to complete
        for thread in threads:
            thread.join()

        # Verify results
        print("\n--- Final Results ---")
        for key, duration in results.items():
            print(f"{key}: {duration} seconds")
            assert duration > 0, f"{key} should have positive duration"

        print("✓ Parallel machines test passed!")

    def test_error_handling(self):
        """Test error handling"""
        print("\n" + "=" * 60)
        print("TESTING ERROR HANDLING")
        print("=" * 60)

        # Test invalid parameter
        try:
            self.time_manager.add_start_time("Machine_A", self.date, self.shift, True, "invalid_param")
            assert False, "Should have raised ValueError"
        except ValueError as e:
            print(f"✓ Correctly caught error: {e}")

        # Test getting data for non-existent machine
        duration = self.time_manager.get_total_duration("NonExistent", self.date, self.shift, "idle")
        assert duration == 0, "Non-existent machine should return 0 duration"
        print("✓ Non-existent machine handled correctly")

        print("✓ Error handling test passed!")

    def display_database_contents(self):
        """Display the contents of all tables"""
        print("\n" + "=" * 60)
        print("DATABASE CONTENTS")
        print("=" * 60)

        for param, table_name in self.time_manager.parameters.items():
            print(f"\n--- {table_name.upper()} ---")
            self.cursor.execute(f"SELECT * FROM {table_name}")
            rows = self.cursor.fetchall()

            if rows:
                # Get column names
                self.cursor.execute(f"PRAGMA table_info({table_name})")
                columns = [col[1] for col in self.cursor.fetchall()]
                print(f"Columns: {', '.join(columns)}")

                for row in rows:
                    print(row)
            else:
                print("No data")

    def run_full_simulation(self):
        """Run the complete simulation"""
        print("🚀 STARTING TIME MANAGEMENT SYSTEM SIMULATION")
        print("=" * 80)

        try:
            # Run all tests
            self.test_basic_functionality()
            self.test_convenience_methods()
            self.test_multiple_machines_parallel()
            self.test_error_handling()

            # Display results
            self.display_database_contents()

            print("\n" + "=" * 80)
            print("🎉 ALL TESTS PASSED! SIMULATION COMPLETED SUCCESSFULLY!")
            print("=" * 80)

        except Exception as e:
            print(f"\n❌ SIMULATION FAILED: {e}")
            import traceback
            traceback.print_exc()
        finally:
            self.connection.close()


# Quick test function
def quick_test():
    """Quick test function"""
    print("🧪 RUNNING QUICK TEST")
    print("-" * 40)

    # Setup
    conn = sqlite3.connect(':memory:')
    cursor = conn.cursor()
    logger = MockLogger()

    # Initialize
    tm = TimeManagementSystem(conn, cursor, logger)

    # Test one machine event
    machine = "TestMachine"
    date = datetime.datetime.now().strftime("%Y-%m-%d")
    shift = "A"

    # Start idle
    tm.add_idle_start_time(machine, date, shift, True)
    time.sleep(1)
    tm.add_idle_duration(machine)
    tm.add_idle_stop_time(machine)

    # Get results
    total = tm.get_total_idle_duration(machine, date, shift)
    count = tm.get_idle_count(date, shift)

    print(f"Machine: {machine}")
    print(f"Total idle time: {total} seconds")
    print(f"Idle events: {count}")

    assert total > 0, "Should have recorded idle time"
    assert count == 1, "Should have 1 idle event"

    print("✅ Quick test passed!")
    conn.close()


# Usage Examples:
if __name__ == "__main__":
    # Run full simulation
    simulation = TimeManagementSimulation()
    simulation.run_full_simulation()

    # Or run quick test
    # quick_test()