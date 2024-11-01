#
# 2024.09.19 v1.1 CN.Wang do some modify for error
#

import os
import pandas as pd
from sqlalchemy import create_engine, MetaData, Table, insert, update, String, Integer, Float, DateTime
from sqlalchemy.dialects.mssql import NVARCHAR
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from datetime import datetime
import logging
import time
import io

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Database connection information
server = '192.168.65.42'
port = '55159'
database = 'm100'
username = 'ptse'
password = 'ptse'

# SQLAlchemy engine and metadata
connection_string = f'mssql+pymssql://{username}:{password}@{server}:{port}/{database}'
engine = create_engine(connection_string)
metadata = MetaData()

# Define the directory to monitor
directory_to_watch = r'\\192.168.1.27\cu-ftp\HT_MachineState'

# Define the column names including the "Tester" column as the first column
columns = [
    "Tester", "HT_Status", "Status_1", "Status_2", "Status_3", "Status_4", "Status_5", "Status_6", 
    "Status_7", "Status_8", "Status_9", "Status_10", "Status_11", "Status_12", 
    "Jam_1", "Jam_2", "Jam_3", "Jam_4", "Jam_5", "Jam_6", "Jam_7", "Jam_8", "Jam_9", 
    "Jam_10", "Jam_11", "Jam_12", "Total_1", "Total_2", "Total_3", "Total_4", "Total_5", 
    "Total_6", "Total_7", "Total_8", "Total_9", "Total_10", "Total_11", "Total_12", 
    "Shuttle_Jam_1", "Shuttle_Jam_2", "Shuttle_Total_1", "Shuttle_Total_2", "AlarmMsg", "Datetime"
]

# Define column types
column_types = {
    "Tester": String(10),
    "HT_Status": String(50),
    "Status_1": String(1),
    "Status_2": String(1),
    "Status_3": String(1),
    "Status_4": String(1),
    "Status_5": String(1),
    "Status_6": String(1),
    "Status_7": String(1),
    "Status_8": String(1),
    "Status_9": String(1),
    "Status_10": String(1),
    "Status_11": String(1),
    "Status_12": String(1),
    "Jam_1": Integer,
    "Jam_2": Integer,
    "Jam_3": Integer,
    "Jam_4": Integer,
    "Jam_5": Integer,
    "Jam_6": Integer,
    "Jam_7": Integer,
    "Jam_8": Integer,
    "Jam_9": Integer,
    "Jam_10": Integer,
    "Jam_11": Integer,
    "Jam_12": Integer,
    "Total_1": Integer,
    "Total_2": Integer,
    "Total_3": Integer,
    "Total_4": Integer,
    "Total_5": Integer,
    "Total_6": Integer,
    "Total_7": Integer,
    "Total_8": Integer,
    "Total_9": Integer,
    "Total_10": Integer,
    "Total_11": Integer,
    "Total_12": Integer,
    "Shuttle_Jam_1": Integer,
    "Shuttle_Jam_2": Integer,
    "Shuttle_Total_1": Integer,
    "Shuttle_Total_2": Integer,
    "AlarmMsg": NVARCHAR(1000),
    "Datetime": DateTime
}

def read_file_with_multiple_encodings(file_path, encodings_to_try):
    for encoding in encodings_to_try:
        try:
            with open(file_path, 'r', encoding=encoding) as file:
                content = file.read()
            return pd.read_csv(io.StringIO(content), header=None, names=columns[1:], sep='@')
        except UnicodeDecodeError:
            logging.warning(f"Failed to read with {encoding} encoding.")
    raise ValueError("Unable to read the file with any of the provided encodings.")

class Watcher(FileSystemEventHandler):
    def on_modified(self, event):
        if not event.is_directory and event.src_path.endswith('.txt'):
            time.sleep(1)
            self.process_file(event.src_path)

    def on_created(self, event):
        if not event.is_directory and event.src_path.endswith('.txt'):
            time.sleep(1)
            self.process_file(event.src_path)

    def process_file(self, file_path):
        logging.info(f"Processing file: {file_path}")
        
        file_name = os.path.basename(file_path)
        machine_name = file_name.split('_')[0]
        
        try:
            encodings_to_try = ['utf-8', 'big5', 'utf-8-sig', 'cp1252', 'iso-8859-1']
            df = read_file_with_multiple_encodings(file_path, encodings_to_try)
        except ValueError as e:
            logging.error(f"Failed to read file: {str(e)}")
            return
        
        df.insert(0, "Tester", machine_name)
        data_dict = df.iloc[0].to_dict()
        
        for key, value in data_dict.items():
            if pd.isna(value):
                data_dict[key] = None
            elif column_types[key] == Integer:
                data_dict[key] = self.safe_convert_to_int(key, value)
            elif column_types[key] == Float:
                data_dict[key] = self.safe_convert_to_float(key, value)
            elif column_types[key] == DateTime and value is not None:
                data_dict[key] = self.safe_convert_to_datetime(key, value)
            elif isinstance(column_types[key], (String, NVARCHAR)):
                data_dict[key] = self.safe_convert_to_string(key, value, column_types[key].length)

        self.update_database(machine_name, data_dict)
        self.update_daily_file(df, machine_name, data_dict)

    def safe_convert_to_int(self, key, value):
        try:
            return int(float(value))
        except (ValueError, TypeError):
            logging.warning(f"Failed to convert column '{key}' value '{value}' to int. Setting to 0.")
            return 0

    def safe_convert_to_float(self, key, value):
        try:
            return float(value)
        except (ValueError, TypeError):
            logging.warning(f"Failed to convert column '{key}' value '{value}' to float. Setting to 0.0.")
            return 0.0

    def safe_convert_to_datetime(self, key, value):
        try:
            return pd.to_datetime(value)
        except (ValueError, TypeError):
            logging.warning(f"Failed to convert column '{key}' value '{value}' to datetime. Setting to current time.")
            return datetime.now()

    def safe_convert_to_string(self, key, value, max_length):
        if isinstance(value, (int, float)):
            return str(value)[:max_length]
        return str(value)[:max_length]

    def update_database(self, machine_name, data_dict):
        with engine.connect() as connection:
            table = Table('ntcent_HT_MachineStatus', metadata, autoload_with=engine)
            existing_entry = connection.execute(
                table.select().where(table.c.Tester == machine_name)
            ).fetchone()
            
            try:
                if existing_entry:
                    update_stmt = (
                        update(table)
                        .where(table.c.Tester == machine_name)
                        .values(data_dict)
                    )
                    connection.execute(update_stmt)
                    logging.info(f"{machine_name} data updated in database.")
                else:
                    insert_stmt = insert(table).values(data_dict)
                    connection.execute(insert_stmt)
                    logging.info(f"{machine_name} data inserted into database.")
            except Exception as e:
                logging.error(f"Database operation failed: {str(e)}")
                logging.error(f"Problematic data: {data_dict}")

    def update_daily_file(self, df, machine_name, data_dict):
        date_str = datetime.now().strftime('%Y%m%d')
        daily_file_name = f'HT_MachineStatus_{date_str}.csv'
        
        if os.path.isfile(daily_file_name):
            existing_df = pd.read_csv(daily_file_name)
            if not ((existing_df['Tester'] == data_dict['Tester']) & (existing_df['Datetime'] == data_dict['Datetime'])).any():
                df.to_csv(daily_file_name, mode='a', header=False, index=False)
                logging.info(f"Data for {machine_name} appended to {daily_file_name}.")
            else:
                logging.info(f"Duplicate entry for {machine_name} on {data_dict['Datetime']} found, not saving.")
        else:
            df.to_csv(daily_file_name, mode='w', header=True, index=False)
            logging.info(f"New file {daily_file_name} created and data saved.")

def main():
    observer = Observer()
    event_handler = Watcher()
    observer.schedule(event_handler, directory_to_watch, recursive=False)

    observer.start()
    logging.info(f"Watching directory: {directory_to_watch}")

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()

    observer.join()

if __name__ == "__main__":
    main()