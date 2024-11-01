# 2024.09.19 v1.1 CN.Wang do some modify for error
# 2024.09.23 v1.2 CN.Wang fix DB error
# 2024.09.24 v1.3 Assistant fix duplicate entry issue
# 2024.09.25 v1.4 Assistant handle encoding issues with Chinese characters
# 2024.09.26 v1.5 Assistant fix Chinese characters display in Excel
# 2024.09.27 v1.6 CN.Wang fix the out of memory error with chunksize to 10000

import os
import pandas as pd
from sqlalchemy import create_engine, MetaData, Table, insert, update, String, Integer, Float, DateTime
from sqlalchemy.dialects.mssql import NVARCHAR
from sqlalchemy.exc import OperationalError
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from datetime import datetime
import logging
import time
import io
import chardet  # Import chardet for encoding detection

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Database connection information
server = '192.168.65.42'
port = '55159'
database = 'm100'
username = 'ptse'
password = 'ptse'

# SQLAlchemy engine and metadata
connection_string = (
    f'mssql+pyodbc://{username}:{password}@{server},{port}/{database}?'
    'driver=ODBC+Driver+17+for+SQL+Server'
)
engine = create_engine(
    connection_string,
    pool_size=10,
    max_overflow=20,
    pool_recycle=3600,    # Recycle connections every hour
    pool_pre_ping=True    # Test connections for liveness
)
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

def is_datetime(string):
    try:
        datetime.strptime(string.strip(), '%Y/%m/%d %H:%M:%S')
        return True
    except ValueError:
        return False

def read_file_with_detected_encoding(file_path):
    expected_num_fields = len(columns[1:])  # Excluding the 'Tester' column

    # Read the file as bytes
    with open(file_path, 'rb') as file:
        raw_data = file.read()

    # Detect encoding
    result = chardet.detect(raw_data)
    encoding = result['encoding']
    confidence = result['confidence']
    logging.info(f"Detected encoding: {encoding} with confidence {confidence}")

    if encoding is None:
        raise ValueError("Unable to detect encoding of the file.")

    # Decode the raw data using the detected encoding
    try:
        content = raw_data.decode(encoding)
    except UnicodeDecodeError as e:
        logging.error(f"Failed to decode file using encoding {encoding}: {e}")
        raise ValueError("Unable to decode the file with the detected encoding.")

    # Proceed with the rest of the processing
    lines = content.splitlines()

    data_rows = []
    i = 0
    while i < len(lines):
        buffer = ''
        content_list = []
        while i < len(lines):
            line = lines[i].rstrip('\n')
            buffer += line
            content_list = buffer.strip().split('@')
            last_field = content_list[-1]
            # Check if the last field is a datetime
            if is_datetime(last_field):
                # We have potentially a complete data row
                i += 1  # Move to next line for the next iteration
                break
            else:
                # The last field is not a datetime, need to read the next line
                i += 1
                if i >= len(lines):
                    # End of file, but still no datetime
                    logging.error("Incomplete data row found at end of file.")
                    break
                # Append next line to buffer
                buffer += ' '
        else:
            # Reached end of file
            break

        # Now, content_list should end with a datetime
        # Handle extra fields in AlarmMsg
        if len(content_list) > expected_num_fields:
            # Combine extra fields into AlarmMsg
            alarm_msg_index = expected_num_fields - 2  # Index of 'AlarmMsg' in columns[1:]
            # Combine fields from alarm_msg_index to the second last field
            content_list[alarm_msg_index] = '@'.join(content_list[alarm_msg_index:-1])
            # Keep only the expected number of fields
            content_list = content_list[:alarm_msg_index+1] + [content_list[-1]]
        elif len(content_list) < expected_num_fields:
            logging.error(f"Not enough fields in data. Expected {expected_num_fields}, got {len(content_list)}. Data: {content_list}")
            continue

        data_rows.append(content_list)

    if data_rows:
        df = pd.DataFrame(data_rows, columns=columns[1:])
        return df
    else:
        logging.error("No complete data rows found in the file.")
        return None

class Watcher(FileSystemEventHandler):
    def __init__(self):
        self.last_processed_times = {}

    def on_modified(self, event):
        if not event.is_directory and event.src_path.endswith('.txt'):
            mod_time = os.path.getmtime(event.src_path)
            last_processed = self.last_processed_times.get(event.src_path, 0)
            if mod_time != last_processed:
                time.sleep(1)
                self.process_file(event.src_path)
                self.last_processed_times[event.src_path] = mod_time
            else:
                logging.info(f"File {event.src_path} has already been processed.")

    def process_file(self, file_path):
        logging.info(f"Processing file: {file_path}")

        file_name = os.path.basename(file_path)
        machine_name = file_name.split('_')[0]

        try:
            df = read_file_with_detected_encoding(file_path)
            if df is None:
                logging.error("DataFrame is None, skipping file.")
                return
        except ValueError as e:
            logging.error(f"Failed to read file: {str(e)}")
            return

        df.insert(0, "Tester", machine_name)
        # Ensure 'Datetime' in df is in datetime format
        df['Datetime'] = pd.to_datetime(df['Datetime'], format='%Y/%m/%d %H:%M:%S', errors='coerce')

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
            return pd.to_datetime(value, format='%Y/%m/%d %H:%M:%S', errors='coerce')
        except (ValueError, TypeError):
            logging.warning(f"Failed to convert column '{key}' value '{value}' to datetime. Setting to current time.")
            return datetime.now()

    def safe_convert_to_string(self, key, value, max_length):
        if value is None:
            return None
        value = str(value).strip()
        return value[:max_length]

    def update_database(self, machine_name, data_dict):
        try:
            with engine.connect() as connection:
                table = Table('ntcent_HT_MachineStatus', metadata, autoload_with=engine)

                # Start a transaction
                with connection.begin():
                    # Check for existing entry
                    select_stmt = table.select().where(table.c.Tester == machine_name)
                    existing_entry = connection.execute(select_stmt).fetchone()

                    if existing_entry:
                        update_stmt = (
                            update(table)
                            .where(table.c.Tester == machine_name)
                            .values(**data_dict)
                        )
                        connection.execute(update_stmt)
                        logging.info(f"{machine_name} data updated in database.")
                    else:
                        insert_stmt = insert(table).values(**data_dict)
                        connection.execute(insert_stmt)
                        logging.info(f"{machine_name} data inserted into database.")
        except OperationalError as e:
            logging.error(f"OperationalError during database operation: {str(e)}")
            logging.error("Attempting to reconnect and retry...")
            # Optional: Implement retry logic here
        except Exception as e:
            logging.error(f"Database operation failed: {str(e)}")
            logging.error(f"Problematic data: {data_dict}")

    def update_daily_file(self, df, machine_name, data_dict):
        date_str = datetime.now().strftime('%Y%m%d')
        daily_file_name = f'HT_MachineStatus_{date_str}.csv'
        chunksize = 10000  # Define the chunk size

        # Ensure 'Datetime' in df is in datetime format
        df['Datetime'] = pd.to_datetime(df['Datetime'], format='%Y/%m/%d %H:%M:%S', errors='coerce')

        if os.path.isfile(daily_file_name):
            # Read the existing CSV in chunks to avoid memory overload
            chunk_iterator = pd.read_csv(daily_file_name, encoding='utf-8-sig', chunksize=chunksize)

            # Variable to track if we should append or create a new file
            is_first_chunk = True
            duplicate_found = False

            for chunk in chunk_iterator:
                # Convert 'Datetime' to datetime objects in the chunk
                chunk['Datetime'] = pd.to_datetime(chunk['Datetime'], errors='coerce')

                # Check for duplicates based on 'Tester' and 'Datetime'
                is_duplicate = ((chunk['Tester'] == data_dict['Tester']) & (chunk['Datetime'] == data_dict['Datetime'])).any()

                if is_duplicate:
                    duplicate_found = True
                    break

                # Write the chunk back to the file in append mode (except the first chunk)
                chunk.to_csv(daily_file_name, mode='a' if not is_first_chunk else 'w', header=is_first_chunk, index=False, encoding='utf-8-sig')
                is_first_chunk = False  # After the first write, switch to append mode

            if not duplicate_found:
                # Append the new data if no duplicate was found
                df.to_csv(daily_file_name, mode='a', header=False, index=False, encoding='utf-8-sig')
                logging.info(f"Data for {machine_name} appended to {daily_file_name}.")
            else:
                logging.info(f"Duplicate entry for {machine_name} on {data_dict['Datetime']} found, not saving.")
        else:
            # Create the CSV file if it doesn't exist
            df.to_csv(daily_file_name, mode='w', header=True, index=False, encoding='utf-8-sig')
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
