import pandas as pd
from sqlalchemy import create_engine
import os
import logging

# 启用 SQLAlchemy 的 SQL 日志记录
logging.basicConfig()
logging.getLogger('sqlalchemy.engine').setLevel(logging.INFO)

# Function to read data from a CSV file and save it to SQL Server
def save_csv_to_sql(csv_file, table_name, engine):
    try:
        # Read the UTF-8 encoded CSV file
        df = pd.read_csv(csv_file, encoding='utf-8', sep='\t')

        print(f"Dataframe shape: {df.shape}")
        print(f"Dataframe head: {df.head()}")

        # Save the dataframe to the SQL table
        df.to_sql(table_name, con=engine, if_exists='append', index=False)
        print("Data has been successfully saved to the database.")
    except Exception as e:
        print(f"An error occurred while saving data to the database: {e}")
        if 'df' in locals():
            print(f"Dataframe shape: {df.shape}")
            print(f"Dataframe head: {df.head()}")

# Function to delete data from SQL Server
def delete_data(engine):
    try:
        # Delete query
        delete_query = """
        DELETE FROM [m100].[dbo].[ntcent_status]
        WHERE [current_time] > '2024-06-26 00:07:00' AND [current_time] < '2024-06-26 08:05:00';
        """
        with engine.connect() as conn:
            conn.execute(delete_query)
        print("Data has been successfully deleted from the database.")
    except Exception as e:
        print(f"An error occurred while deleting data from the database: {e}")

# Main function to execute the script
def main():
    try:
        # Database connection details
        server = '192.168.65.42:55159'
        database = 'm100'
        username = 'ptse'
        password = 'ptse'
        table_name = 'ntcent_status'  # Table name without schema

        # Create engine
        engine = create_engine(f'mssql+pymssql://{username}:{password}@{server}/{database}?charset=utf8')

        # Test the database connection
        with engine.connect() as conn:
            result = conn.execute("SELECT 1")
            if result.fetchone()[0] == 1:
                print("Successfully connected to the database.")
            else:
                print("Failed to verify the database connection.")

        # Delete the data in the specified range
        #delete_data(engine)

        # Use the UTF-8 CSV file saved with UltraEdit
        csv_file = os.path.join(os.path.dirname(__file__), 'status_1027-0205.csv')

        # Save the CSV data to SQL Server
        save_csv_to_sql(csv_file, table_name, engine)

    except Exception as e:
        print(f"An error occurred: {e}")

# Execute the main function
if __name__ == '__main__':
    main()
