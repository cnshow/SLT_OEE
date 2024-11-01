# 2024.10.09 v1.0 CN.Wang 1st version
# 2024.10.21 v1.1 CN.Wang add option HT and YY for runrate
# 2024.10.25 v1.2 CN.Wang use HT data to calculate runrate, writing to ntcent_daily_runtime_ht
#

# 讀入的資料表:
# - ntcent_layout
# - [m100].[dbo].[ntcent_status]

# 寫入的資料表:
# - ntcent_daily_runtime_ht

# 程式邏輯重點:
# 1. 連接數據庫並讀取測試機佈局和狀態數據
# 2. 處理狀態數據，計算運行率
# 3. 創建樞紐表，計算每個狀態的分鐘數
# 4. 格式化數據並計算總時間
# 5. 將處理後的數據插入或更新到數據庫

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
import numpy as np
from datetime import date, timedelta

# Database connection details
server = '192.168.65.42:55159'
database = 'm100'
username = 'ptse'
password = 'ptse'

# Create database engine
engine = create_engine(f'mssql+pymssql://{username}:{password}@{server}/{database}')
Session = sessionmaker(bind=engine)


# option
siteoff_option = 'HT'  # HT or YY

# Specify the date you want (YYYY-MM-DD)
start_date = date(2024, 10, 26)
stop_date = start_date + timedelta(days=1)
period_days = 1


# Date selection
#yesterday = date.today() - timedelta(days=1)
#start_date = yesterday  # You can change this to any specific date if needed
#stop_date = start_date + timedelta(days=1)
#period_days = 1

print(f"Processing data for {start_date} 00:00:00 ~ {start_date} 23:59:59, {period_days} day")

# SQL queries
Tester_Layout = "SELECT * FROM ntcent_layout"
Status_data = f"""
SELECT *
FROM [m100].[dbo].[ntcent_status]
WHERE 
[current_time] >= '{start_date} 00:00:00' AND
[current_time] < '{stop_date} 00:00:00' 
"""

# Connect to database and fetch data
df_tmp = pd.read_sql(Status_data, con=engine)
df_layout = pd.read_sql(Tester_Layout, con=engine)
df_layout['tester'] = df_layout['Tester'].str.strip()

# Merge data
df = pd.merge(df_tmp, df_layout, on='tester', how='left')

# Process status
df['status_new'] = np.where(
  (df['status'] == 'RUN') & df['step'].str.contains('RT'), 'RT',
  np.where(
      (df['status'] == 'RUN') & df['step'].str.contains('EQC'), 'EQC',
      np.where(
          (df['status'] == 'RUNP') & df['step'].str.contains('RT'), 'RTP',
          df['status']
      )
  )
)


df['runrate'] = np.where(
  (df['status_new'].isin(['RUN', 'RT', 'RUNP', 'RTP', 'EQC'])),
  np.where(
      (
          (siteoff_option != 'HT') | 
          ((siteoff_option == 'HT') & 
           ((df['HT_M_qty'] + df['HT_A_qty'] + df['HT_1_qty']) < 6))
      ),
      df['runsiteqty'] / df['maxsites'],
      np.where(
          (siteoff_option == 'HT') & 
          ((df['HT_M_qty'] + df['HT_A_qty'] + df['HT_1_qty']) >= 6),
          df['HT_1_qty'] / df['maxsites'],
          1                
      )
  ),
  1  # Default runrate for non-matching status
)

# Replace inf and -inf with NaN
df['runrate'] = df['runrate'].replace([np.inf, -np.inf], np.nan)

# Create pivot table
df_pivot = df.pivot_table(index='tester', columns='status_new', values='runrate', aggfunc='sum').reset_index()

# Calculate minutes
total_minutes = period_days * 1440
df_minutes = df_pivot.set_index('tester').apply(lambda x: (x * 5), axis=1).reset_index()

# Add 'SiteOff' column
df_minutes['SiteOff'] = total_minutes - df_minutes.iloc[:, 1:].sum(axis=1)

# Add 'Date' column
df_minutes['Date'] = start_date

# Reorder columns, putting 'Date' first
columns_order = ['Date', 'tester', 'RUN', 'RUNP', 'RT', 'RTP', 'EQC', 'RUNR', 'DOWN', 'REPAIR', 'SiteOff', 'SETUP', 'SETUPW', 'SETUPW_L', 'ENG', 'ENG_L', 'IDLE', 'IDLE_L']
df_formatted = df_minutes.reindex(columns=columns_order, fill_value=np.nan)

# Calculate Total column
df_formatted['Total'] = df_formatted.iloc[:, 2:].sum(axis=1)  # Start from index 2 to exclude 'Date' and 'tester'

# Format the DataFrame
df_formatted = df_formatted.apply(lambda col: col.map(lambda x: f'{x:.0f}' if isinstance(x, (float, int)) and not np.isnan(x) else '') if col.name not in ['Date', 'tester'] else col)

print(f"Total : {len(df_formatted)} testers")
print(df_formatted)

# Function to insert or update data in the database
def upsert_data(df, date):
  session = Session()
  try:
      for _, row in df.iterrows():
          merge_statement = text("""
          MERGE INTO ntcent_daily_runtime_ht AS target
          USING (VALUES (:date, :tester, :RUN, :RUNP, :RT, :RTP, :EQC, :RUNR, :DOWN, :REPAIR, 
                         :SiteOff, :SETUP, :SETUPW, :SETUPW_L, :ENG, :ENG_L, :IDLE, :IDLE_L, :Total))
          AS source (Date, tester, RUN, RUNP, RT, RTP, EQC, RUNR, DOWN, REPAIR, SiteOff, SETUP, SETUPW, SETUPW_L, ENG, ENG_L, IDLE, IDLE_L, Total)
          ON target.Date = source.Date AND target.tester = source.tester
          WHEN MATCHED THEN
              UPDATE SET
                  RUN = source.RUN, RUNP = source.RUNP, RT = source.RT, RTP = source.RTP,
                  EQC = source.EQC, RUNR = source.RUNR, DOWN = source.DOWN, REPAIR = source.REPAIR,
                  SiteOff = source.SiteOff, SETUP = source.SETUP, SETUPW = source.SETUPW,
                  SETUPW_L = source.SETUPW_L, ENG = source.ENG, ENG_L = source.ENG_L,
                  IDLE = source.IDLE, IDLE_L = source.IDLE_L, Total = source.Total
          WHEN NOT MATCHED THEN
              INSERT (Date, tester, RUN, RUNP, RT, RTP, EQC, RUNR, DOWN, REPAIR, SiteOff, SETUP, SETUPW, SETUPW_L, ENG, ENG_L, IDLE, IDLE_L, Total)
              VALUES (source.Date, source.tester, source.RUN, source.RUNP, source.RT, source.RTP, source.EQC, source.RUNR, source.DOWN, source.REPAIR,
                      source.SiteOff, source.SETUP, source.SETUPW, source.SETUPW_L, source.ENG, source.ENG_L, source.IDLE, source.IDLE_L, source.Total);
          """)
          
          params = {col: (int(row[col]) if row[col] != '' else None) for col in df.columns if col != 'Date' and col != 'tester'}
          params['date'] = date
          params['tester'] = row['tester']
          
          session.execute(merge_statement, params)
      
      session.commit()
      print("Data saved to database successfully!")
  except Exception as e:
      session.rollback()
      print(f"Error saving data: {e}")
  finally:
      session.close()

# Save data to database
upsert_data(df_formatted, start_date)