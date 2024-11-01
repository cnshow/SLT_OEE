# encoding:utf-8

# v1.0  2024.06.11  CN.Wang   Save runtime data to database dbo.ntcent_runtime

import datetime
from datetime import date
from dateutil.relativedelta import relativedelta
import streamlit as st
import pandas as pd
from PIL import Image
from sqlalchemy import create_engine, text
import numpy as np

# Function to safely write status
def safe_write_status(status, dataframe):
    try:
        value = dataframe.loc[status, 'total_runrate']
        return value * 5 if not pd.isna(value) else 0
    except KeyError:
        return 0

# Function to process dataframe
def process_dataframe(df, period_days, runtime_ordered_list):
    df_pivot = df.pivot_table(index='tester', columns='status_new', values='runrate', aggfunc='sum').reset_index()
    df_percentage = df_pivot.set_index('tester').apply(lambda x: (x * 5) / (period_days * 1440) * 100, axis=1).reset_index()
    
    for column in df_percentage.columns[1:]:
        df_percentage[column] = pd.to_numeric(df_percentage[column], errors='coerce')
    
    df_percentage['SiteOff'] = 100.0001 - df_percentage.iloc[:, 1:].sum(axis=1)
    df_nan = df_percentage.fillna('')
    df_formatted = df_nan.applymap(lambda x: f'{x:6.2f}' if isinstance(x, (float, int)) else x)
    
    filtered_runtime_ordered_list = [col for col in runtime_ordered_list if col in df_formatted.columns]
    average_order_list = [col for col in filtered_runtime_ordered_list if col != 'tester']
    
    df_numeric = df_percentage[average_order_list].replace('', 0).astype(float)
    total_runtime_percentage = df_numeric.sum().to_frame().T
    test_qty = len(df_numeric)
    average_runtime_percentage = total_runtime_percentage / test_qty
    
    average_runtime_percentage_formatted = average_runtime_percentage.applymap(lambda x: f'{x:6.2f}' if isinstance(x, (float, int)) else x)
    
    for column in average_runtime_percentage_formatted.columns:
        average_runtime_percentage_formatted[column] = pd.to_numeric(average_runtime_percentage_formatted[column], errors='coerce')
    
    def get_column(df, col_name):
        return df[col_name] if col_name in df.columns else pd.Series([0] * len(df), index=df.index)
    
    runtime_summary = pd.DataFrame()
    runtime_summary['RUN'] = get_column(average_runtime_percentage_formatted, 'RUN') + get_column(average_runtime_percentage_formatted, 'RUNP')
    runtime_summary['RT'] = get_column(average_runtime_percentage_formatted, 'RT') + get_column(average_runtime_percentage_formatted, 'RTP')
    runtime_summary['EQC'] = get_column(average_runtime_percentage_formatted, 'EQC')
    runtime_summary['REPAIR'] = (get_column(average_runtime_percentage_formatted, 'REPAIR') + 
                                 get_column(average_runtime_percentage_formatted, 'RUNR') + 
                                 get_column(average_runtime_percentage_formatted, 'DOWN'))
    runtime_summary['SiteOff'] = get_column(average_runtime_percentage_formatted, 'SiteOff')
    runtime_summary['SETUP'] = (get_column(average_runtime_percentage_formatted, 'SETUP') + 
                                get_column(average_runtime_percentage_formatted, 'SETUPW') + 
                                get_column(average_runtime_percentage_formatted, 'SETUPW_L'))
    runtime_summary['ENG'] = get_column(average_runtime_percentage_formatted, 'ENG') + get_column(average_runtime_percentage_formatted, 'ENG_L')
    runtime_summary['IDLE_ops'] = get_column(average_runtime_percentage_formatted, 'IDLE')
    runtime_summary['IDLE_wait'] = get_column(average_runtime_percentage_formatted, 'IDLE_L')
    
    return df_formatted[filtered_runtime_ordered_list], average_runtime_percentage_formatted, runtime_summary

# Function to insert or update data into the database
def insert_data_to_db(engine, date, customer, df_summary, tester_qty):
    # Check if the record exists
    check_query = text("""
    SELECT COUNT(*) FROM [m100].[dbo].[ntcent_runtime] 
    WHERE [Date] = :Date AND [Customer] = :Customer
    """)
    
    check_data = {'Date': date, 'Customer': customer}
    
    with engine.connect() as conn:
        result = conn.execute(check_query, check_data).scalar()
    
    if result > 0:
        # Update the existing record
        update_query = text("""
        UPDATE [m100].[dbo].[ntcent_runtime]
        SET [RUN] = :RUN, [RUNP] = :RUNP, [RT] = :RT, [RTP] = :RTP, [EQC] = :EQC, 
            [RUNR] = :RUNR, [DOWN] = :DOWN, [REPAIR] = :REPAIR, [SiteOff] = :SiteOff, 
            [SETUP] = :SETUP, [SETUPW] = :SETUPW, [SETUPW_L] = :SETUPW_L, [ENG] = :ENG, 
            [ENG_L] = :ENG_L, [IDLE] = :IDLE, [IDLE_L] = :IDLE_L, [Tester_qty] = :Tester_qty
        WHERE [Date] = :Date AND [Customer] = :Customer
        """)
    else:
        # Insert a new record
        update_query = text("""
        INSERT INTO [m100].[dbo].[ntcent_runtime] 
        ([Date], [Customer], [RUN], [RUNP], [RT], [RTP], [EQC], [RUNR], [DOWN], [REPAIR], 
        [SiteOff], [SETUP], [SETUPW], [SETUPW_L], [ENG], [ENG_L], [IDLE], [IDLE_L], [Tester_qty])
        VALUES (:Date, :Customer, :RUN, :RUNP, :RT, :RTP, :EQC, :RUNR, :DOWN, :REPAIR, :SiteOff, 
        :SETUP, :SETUPW, :SETUPW_L, :ENG, :ENG_L, :IDLE, :IDLE_L, :Tester_qty)
        """)
    
    data = {
        'Date': date,
        'Customer': customer,
        'RUN': df_summary['RUN'].values[0] if 'RUN' in df_summary else 0,
        'RUNP': df_summary['RUNP'].values[0] if 'RUNP' in df_summary else 0,
        'RT': df_summary['RT'].values[0] if 'RT' in df_summary else 0,
        'RTP': df_summary['RTP'].values[0] if 'RTP' in df_summary else 0,
        'EQC': df_summary['EQC'].values[0] if 'EQC' in df_summary else 0,
        'RUNR': df_summary['RUNR'].values[0] if 'RUNR' in df_summary else 0,
        'DOWN': df_summary['DOWN'].values[0] if 'DOWN' in df_summary else 0,
        'REPAIR': df_summary['REPAIR'].values[0] if 'REPAIR' in df_summary else 0,
        'SiteOff': df_summary['SiteOff'].values[0] if 'SiteOff' in df_summary else 0,
        'SETUP': df_summary['SETUP'].values[0] if 'SETUP' in df_summary else 0,
        'SETUPW': df_summary['SETUPW'].values[0] if 'SETUPW' in df_summary else 0,
        'SETUPW_L': df_summary['SETUPW_L'].values[0] if 'SETUPW_L' in df_summary else 0,
        'ENG': df_summary['ENG'].values[0] if 'ENG' in df_summary else 0,
        'ENG_L': df_summary['ENG_L'].values[0] if 'ENG_L' in df_summary else 0,
        'IDLE': df_summary['IDLE'].values[0] if 'IDLE' in df_summary else 0,
        'IDLE_L': df_summary['IDLE_L'].values[0] if 'IDLE_L' in df_summary else 0,
        'Tester_qty': tester_qty
    }
    
    with engine.connect() as conn:
        conn.execute(update_query, data)


# Sidebar menu
start_date =  date.today() - relativedelta(days=1)
stop_date = start_date + relativedelta(days=1)
print(start_date, stop_date)
period = stop_date - start_date
period_days = period.days
period_min = period_days * 24 * 60
sampling_rate = 2

# Database connection
server = '192.168.65.42:55159'
database = 'm100'
username = 'ptse'
password = 'ptse'

# SQL queries
Tester_Layout = "SELECT * FROM ntcent_layout"
Tester_Type = """
SELECT [ent_entity] as Tester, [ent_type] as Type
FROM [dbo].[ntcent_attr]
WHERE ent_entity LIKE 'QAME%' OR ent_entity LIKE 'QAMT%' OR ent_entity LIKE 'QAMN%'
"""
Status_data = f"""
SELECT *
FROM [m100].[dbo].[ntcent_status]
WHERE [current_time] >= '{start_date} 07:00:00' AND [current_time] < '{stop_date} 06:59:59'
"""

# Create engine and load data
engine = create_engine(f'mssql+pymssql://{username}:{password}@{server}/{database}')
df_tmp = pd.read_sql(Status_data, con=engine)
df_layout = pd.read_sql(Tester_Layout, con=engine)
df_layout['tester'] = df_layout['Tester'].str.strip()
df = pd.merge(df_tmp, df_layout, on='tester', how='left')

df['status_new'] = np.where(
    (df['status'] == 'RUN') & df['step'].str.startswith('RT'), 'RT',
    np.where(
        (df['status'] == 'RUN') & df['step'].str.startswith('EQC'), 'EQC',
        np.where(
            (df['status'] == 'RUNP') & df['step'].str.startswith('RT'), 'RTP',
            df['status']
        )
    )
)
df['runrate'] = np.where((df['status'] == 'RUNP') | (df['status'] == 'RTP'), df['runsiteqty'] / df['maxsites'], 1)

df_MTK = df[df['customer'].str.startswith('MTK') | df['customer'].str.startswith('MSL')]
df_NVT = df[df['customer'].str.startswith('NVT')]

df['runrate'] = df['runrate'].replace([np.inf, -np.inf], np.nan)
df_MTK['runrate'] = df_MTK['runrate'].replace([np.inf, -np.inf], np.nan)
df_NVT['runrate'] = df_NVT['runrate'].replace([np.inf, -np.inf], np.nan)

runtime_ordered_list = ['tester', 'RUN', 'RUNP', 'RT', 'RTP', 'EQC', 'RUNR', 'DOWN', 'REPAIR', 'SiteOff', 'SETUP', 'SETUPW', 'SETUPW_L', 'ENG', 'ENG_L', 'IDLE', 'IDLE_L']

# Process dataframes
df_results = process_dataframe(df, period_days, runtime_ordered_list)
df_MTK_results = process_dataframe(df_MTK, period_days, runtime_ordered_list)
df_NVT_results = process_dataframe(df_NVT, period_days, runtime_ordered_list)

# Insert data into the database
insert_data_to_db(engine, start_date, 'ALL', df_results[1], len(df_results[0]))
insert_data_to_db(engine, start_date, 'MTK', df_MTK_results[1], len(df_MTK_results[0]))
insert_data_to_db(engine, start_date, 'NVT', df_NVT_results[1], len(df_NVT_results[0]))
