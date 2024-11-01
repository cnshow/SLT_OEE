# encoding:utf-8
#
# v0.0  2024.03.20  CN.Wang   Save tester status to database dbo.ntcent_status
# v1.0  2024.06.06  CN.Wang   Save SE name
 

import datetime
import pandas as pd 
import sys
import os
from sqlalchemy import create_engine
import numpy as np

#
# Main program
#


# connect RTC database
server   = '192.168.65.42:55159' 
database = 'm100' 
username = 'ptse' 
password = 'ptse' 

global idle_limit, setupw_limit, ENG_limit
idle_limit = 90
setupw_limit = 720
ENG_limit = 120

# SQL command remains unchanged
Tester_data = """
SELECT
rtrim([ent_customer]) as Customer,
[ent_entity] as Tester,
ISNULL(rtrim(ent_status),'IDLE') as Status,
rtrim(ent_code) as ent_code,
rtrim(ent_code)+':'+[code_desc] as Scode,
--[ent_product] as Device,
--[ent_package] as Package,
--[ent_lot_no] as Lotno,
ent_max_sites as MaxSites,
ent_run_sites as RunSites,
ent_last_update,
getdate() as [Current],
ISNULL(datediff(minute,ent_last_update,GETDATE()),0) as [Dtime],
ent_deleted,
[ent_step] as Step,
ent_dis_site as SiteOff,
(SELECT     TOP (1) operator.op_name
FROM         eq_hist LEFT JOIN
                     operator ON eq_hist.eq_operator = operator.op_id
WHERE     (eq_hist.eq_machine = ntcent.ent_entity) and ntcent.ent_status in ('RUNR','REPAIR','SETUP')
ORDER BY eq_hist.eq_start_time DESC) as UserName,

--空白Device查詢mt_data
case when ent_product = '' then  (select mt_device from(
select top 1 mt_device,mt_end
from mt_data8
where mt_machine = [ent_entity] and mt_end > DATEADD(day, -1, GETDATE())
union all
select top 1 mt_device,mt_end
from mt_data
where mt_machine = [ent_entity] and mt_end > DATEADD(day, -1, GETDATE())
order by mt_end desc) as AA) else ent_product end as Device,

--空白LotNo查詢mt_data
case when ent_lot_no = '' then  (select mt_rc_no from(
select top 1 mt_rc_no,mt_end
from mt_data8
where mt_machine = [ent_entity] and mt_end > DATEADD(day, -1, GETDATE())
union all
select top 1 mt_rc_no,mt_end
from mt_data
where mt_machine = [ent_entity] and mt_end > DATEADD(day, -1, GETDATE())
order by mt_end desc) as AA) else ent_lot_no end as Lotno,

--空白Package查詢mt_data
case when [ent_package] = '' then  (select mt_package from(
select top 1 mt_package,mt_end
from mt_data8
where mt_machine = [ent_entity] and mt_end > DATEADD(day, -1, GETDATE())
--order by mt_end desc
union all
select top 1 mt_package,mt_end
from mt_data
where mt_machine = [ent_entity] and mt_end > DATEADD(day, -1, GETDATE())
order by mt_end desc) as AA) else [ent_package] end as Package,

ent_qty as Qty, pass as Pass, total as Total
FROM ntcent
left outer join ft_run on ent_entity=machine
left outer join jcode on ent_code=code_id
where
ent_group = 'M100'
and ent_entity like 'QAM%'
and isnull(ent_deleted,'') <>'Y'
order by ent_order; 
"""

Tester_Layout = """
SELECT * FROM ntcent_layout
"""

MTK_Family = """
SELECT [PRODUCT_CODE] as Device
      ,[Family]
FROM [dbo].[oee_MTK_Family]
"""

Tester_Type = """
SELECT [ent_entity] as Tester  ,[ent_type] as Type
FROM [dbo].[ntcent_attr]
WHERE ent_entity LIKE 'QAME%'
   OR ent_entity LIKE 'QAMT%'
   OR ent_entity LIKE 'QAMN%'
"""

# Assuming you have already defined server, username, password, and database variables
engine = create_engine(f'mssql+pymssql://{username}:{password}@{server}/{database}')

# get test status 
df_tmp = pd.read_sql(Tester_data, con=engine)
df_tmp['Tester'] = df_tmp['Tester'].str.strip()
df_tmp['UserName'] = df_tmp['UserName'].str.strip()
#st.write(df)

# get layout
df_layout = pd.read_sql(Tester_Layout, con=engine)
df_layout['Tester'] = df_layout['Tester'].str.strip()

# get family
df_family = pd.read_sql(MTK_Family, con=engine)
df_family['Device'] = df_family['Device'].str.strip()

# get tester type
df_tester_type = pd.read_sql(Tester_Type, con=engine)
df_tester_type['Tester'] = df_tester_type['Tester'].str.strip()

# First, merge df_tmp and df_layout on 'Tester'
df_merged = pd.merge(df_tmp, df_layout, on='Tester', how='left')
df_merged2 = pd.merge(df_merged, df_family, on='Device', how='left')
df_merged2_clean = df_merged2.drop_duplicates()
df = pd.merge(df_merged2_clean, df_tester_type, on='Tester', how='left')


# seperate idle, ENG, setupw
df['Status'] = np.where((df['Status'] == 'IDLE') & (df['Dtime'] > idle_limit), 'IDLE_L', df['Status'])
df['Status'] = np.where((df['Status'] == 'ENG') & (df['Dtime'] > ENG_limit), 'ENG_L', df['Status'])
df['Status'] = np.where((df['Status'] == 'SETUPW') & (df['Dtime'] > setupw_limit), 'SETUPW_L', df['Status'])
df['SiteOff'] = df['SiteOff'].astype(str).str.strip()
df['OffQty'] = df['SiteOff'].astype(str).str.len()


# Assuming 'df' is your DataFrame
total_max_sites = df['MaxSites'].sum()  # Sum the values in the 'MaxSites' column
total_off_qty = df['OffQty'].sum()  # Sum the values in the 'OffQty' column
total_off_qty_runp = df[df['Status'] == 'RUNP']['OffQty'].sum()
idle_qty = len(df[(df['Status'] == 'IDLE_L')])

# Print the totals
#print("Total of MaxSites:", total_max_sites)
#print("Total of OffQty under RUNP: ", total_off_qty_runp,"Site Off Rate under RUNP: ", f"{total_off_qty_runp/total_max_sites*100:.2f}%")
#print("Total of OffQty under idle_L: ", idle_qty*6, "Site Off Rate under idle_L: ", f"{idle_qty*6/total_max_sites*100:.2f}%")


# Assuming 'df' is your DataFrame prepared earlier
grouped = df.groupby('Tester')

# Prepare a list to hold data for each row
data_to_insert = []

# 获取当前时间
current_time = datetime.datetime.now()
current_time_str = current_time.strftime("%Y-%m-%d %H:%M:%S")
# 确定当前日期和前一天的日期（如果当前时间在早上7点之前）
Ndate = current_time.strftime("%Y-%m-%d")
Sdate = (current_time - datetime.timedelta(days=1)).strftime("%Y-%m-%d") if current_time.time() < datetime.time(7, 0) else Ndate
# 确定班次
Shift = 'day' if datetime.time(7, 0) <= current_time.time() < datetime.time(19, 0) else 'night'
# 计算当前班级
start_date = datetime.date(2024, 5, 29)
class_schedule = ["A", "A", "B", "B"]
days_since_start = (current_time.date() - start_date).days
if Shift == 'day':
    current_class = class_schedule[days_since_start % 4]
else:
    # 夜班的班级在早上7点之前应该使用前一天的班级
    current_class = class_schedule[(days_since_start - 1) % 4] if current_time.time() < datetime.time(7, 0) else class_schedule[days_since_start % 4]


# Loop through grouped DataFrame to prepare data
for tester, group in grouped:
    customer = str(group['Customer'].iloc[0])
    status = str(group['Status'].iloc[0])
    step = str(group['Step'].iloc[0])
    maxsites = str(group['MaxSites'].iloc[0])
    offqty = str(group['OffQty'].iloc[0])
    device = str(group['Device'].iloc[0])   
    family = str(group['Family'].iloc[0])
    username = str(group['UserName'].iloc[0])

    # Logic to determine runsiteqty based on your condition
    runsiteqty = 0
    if status == 'RUNP':
        runsiteqty = int(maxsites) - int(offqty)
    elif status == 'RUN':
        runsiteqty = int(maxsites)
    
    # Add a row of data for insertion
    data_to_insert.append([current_time_str, Ndate, Sdate, Shift, customer, tester, status, step, maxsites, runsiteqty, device, family, username, current_class])  # Assuming RowID is auto-incremented or not needed
    
    # Prepare the formatted string
    log = f"{current_time} {Ndate} {Sdate} {Shift} {customer} {tester} {status} {step}  {maxsites} {runsiteqty} {device} {family} {username} {current_class}"
    #print(log)

# Create a DataFrame with the correct structure for insertion
df_to_insert = pd.DataFrame(data_to_insert, columns=['current_time', 'Ndate', 'Sdate', 'Shift', 'customer', 'tester', 'status', 'step', 'maxsites', 'runsiteqty', 'device', 'family', 'SE', 'Class'])

# Use SQLAlchemy to insert the DataFrame into the database
df_to_insert.to_sql('ntcent_status_test', con=engine, schema='dbo', index=False, if_exists='append')
