'''
CREATED BY: KENNETH LIANG
DATE: JULY 27, 2021
LAST CHANGE: August 6, 2021

THIS SCRIPT DOES THE FOLLOWING THREE THINGS.
ONE, MAKE A CONNECTION TO AN FTP AND PULLS DATA.
TWO, TAKES THE VARIOUS INPUT FILES AND PERFORMS SOME DATA ANALYSIS SUCH AS ADDING THE BUSINESS DAYS AGING.
THREE, UPLOAD AND APPEND THE RESULTS TO EXISTING GOOGLE SHEETS THAT RESIDE ON THE GOOGLE TEAM DRIVE.
'''
# google drive, sheets libraries that need to be imported for upload
from __future__ import print_function
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials

# Libraries needed for data ingestion / analysis
import pandas as pd
import numpy as np
import os
import datetime
import json # data conversion

import sys # Library to determine script directory

# Libraries needed for SFTP connection
import pysftp
import io

# determine if application is a script file or frozen exe
if getattr(sys, 'frozen', False):
    directory = os.path.dirname(os.path.realpath(sys.executable))
else:
    directory = os.path.abspath('')
    
# Method for authenticating Google Sheets login
def google_sheets():
    
    SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
    
    creds = None
    
    # The file .json stores the user's access and refresh tokens, and is created automatically when the authorization flow completes for the first time
    if os.path.exists(os.path.join(directory, '.json')):
        creds = Credentials.from_authorized_user_file(os.path.join(directory, '.json'), SCOPES)
    
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                os.path.join(directory, '.json'), SCOPES)
            creds = flow.run_local_server(port=0)
        
        # Save the credentials for the next run
        with open(os.path.join(directory, '.json'), 'w') as token:
            token.write(creds.to_json())
    
    # connect to Google Sheets using .json authentication files
    service = build('sheets', 'v4', credentials=creds)
    
    return service

def optimal_status(data):
    
    # initialize empty dataframe
    filter_data = pd.DataFrame()
    group_by_data = pd.DataFrame()

    # add new columns
    data['Optimal Status'] = np.where(data.OPTIMAL_KEEP == data.BOH, 'At Optimal', np.where(data.OPTIMAL_KEEP > data.BOH, 'Below Optimal', 'Above Optimal'))
    data['Optimal at Zero'] = np.where(data.OPTIMAL_KEEP == 0, 'Yes', 'No')
    # add the week number for the KPI data
    data['Week_Number'] = datetime.date.today() - datetime.timedelta(days=7)
    
    # change the formatting of date fields to month/day/year
    data['Week_Number'] = data['Week_Number'].apply(lambda x: x.strftime('%m/%d/%Y') if pd.notnull(x) else '')

    # Store weekly optimal data as a trend line of 'at optimal', 'above optimal' and 'below optimal'
    group_by_data = group_by_data.append(pd.pivot_table(data, index='Week_Number', columns='Optimal Status', aggfunc='size', fill_value=0))
    
    # Remove Week_Number title in the dataframe as it appears on it's own row. Rename the 'Optimal Status' title to Week_Number.
    group_by_data = group_by_data.reset_index(level=['Week_Number'])
    group_by_data = group_by_data.rename_axis('Week_Number', axis=1)
    group_by_data = group_by_data.rename_axis(None, axis=0)

    # This dataframe stores the raw data. However, due to the large size (~170K rows) we can't store this on a weekly basis. Instead, last weeks data will be replace (overwritten) by the current week.
    # Side note that Google Sheets has an upper limit of 5 million CELLS, not rows.
    filter_data = filter_data.append(data[['UNIT', 'DESCRIPTION', 'CUSTOMER CODE', 'STOCK_LOC_ID', 'STATE', 'BOH', 'OPTIMAL_KEEP', 'TWO_YR_USAGE', 'Optimal Status', 'Optimal at Zero']])

    return filter_data, group_by_data
    
def ingest_rpln_open_and_transfers(data):
    
    # initialize empty dataframe
    filter_data = pd.DataFrame()
    
    # change the data type of order date ORD_DATE from string to date
    data['ORD_DATE'] = pd.to_datetime(data['ORD_DATE'],errors='coerce')
    
    # calculate the business days aging for open replenishments
    data['Business_Days_Aging'] = np.busday_count(data['ORD_DATE'].values.astype('datetime64[D]'),np.datetime64(datetime.datetime.today()).astype('datetime64[D]'))
    
    # add the aging category based on the business days aging from above
    data['Aging_Category'] = np.where(data.Business_Days_Aging < 5, '<5', np.where((data.Business_Days_Aging >= 5) & (data.Business_Days_Aging < 10), '>=5', np.where((data.Business_Days_Aging >= 10) & (data.Business_Days_Aging < 20), '>=10', np.where((data.Business_Days_Aging >= 20) & (data.Business_Days_Aging < 40), '>=20', '>40'))))
    
    # add the week number for the KPI data
    data['Week_Number'] = datetime.date.today() - datetime.timedelta(days=7)
    
    # change the formatting of date fields to month/day/year
    data['ORD_DATE'] = data['ORD_DATE'].apply(lambda x: x.strftime('%m/%d/%Y'))
    data['Week_Number'] = data['Week_Number'].apply(lambda x: x.strftime('%m/%d/%Y'))
    
    # append data to the dataframe
    filter_data = filter_data.append(data)
    
    return filter_data
    
def ord_open_all_rsl(data):
    
    # initialize empty dataframe
    filter_data = pd.DataFrame()
    
    # filter data on order type MOL, order status B, O and PR
    data = data.query("ORD_TYPE == 'MOL' & (ORD_STATUS == 'B' or ORD_STATUS == 'O' or ORD_STATUS == 'PR')")  
    
    # change the data type of order date ORD_DATE from string to date
    data['ORD_DATE'] = pd.to_datetime(data['ORD_DATE'],errors='coerce')
    
    # calculate the business days aging for incomplete orders
    data['Business_Days_Aging'] = np.busday_count(data['ORD_DATE'].values.astype('datetime64[D]'),np.datetime64(datetime.datetime.today()).astype('datetime64[D]'))
    
    # add the aging category based on the business days aging from above
    data['Aging_Category'] = np.where(data.Business_Days_Aging < 10, '<10', np.where(data.Business_Days_Aging < 30, '<30', np.where(data.Business_Days_Aging < 60, '<60', '>60')))
    
    # add the week number for the KPI data
    data['Week_Number'] = datetime.date.today() - datetime.timedelta(days=7)
    
    # change the formatting of date fields to month/day/year
    data['ORD_DATE'] = data['ORD_DATE'].apply(lambda x: x.strftime('%m/%d/%Y'))
    data['Week_Number'] = data['Week_Number'].apply(lambda x: x.strftime('%m/%d/%Y'))
    
    # append data to the dataframe
    filter_data = filter_data.append(data)
    return filter_data

def cs_mol_return(data):
    
    # initialize empty dataframe
    filter_data = pd.DataFrame()
    
    # filter data on order status shippped
    data = data.query("STATUS == 'S'")
    
    # change the data type of ship time SHIP_TIME from string to date
    data['SHIP_TIME'] = pd.to_datetime(data['SHIP_TIME'],errors='coerce') 
    
    # initialize empty list
    Business_Days_Aging = []
    
    # Some rows of ship time will be empty indicating that it has not been shipped yet.
    # Cannot calculate business days aging on a start date of null. Need to check on null ship time
    # calculate the business days aging for central stock MOL returns
    for value in data['SHIP_TIME'].values.astype('datetime64[D]'):
        if str(value) != 'NaT':
            Business_Days_Aging.append(np.busday_count(value,np.datetime64(datetime.datetime.today()).astype('datetime64[D]')))
        else:
            Business_Days_Aging.append(np.nan)
    
    data['Business_Days_Aging'] = Business_Days_Aging
    
    # add the aging category based on the business days aging from above
    data['Aging_Category'] = np.where(data.Business_Days_Aging < 10, '<10', np.where(data.Business_Days_Aging < 30, '<30', np.where(data.Business_Days_Aging < 60, '<60', np.where(data.Business_Days_Aging >= 60, '>=60', 'Not shipped'))))
    
    # add the week number for the KPI data
    data['Week_Number'] = datetime.date.today() - datetime.timedelta(days=7)
    
    # change the formatting of date fields to month/day/year
    data['Week_Number'] = data['Week_Number'].apply(lambda x: x.strftime('%m/%d/%Y'))
    
    # append data to the dataframe
    filter_data = filter_data.append(data)
    return filter_data

def osl_tsl_mol_return(data):
    
    # initialize empty dataframe
    filter_data = pd.DataFrame()
    
    # change the data type of finalize date (FINALIZE_DATE) from string to date
    data['FINALIZE_DATE'] = pd.to_datetime(data['FINALIZE_DATE'],errors='coerce') 
    
    # initialize empty list
    Business_Days_Aging = []
    
    # Some rows of finalize time will be empty indicating that it has not been shipped yet.
    # Cannot calculate business days aging on a start date of null. Need to check on null finalize time
    # calculate the business days aging for field MOL returns
    for value in data['FINALIZE_DATE'].values.astype('datetime64[D]'):
        if str(value) != 'NaT':
            Business_Days_Aging.append(np.busday_count(value,np.datetime64(datetime.datetime.today()).astype('datetime64[D]')))
        else:
            Business_Days_Aging.append(np.nan)
    
    data['Business_Days_Aging'] = Business_Days_Aging
    
    # add the aging category based on the business days aging from above
    data['Aging_Category'] = np.where(data.Business_Days_Aging < 10, '<10', np.where(data.Business_Days_Aging < 30, '<30', np.where(data.Business_Days_Aging < 60, '<60', np.where(data.Business_Days_Aging >= 60, '>=60', 'No Finalize Date'))))
    
    # add the week number for the KPI data
    data['Week_Number'] = datetime.date.today() - datetime.timedelta(days=7)
    
    # change the formatting of date fields to month/day/year
    data['Week_Number'] = data['Week_Number'].apply(lambda x: x.strftime('%m/%d/%Y'))
    
    # append data to the dataframe
    filter_data = filter_data.append(data)
    return filter_data

def open_rpln_putaway(data):
    
    # initialize empty dataframes
    filter_data_MOL = pd.DataFrame()
    filter_data_NEW = pd.DataFrame()
    
    # filter data
    data_MOL = data.query("ORD_TYPE == 'MOL' or ORD_TYPE == 'SPARE-HOLD'")
    data_NEW = data.query("ORD_TYPE == 'NEW'")
    
    # change the data type of ship time SHIP_TIME from string to date
    data_MOL['SHIP_TIME'] = pd.to_datetime(data_MOL['SHIP_TIME'],errors='coerce') 
    data_NEW['SHIP_TIME'] = pd.to_datetime(data_NEW['SHIP_TIME'],errors='coerce') 
    
    # initialize empty list
    Business_Days_Aging_MOL = []
    Business_Days_Aging_NEW = []
    
    # Some rows of ship time will be empty indicating that it has not been shipped yet.
    # Cannot calculate business days aging on a start date of null. Need to check on null ship time
    # calculate the business days aging for aging put aways
    for value in data_MOL['SHIP_TIME'].values.astype('datetime64[D]'):
        if str(value) != 'NaT':
            Business_Days_Aging_MOL.append(np.busday_count(value,np.datetime64(datetime.datetime.today()).astype('datetime64[D]')))
        else:
            Business_Days_Aging_MOL.append(np.nan)
    
    for value in data_NEW['SHIP_TIME'].values.astype('datetime64[D]'):
        if str(value) != 'NaT':
            Business_Days_Aging_NEW.append(np.busday_count(value,np.datetime64(datetime.datetime.today()).astype('datetime64[D]')))
        else:
            Business_Days_Aging_NEW.append(np.nan)
    
    data_MOL['Business_Days_Aging'] = Business_Days_Aging_MOL
    data_NEW['Business_Days_Aging'] = Business_Days_Aging_NEW
    
    # add the aging category based on the business days aging from above
    data_MOL['Aging_Category'] = np.where(data_MOL.Business_Days_Aging < 10, '<10', np.where(data_MOL.Business_Days_Aging < 30, '<30', np.where(data_MOL.Business_Days_Aging < 60, '<60', np.where(data_MOL.Business_Days_Aging >= 60, '>=60', 'No shipping Info'))))
    data_NEW['Aging_Category'] = np.where(data_NEW.Business_Days_Aging < 10, '<10', np.where(data_NEW.Business_Days_Aging < 30, '<30', np.where(data_NEW.Business_Days_Aging < 60, '<60', np.where(data_NEW.Business_Days_Aging >= 60, '>=60', 'No shipping Info'))))
    
    # add the week number for the KPI data
    data_MOL['Week_Number'] = datetime.date.today() - datetime.timedelta(days=7)
    data_NEW['Week_Number'] = datetime.date.today() - datetime.timedelta(days=7)
    
    # change the formatting of date fields to month/day/year
    data_MOL['Week_Number'] = data_MOL['Week_Number'].apply(lambda x: x.strftime('%m/%d/%Y'))
    data_NEW['Week_Number'] = data_NEW['Week_Number'].apply(lambda x: x.strftime('%m/%d/%Y'))
    
    # append data to the dataframe
    filter_data_MOL = filter_data_MOL.append(data_MOL)
    filter_data_NEW = filter_data_NEW.append(data_NEW)
    return filter_data_MOL, filter_data_NEW

def ord_closed_rsl(data):
    
    # initialize empty dataframe
    filter_data = pd.DataFrame()
    
    # change the data type of order date ORD_DATE from string to date
    data['ORD_DATE'] = pd.to_datetime(data['ORD_DATE'],errors='coerce')
    data['ORDER_MODIFIED_DATE'] = pd.to_datetime(data['ORDER_MODIFIED_DATE'],errors='coerce')
    data['BORROWED_DATE'] = pd.to_datetime(data['BORROWED_DATE'],errors='coerce')
    data['PENDING_RETURN_DATE'] = pd.to_datetime(data['PENDING_RETURN_DATE'],errors='coerce')
    data['FINALIZE_DATE'] = pd.to_datetime(data['FINALIZE_DATE'],errors='coerce')
    data['RETURN_DATE'] = pd.to_datetime(data['RETURN_DATE'],errors='coerce')
    data['REPLEN_DATE'] = pd.to_datetime(data['REPLEN_DATE'],errors='coerce')
    data['RMS_CREATE_DATE'] = pd.to_datetime(data['RMS_CREATE_DATE'],errors='coerce')
    data['RMS_SHIP_TIME'] = pd.to_datetime(data['RMS_SHIP_TIME'],errors='coerce')
    data['RMS_RECV_TIME'] = pd.to_datetime(data['RMS_RECV_TIME'],errors='coerce')
    data['NMS_SHIP_TIME'] = pd.to_datetime(data['NMS_SHIP_TIME'],errors='coerce')
    
    # add the week number for the KPI data
    data['Week_Number'] = datetime.date.today() - datetime.timedelta(days=7)
    
    # calculate the business days aging for open replenishments
    data['Business_Days_Aging'] = np.busday_count(data['ORD_DATE'].values.astype('datetime64[D]'),data['REPLEN_DATE'].values.astype('datetime64[D]'))
    
    # change the formatting of date fields to month/day/year
    data['ORD_DATE'] = data['ORD_DATE'].apply(lambda x: x.strftime('%m/%d/%Y') if pd.notnull(x) else '')
    data['ORDER_MODIFIED_DATE'] = data['ORDER_MODIFIED_DATE'].apply(lambda x: x.strftime('%m/%d/%Y') if pd.notnull(x) else '')
    data['BORROWED_DATE'] = data['BORROWED_DATE'].apply(lambda x: x.strftime('%m/%d/%Y') if pd.notnull(x) else '')
    data['PENDING_RETURN_DATE'] = data['PENDING_RETURN_DATE'].apply(lambda x: x.strftime('%m/%d/%Y') if pd.notnull(x) else '')
    data['FINALIZE_DATE'] = data['FINALIZE_DATE'].apply(lambda x: x.strftime('%m/%d/%Y') if pd.notnull(x) else '')
    data['RETURN_DATE'] = data['RETURN_DATE'].apply(lambda x: x.strftime('%m/%d/%Y') if pd.notnull(x) else '')
    data['REPLEN_DATE'] = data['REPLEN_DATE'].apply(lambda x: x.strftime('%m/%d/%Y') if pd.notnull(x) else '')
    data['RMS_CREATE_DATE'] = data['RMS_CREATE_DATE'].apply(lambda x: x.strftime('%m/%d/%Y') if pd.notnull(x) else '')
    data['RMS_SHIP_TIME'] = data['RMS_SHIP_TIME'].apply(lambda x: x.strftime('%m/%d/%Y') if pd.notnull(x) else '')
    data['RMS_RECV_TIME'] = data['RMS_RECV_TIME'].apply(lambda x: x.strftime('%m/%d/%Y') if pd.notnull(x) else '')
    data['NMS_SHIP_TIME'] = data['NMS_SHIP_TIME'].apply(lambda x: x.strftime('%m/%d/%Y') if pd.notnull(x) else '')
    data['Week_Number'] = data['Week_Number'].apply(lambda x: x.strftime('%m/%d/%Y') if pd.notnull(x) else '')
    
    # append data to the dataframe
    filter_data = filter_data.append(data)
    
    return filter_data

def ingest_file_method(data):
    
    # initialize empty dataframe
    filter_data = pd.DataFrame()
    
    # add the week number for the KPI data
    data['Week_Number'] = datetime.date.today() - datetime.timedelta(days=7)
    data['Week_Number'] = data['Week_Number'].apply(lambda x: x.strftime('%m/%d/%Y') if pd.notnull(x) else '')
    
    # append data to the dataframe
    filter_data = filter_data.append(data)
    
    return filter_data

def zero_stock(data):
    
    # initialize empty dataframe
    filter_data = pd.DataFrame()
    group_by_orders = pd.DataFrame()
    group_by_no_orders = pd.DataFrame()

    # add new columns
    # add the week number for the KPI data
    data['Week_Number'] = datetime.date.today() - datetime.timedelta(days=7)
    
    # change the formatting of date fields to month/day/year
    data['Week_Number'] = data['Week_Number'].apply(lambda x: x.strftime('%m/%d/%Y') if pd.notnull(x) else '')

    # Store weekly optimal data as a trend line of 'at optimal', 'above optimal' and 'below optimal'
    group_by_no_orders = group_by_no_orders.append(pd.pivot_table(data[data.ORDER_REFERENCE.isnull()], index='Week_Number', columns='STATE', aggfunc='size', fill_value=0))
    
    # Remove Week_Number title in the dataframe as it appears on it's own row. Rename the 'Optimal Status' title to Week_Number.
    group_by_no_orders = group_by_no_orders.reset_index(level=['Week_Number'])
    group_by_no_orders = group_by_no_orders.rename_axis('Week_Number', axis=1)
    group_by_no_orders = group_by_no_orders.rename_axis(None, axis=0)
    
    # Store weekly optimal data as a trend line of 'at optimal', 'above optimal' and 'below optimal'
    group_by_orders = group_by_orders.append(pd.pivot_table(data, index='Week_Number', columns='STATE', values='ORDER_REFERENCE', aggfunc='count', fill_value=0))
    
    # Remove Week_Number title in the dataframe as it appears on it's own row. Rename the 'Optimal Status' title to Week_Number.
    group_by_orders = group_by_orders.reset_index(level=['Week_Number'])
    group_by_orders = group_by_orders.rename_axis('Week_Number', axis=1)
    group_by_orders = group_by_orders.rename_axis(None, axis=0)
    
    # This dataframe stores the raw data. However, due to the large size (~170K rows) we can't store this on a weekly basis. Instead, last weeks data will be replace (overwritten) by the current week.
    # Side note that Google Sheets has an upper limit of 5 million CELLS, not rows.
    filter_data = filter_data.append(data)

    return filter_data, group_by_orders, group_by_no_orders

service = google_sheets()

# Change directory of public key file. Otherwise it looks at the ~/.ssh/known_hosts directory locally
cnopts = pysftp.CnOpts(knownhosts=os.path.join(directory, '.pub'))

# Instruct pysftp to not look for hostkeys directory
cnopts.hostkeys = None

# Open SFTP (secure file transfer protocol) to internal gateway that controls connection to outside networks.
with pysftp.Connection(host='', username='', private_key=os.path.join(directory, '.pem'), cnopts=cnopts) as sftp:
    
    print("Connection succesfully established ... ")
    
    # Switch to a remote directory
    sftp.cwd('/..../Reporting/')
    
    # Obtain structure of the remote directory
    directory_structure = sftp.listdir()

    # Print file names
    for file in directory_structure:
        
        print(file)
        
        with io.BytesIO() as fl:
            
            # read data from the excel files pulled from the CTDI FTP
            if file == 'NMS_Call_log.xlsx':
                sftp.getfo(file, fl, callback=None)
                fl.seek(0)
                data = pd.read_excel(fl)
            else:
                sftp.getfo(file, fl, callback=None)
                fl.seek(0)
                data = pd.read_excel(fl,skiprows=1)
        
        if file == '01_ORD_OPEN_ALL_RSL.xlsx':
        
            # call the method
            filter_data = ord_open_all_rsl(data)

            # The ID and range of a Google spreadsheet.
            SPREADSHEET_ID = 'ENTER GOOGLE SHEET ID'
            RANGE_NAME = 'Sheet1!A1:AS1'
            
            # append data to Google Sheets
            request = service.spreadsheets().values().append(spreadsheetId=SPREADSHEET_ID, range=RANGE_NAME, valueInputOption='USER_ENTERED', insertDataOption='INSERT_ROWS', body={'values':json.loads(filter_data.to_json(date_unit='s', date_format='iso', orient='values'))})
            request.execute()

        elif file == '06_RPLN_OPEN.xlsx':

            # call the method
            filter_data = ingest_rpln_open_and_transfers(data)

            # The ID and range of a Google spreadsheet.
            SPREADSHEET_ID = 'ENTER GOOGLE SHEET ID'
            RANGE_NAME = 'Sheet1!A1:AF1'

            # append data to Google Sheets
            request = service.spreadsheets().values().append(spreadsheetId=SPREADSHEET_ID, range=RANGE_NAME, valueInputOption='USER_ENTERED', insertDataOption='INSERT_ROWS', body={'values':json.loads(filter_data.to_json(date_unit='s', date_format='iso', orient='values'))})
            request.execute()

        elif file == 'Incomplete_RSL_Transfer.xlsx':

            # call the method
            filter_data = ingest_rpln_open_and_transfers(data)

            # The ID and range of a Google spreadsheet.
            SPREADSHEET_ID = 'ENTER GOOGLE SHEET ID'
            RANGE_NAME = 'Sheet1!A1:AW1'

            # append data to Google Sheets
            request = service.spreadsheets().values().append(spreadsheetId=SPREADSHEET_ID, range=RANGE_NAME, valueInputOption='USER_ENTERED', insertDataOption='INSERT_ROWS', body={'values':json.loads(filter_data.to_json(date_unit='s', date_format='iso', orient='values'))})
            request.execute()

        elif file == '02_CS_MOL.xlsx':

            # call the method
            filter_data = cs_mol_return(data)

            # The ID and range of a Google spreadsheet.
            SPREADSHEET_ID = 'ENTER GOOGLE SHEET ID'
            RANGE_NAME = 'Sheet1!A1:Z1'

            # append data to Google Sheets
            request = service.spreadsheets().values().append(spreadsheetId=SPREADSHEET_ID, range=RANGE_NAME, valueInputOption='USER_ENTERED', insertDataOption='INSERT_ROWS', body={'values':json.loads(filter_data.to_json(date_unit='s', date_format='iso', orient='values'))})
            request.execute()

        elif file == '02_OSL_TSL_MOL.xlsx':

            # call the method
            filter_data = osl_tsl_mol_return(data)

            # The ID and range of a Google spreadsheet.
            SPREADSHEET_ID = 'ENTER GOOGLE SHEET ID'
            RANGE_NAME = 'Sheet1!A1:AJ1'

            # append data to Google Sheets
            request = service.spreadsheets().values().append(spreadsheetId=SPREADSHEET_ID, range=RANGE_NAME, valueInputOption='USER_ENTERED', insertDataOption='INSERT_ROWS', body={'values':json.loads(filter_data.to_json(date_unit='s', date_format='iso', orient='values'))})
            request.execute()

        elif file == '08_OPEN_RPLN_NEW_PUTAWAY.xlsx':

            # call the method
            filter_data_MOL, filter_data_NEW = open_rpln_putaway(data)

            # Range of Google spreadsheet. Both the MOL and NEW putaway have the same range.
            RANGE_NAME = 'Sheet1!A1:AB1'

            # The ID of a MOL putaway Google spreadsheet.
            SPREADSHEET_ID_1 = 'ENTER GOOGLE SHEET ID'

            # The ID of a NEW putaway Google spreadsheet.
            SPREADSHEET_ID_2 = 'ENTER GOOGLE SHEET ID'
            
            # append data to Google Sheets
            request = service.spreadsheets().values().append(spreadsheetId=SPREADSHEET_ID_1, range=RANGE_NAME, valueInputOption='USER_ENTERED', insertDataOption='INSERT_ROWS', body={'values':json.loads(filter_data_MOL.to_json(date_unit='s', date_format='iso', orient='values'))})
            request.execute()
            
            # append data to Google Sheets
            request = service.spreadsheets().values().append(spreadsheetId=SPREADSHEET_ID_2, range=RANGE_NAME, valueInputOption='USER_ENTERED', insertDataOption='INSERT_ROWS', body={'values':json.loads(filter_data_NEW.to_json(date_unit='s', date_format='iso', orient='values'))})
            request.execute()

        elif file == '01_ORD_CLOSED_RSL.xlsx':

            # call the method
            filter_data = ord_closed_rsl(data)

            # The ID and range of a Google spreadsheet.
            SPREADSHEET_ID = 'ENTER GOOGLE SHEET ID'
            RANGE_NAME = 'Sheet1!A1:AP1'

            # append data to Google Sheets
            request = service.spreadsheets().values().append(spreadsheetId=SPREADSHEET_ID, range=RANGE_NAME, valueInputOption='USER_ENTERED', insertDataOption='INSERT_ROWS', body={'values':json.loads(filter_data.to_json(date_unit='s', date_format='iso', orient='values'))})
            request.execute()

        elif file == "01_ORD_ALL_RSL.xlsx":

            # call the method
            filter_data = ingest_file_method(data)

            # The ID and range of a Google spreadsheet.
            SPREADSHEET_ID = 'ENTER GOOGLE SHEET ID'
            RANGE_NAME = 'Sheet1!A1:AQ1'

            # append data to Google Sheets
            request = service.spreadsheets().values().append(spreadsheetId=SPREADSHEET_ID, range=RANGE_NAME, valueInputOption='USER_ENTERED', insertDataOption='INSERT_ROWS', body={'values':json.loads(filter_data.to_json(date_unit='s', date_format='iso', orient='values'))})
            request.execute()

        elif file == "01_ORD_ALL_CS.xlsx":

            # call the method
            filter_data = ingest_file_method(data)

            # The ID and range of a Google spreadsheet.
            SPREADSHEET_ID = 'ENTER GOOGLE SHEET ID'
            RANGE_NAME = 'Sheet1!A1:Z1'

            # append data to Google Sheets
            request = service.spreadsheets().values().append(spreadsheetId=SPREADSHEET_ID, range=RANGE_NAME, valueInputOption='USER_ENTERED', insertDataOption='INSERT_ROWS', body={'values':json.loads(filter_data.to_json(date_unit='s', date_format='iso', orient='values'))})
            request.execute()

        elif file == "01_ORD_ALL_CS_CANCELLED.xlsx":

            # call the method
            filter_data = ingest_file_method(data)

            # The ID and range of a Google spreadsheet.
            SPREADSHEET_ID = 'ENTER GOOGLE SHEET ID'
            RANGE_NAME = 'Sheet1!A1:Z1'

            # append data to Google Sheets
            request = service.spreadsheets().values().append(spreadsheetId=SPREADSHEET_ID, range=RANGE_NAME, valueInputOption='USER_ENTERED', insertDataOption='INSERT_ROWS', body={'values':json.loads(filter_data.to_json(date_unit='s', date_format='iso', orient='values'))})
            request.execute()

        elif file == "01_ORD_CANCEL_RSL.xlsx":

            # call the method
            filter_data = ingest_file_method(data)

            # The ID and range of a Google spreadsheet.
            SPREADSHEET_ID = 'ENTER GOOGLE SHEET ID'
            RANGE_NAME = 'Sheet1!A1:AO1'

            # append data to Google Sheets
            request = service.spreadsheets().values().append(spreadsheetId=SPREADSHEET_ID, range=RANGE_NAME, valueInputOption='USER_ENTERED', insertDataOption='INSERT_ROWS', body={'values':json.loads(filter_data.to_json(date_unit='s', date_format='iso', orient='values'))})
            request.execute()

        elif file == "01_ORD_CS_NMS_CLOSED.xlsx":

            # call the method
            filter_data = ingest_file_method(data)

            # The ID and range of a Google spreadsheet.
            SPREADSHEET_ID = 'ENTER GOOGLE SHEET ID'
            RANGE_NAME = 'Sheet1!A1:U1'

            # append data to Google Sheets
            request = service.spreadsheets().values().append(spreadsheetId=SPREADSHEET_ID, range=RANGE_NAME, valueInputOption='USER_ENTERED', insertDataOption='INSERT_ROWS', body={'values':json.loads(filter_data.to_json(date_unit='s', date_format='iso', orient='values'))})
            request.execute()

        elif file == "06_RPLN_DUE.xlsx":

            # call the method
            filter_data = ingest_file_method(data)

            # The ID and range of a Google spreadsheet.
            SPREADSHEET_ID = 'ENTER GOOGLE SHEET ID'
            RANGE_NAME = 'Sheet1!A1:AI1'

            # append data to Google Sheets
            request = service.spreadsheets().values().append(spreadsheetId=SPREADSHEET_ID, range=RANGE_NAME, valueInputOption='USER_ENTERED', insertDataOption='INSERT_ROWS', body={'values':json.loads(filter_data.to_json(date_unit='s', date_format='iso', orient='values'))})
            request.execute()
        
        elif file == "OSL_TSL_Live_Sites.xlsx":

            # call the method
            filter_data = ingest_file_method(data)

            # The ID and range of a Google spreadsheet.
            SPREADSHEET_ID = 'ENTER GOOGLE SHEET ID'
            RANGE_NAME = 'Sheet1!A1'

            # append data to Google Sheets
            request = service.spreadsheets().batchUpdate(spreadsheetId=SPREADSHEET_ID, body={'requests': [{'updateCells': {'range': {'sheetId': '0'}, 'fields': 'userEnteredValue'}}]})
            request.execute()
            
            filter_data.replace(np.nan,'',inplace=True)
            request = service.spreadsheets().values().update(spreadsheetId=SPREADSHEET_ID, range=RANGE_NAME, valueInputOption='USER_ENTERED', body={'values':filter_data.T.reset_index().T.values.tolist()})
            request.execute()

        elif file == "RSL_Planning_Rpt.xlsx":

            # call the method
            filter_data, group_by_data = optimal_status(data)

            # The ID and range of a Google spreadsheet. This is for the Raw RSL Planning Report. Due to the size limitation of Google Sheet at 5 million cells. I've had to trim the columns in the report significantly.
            # Also, due to the number of rows in this report at ~170K, I will not be recording historical data. The Google Sheet data gets replaced weekly, not appended.
            SPREADSHEET_ID_1 = 'ENTER GOOGLE SHEET ID'
            RANGE_NAME_1 = 'Sheet1!A1'

             # The ID and range of a Google spreadsheet. This is for the Optimal Keep Level trend line report.
            SPREADSHEET_ID_2 = 'ENTER GOOGLE SHEET ID'
            RANGE_NAME_2 = 'Sheet1!A1:D1'

            # append data to Google Sheets
            request = service.spreadsheets().values().append(spreadsheetId=SPREADSHEET_ID_2, range=RANGE_NAME_2, valueInputOption='USER_ENTERED', insertDataOption='INSERT_ROWS', body={'values':json.loads(group_by_data.to_json(date_unit='s', date_format='iso', orient='values'))})
            request.execute()
            
            # append data to Google Sheets
            request = service.spreadsheets().batchUpdate(spreadsheetId=SPREADSHEET_ID_1, body={'requests': [{'updateCells': {'range': {'sheetId': '0'}, 'fields': 'userEnteredValue'}}]})
            request.execute()
            
            filter_data.replace(np.nan,'',inplace=True)
            request = service.spreadsheets().values().update(spreadsheetId=SPREADSHEET_ID_1, range=RANGE_NAME_1, valueInputOption='USER_ENTERED', body={'values':filter_data.T.reset_index().T.values.tolist()})
            request.execute()
            
        elif file == "Zero_Stock.xlsx":

            # call the method
            filter_data, group_by_orders, group_by_no_orders = zero_stock(data)

            # The ID and range of a Google spreadsheet. This is for the Raw RSL Planning Report. Due to the size limitation of Google Sheet at 5 million cells. I've had to trim the columns in the report significantly.
            # Also, due to the number of rows in this report at ~170K, I will not be recording historical data. The Google Sheet data gets replaced weekly, not appended.
            SPREADSHEET_ID_1 = 'ENTER GOOGLE SHEET ID'
            RANGE_NAME_1 = 'Sheet1!A1'

             # The ID and range of a Google spreadsheet. This is for the Optimal Keep Level trend line report.
            SPREADSHEET_ID_2 = 'ENTER GOOGLE SHEET ID'
            RANGE_NAME_2 = 'With Orders!A1:Q1'
            RANGE_NAME_3 = 'No Orders!A1:P1'
            
            # append data to Google Sheets
            request = service.spreadsheets().values().append(spreadsheetId=SPREADSHEET_ID_2, range=RANGE_NAME_2, valueInputOption='USER_ENTERED', insertDataOption='INSERT_ROWS', body={'values':json.loads(group_by_orders.to_json(date_unit='s', date_format='iso', orient='values'))})
            request.execute()
            
            # append data to Google Sheets
            request = service.spreadsheets().values().append(spreadsheetId=SPREADSHEET_ID_2, range=RANGE_NAME_3, valueInputOption='USER_ENTERED', insertDataOption='INSERT_ROWS', body={'values':json.loads(group_by_no_orders.to_json(date_unit='s', date_format='iso', orient='values'))})
            request.execute()
            
            # append data to Google Sheets
            request = service.spreadsheets().batchUpdate(spreadsheetId=SPREADSHEET_ID_1, body={'requests': [{'updateCells': {'range': {'sheetId': '0'}, 'fields': 'userEnteredValue'}}]})
            request.execute()
            
            filter_data.replace(np.nan,'',inplace=True)
            request = service.spreadsheets().values().update(spreadsheetId=SPREADSHEET_ID_1, range=RANGE_NAME_1, valueInputOption='USER_ENTERED', body={'values':filter_data.T.reset_index().T.values.tolist()})
            request.execute()
            
        elif file == "AVP_Report_Weekly.xlsx":

            # call the method
            filter_data = ingest_file_method(data)

            # The ID and range of a Google spreadsheet.
            SPREADSHEET_ID = 'ENTER GOOGLE SHEET ID'
            RANGE_NAME = 'Sheet1!A1'

            # append data to Google Sheets
            request = service.spreadsheets().batchUpdate(spreadsheetId=SPREADSHEET_ID, body={'requests': [{'updateCells': {'range': {'sheetId': '0'}, 'fields': 'userEnteredValue'}}]})
            request.execute()
            
            filter_data.replace(np.nan,'',inplace=True)
            filter_data = filter_data.applymap(str)
            request = service.spreadsheets().values().update(spreadsheetId=SPREADSHEET_ID, range=RANGE_NAME, valueInputOption='USER_ENTERED', body={'values':filter_data.T.reset_index().T.values.tolist()})
            request.execute()
        
        elif file == "NMS_Call_log.xlsx":

            # call the method
            filter_data = ingest_file_method(data)

            # The ID and range of a Google spreadsheet.
            SPREADSHEET_ID = 'ENTER GOOGLE SHEET ID'
            RANGE_NAME = 'Sheet1!A1'

            # append data to Google Sheets
            request = service.spreadsheets().batchUpdate(spreadsheetId=SPREADSHEET_ID, body={'requests': [{'updateCells': {'range': {'sheetId': '0'}, 'fields': 'userEnteredValue'}}]})
            request.execute()
            
            filter_data.replace(np.nan,'',inplace=True)
            filter_data = filter_data.applymap(str)
            request = service.spreadsheets().values().update(spreadsheetId=SPREADSHEET_ID, range=RANGE_NAME, valueInputOption='USER_ENTERED', body={'values':filter_data.T.reset_index().T.values.tolist()})
            request.execute()
            
        else:
            print('unknown')