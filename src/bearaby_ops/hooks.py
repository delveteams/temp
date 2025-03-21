import datetime
import json
import logging
import os

import dotenv
import pandas as pd
import requests
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaFileUpload
from kedro.framework.hooks import hook_impl

dotenv.load_dotenv()
import sys

project_url = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append("src/bearaby_ops")

from .customClasses.BergenAPI import BergenAPI
from .customClasses._3PLCenterAPI import _3PLCenterAPI
from .customClasses.GoogleSheetUpdater import GoogleSheetUpdater

# If modifying these scopes, delete the file token.json.
SCOPES = ['https://www.googleapis.com/auth/drive']
    
class APIAccessHooks:
    
    @staticmethod
    @hook_impl
    def after_catalog_created(  ) -> None:
        logging.info("Downloading inventory data from the Bergen API...")
        # Loop through credentials and fetch/write data

        credentials_list = [
            (
                os.getenv("USER_NJ_EMAIL"),
                os.getenv("USER_NJ_USERNAME"),
                os.getenv("USER_NJ_PASSWORD"),
                "/BergenInventoryNJ.csv"
            ),
        ]
        for creds in credentials_list:
            web_address, username, password, file_name = creds
            rex_api = BergenAPI(web_address, username, password)
            authentication_token = rex_api.get_authentication_token()

            if not authentication_token:
                print("Authentication token is missing. Please authenticate first.")
            if authentication_token:
                logging.info("Authentication token:", authentication_token)
                inventory = rex_api.get_inventory()
                if inventory:
                    csv_filename = project_url + r"/data/01_raw" + file_name
                    
                    rex_api.write_inventory_to_csv(inventory, csv_filename)
                    
        
        logging.info("Downloading inventory data from the 3PL Center API...")            
    
        client_id = os.getenv("TPL_CLIENT_ID")
        client_secret = os.getenv("TPL_CLIENT_SECRET")

        api = _3PLCenterAPI(client_id, client_secret)
        api.save_inventory_data_to_csv()

    @staticmethod
    @hook_impl
    def after_pipeline_run() -> None:
        file_info = [(
            project_url + r"/data/01_raw/BergenInventoryNJ.csv",
            "BergenInventoryNJ",
            os.getenv("SHEETS_ID_BERGEN_NJ")
        ), (
            project_url + r"/data/03_primary/final_SKU_table.xlsx",
            "",
            os.getenv("SHEETS_ID_DAILY_INVENTORY")
        ), (
            project_url + r"/data/01_raw/InventoryReportTPLC.csv",
            "3PLCenter",
            os.getenv("SHEETS_ID_TPLC")
        )]
        logging.info("Pipeline has run successfully! Uploading the file to the drive...")
        
        for info in file_info:
            filename_path, filename, folder_location = info
            creds = None
            # The file token.json stores the user's access and refresh tokens, and is
            # created automatically when the authorization flow completes for the first
            # time.
            if os.path.exists('token.json'):
                creds = Credentials.from_authorized_user_file('token.json', SCOPES)
            # If there are no (valid) credentials available, let the user log in.
            if not creds or not creds.valid:
                if creds and creds.expired and creds.refresh_token:
                    creds.refresh(Request())
                else:
                    flow = InstalledAppFlow.from_client_secrets_file(project_url + r'/src/bearaby_ops/credentials.json', SCOPES)
                    creds = flow.run_local_server(port=0)
                # Save the credentials for the next run
                with open('token.json', 'w') as token:
                    token.write(creds.to_json())
            try:
                service = build('drive', 'v3', credentials=creds)

               
                # get the current date in the format of 081523
                date = datetime.datetime.now().strftime("%m%d%y")
                # get formated date to 08-18-2023
            
                formated_date = datetime.datetime.now().strftime("%m-%d-%Y")
                # Upload a CSV file
            
                if filename == "":
                    # filename as mm-dd-yy.xlsx
                    csv_file_metadata = {
                        'name' : formated_date + ".xlsx",
                        'parents': [folder_location]
                    }
                    media = MediaFileUpload(filename_path, mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
                    
                elif filename == "display":
                    csv_file_metadata = {
                        'name' : "display",
                        'parents': [folder_location]
                    }
                    media = MediaFileUpload(filename_path, mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
                else:
                    csv_file_metadata = {
                        'name': f'{filename}{date}.csv',
                        'parents': [folder_location]
                    }
                    
                    media = MediaFileUpload(filename_path, mimetype='text/csv')
    
                # Check if the file with the same name already exists in the folder
                existing_file = None
                results = service.files().list(
                   ).execute()
                
                
                if csv_file_metadata['name'] in results:
                    existing_files = results['files']
                    if existing_files:
                        existing_file = existing_files[0]['id']

                # If an existing file with the same name is found, replace it
                if existing_file:
                    uploaded_file = service.files().update(
                        fileId=existing_file, media_body=media).execute()
                else:
                    uploaded_file = service.files().create(
                        body=csv_file_metadata, media_body=media, fields='id', supportsAllDrives=True).execute()

                logging.info('CSV file uploaded. File ID: %s' % uploaded_file.get('id'))

            except HttpError as error:
                logging.info(f'An error occurred: {error}')
                
            
        file_to_edit = (
            project_url + r"/data/03_primary/final_SKU_table.xlsx",
            "display",
            os.getenv("SHEETS_LOOKER_DISPLAY")
        )
            

        token_file = 'token_.json'
        credentials_file = project_url+r'/src/bearaby_ops/credentials.json'

        updater = GoogleSheetUpdater(file_to_edit, token_file, credentials_file)
        updater.update_sheet(sheet_name='Sheet1', update_range='A1')
        
        updater.download_sheet('total_inventory',  os.getenv("SHEETS_TIME_SERIES"), project_url+r'/data/02_intermediate/test.csv')
        
        # open the file and read the data as dataframe
        df = pd.read_csv(project_url+r'/data/02_intermediate/test.csv')
        df2 = pd.read_csv(project_url+r'/data/03_primary/total_inventory.csv')
        
        # concat the dataframes
        df3 = pd.concat([df, df2])
        
        # check the number of unique elements Date column and if len > 30 remove the oldest date
        while len(df3['Date'].unique()) > 31:
            # oldest date
            oldest_date = df3['Date'].unique()[0]
            # remove the rows with the oldest date
            df3 = df3[df3['Date'] != oldest_date]
            
        # get the sum of "Total Available" for last 2 days
        total = df3.copy()
        total["Date"] = pd.to_datetime(total["Date"]).dt.date
        total = total.groupby(['Date']).sum(numeric_only=True)
        # sort by the date
        total = total.sort_values(by=['Date'])
        total = total.reset_index()
        total = total.tail(2)
        # get difference between the 2 days
        difference =  total["Total Available"].iloc[1] - total["Total Available"].iloc[0] 
        logging.info(total)
        logging.info(total["Total Available"].iloc[1], total["Total Available"].iloc[0] , difference)
        
        if abs(difference) >= 500:
             
            # send Slack message
            slack_text = f"Inventory diff is {difference} from yesterday. Date: {datetime.datetime.now().strftime('%m/%d/%Y')}."
            slack_message = {"text": slack_text}
            slack_message = json.dumps(slack_message)
            slack_message = slack_message.encode('utf-8')
            slack_url = os.getenv("SLACK_URL")
            slack_url = slack_url.encode('utf-8')
            
            slack_headers = {'Content-type': 'application/json'}
            
            
            slack_response = requests.post(slack_url, data=slack_message, headers=slack_headers)
            logging.info(slack_response)
            
            
         
        # save it as excel 
        df3.to_excel(project_url+r'/data/03_primary/total_inventory.xlsx', index=False)
       
        file_to_edit_ = (
            project_url+r'/data/03_primary/total_inventory.xlsx',
            "new",
            os.getenv("SHEETS_TIME_SERIES")
        )
        update_timeseries = GoogleSheetUpdater(file_to_edit_, token_file, credentials_file)
        update_timeseries.update_sheet(sheet_name='total_inventory', update_range='A1')
