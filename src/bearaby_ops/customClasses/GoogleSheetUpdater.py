import pandas as pd
import os
import xml.etree.ElementTree as ET


import gspread
from oauth2client.service_account import ServiceAccountCredentials

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaFileUpload
from googleapiclient.http import MediaIoBaseDownload
 
class GoogleSheetUpdater:
    def __init__(self, file_to_edit, token_file, credentials_file):
        self.file_to_edit = file_to_edit
        self.SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
        self.filePath, self.filename, self.folderLocation = file_to_edit

        # Authorize the API
        scope = [
            'https://www.googleapis.com/auth/drive',
            'https://www.googleapis.com/auth/drive.file'
        ]
        file_name = r'src\bearaby_ops\service_account.json'
        self.creds = ServiceAccountCredentials.from_json_keyfile_name(file_name, scope)
        self.client = gspread.authorize(self.creds)

        # Fetch the sheet
        self.sheet = self.client.open('new').sheet1
        self.python_sheet = self.sheet.get_all_records()

        if os.path.exists(token_file):
            self.creds = Credentials.from_authorized_user_file(token_file, self.SCOPES)
         
        else:
            flow = InstalledAppFlow.from_client_secrets_file(credentials_file, self.SCOPES)
            self.creds = flow.run_local_server(port=0)
            
            with open(token_file, 'w') as token:
                token.write(self.creds.to_json())

        # Build the service for Google Sheets API
        self.service_sheets = build('sheets', 'v4', credentials=self.creds)

    def update_sheet(self, sheet_name, update_range):
        try:
            spreadsheet_id = self.folderLocation

            df = pd.read_excel(self.filePath)

            # data is the header + the values from df
            data = [df.columns.values.tolist()] + df.values.tolist()
            value_input_option = 'RAW'
            request = self.service_sheets.spreadsheets().values().update(
                spreadsheetId=spreadsheet_id,
                valueInputOption=value_input_option,
                body={'values': data},
                range=f'{sheet_name}'
            )

            response = request.execute()
            print('Cells updated successfully:', response.get('updatedCells'))
            

        except HttpError as error:
            print(f'An error occurred: {error}')
            
    def download_sheet(self, sheet_name, sheet_id, file_name):
        try:
            spreadsheet_id = sheet_id
            request = self.service_sheets.spreadsheets().values().get(
                spreadsheetId=spreadsheet_id,
                range=f'{sheet_name}'
            )

            response = request.execute()
            print('Downloaded Time Series Data')
            # the top row is the columns names and the rest is the data
            df = pd.DataFrame(response.get('values'), index=None)
            
            # set the first row as the columns names
            df.columns = df.iloc[0]
            # drop the first row
            df = df.iloc[1:]
            
            df.to_csv(file_name, index=False)
             
        except HttpError as error:
            print(f'An error occurred: {error}')
         