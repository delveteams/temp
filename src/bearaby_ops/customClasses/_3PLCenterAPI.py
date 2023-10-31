
import datetime
import pandas as pd
import requests
import base64
import csv
import json
import requests
import xml.etree.ElementTree as ET
from urllib.parse import quote_plus
 
import gspread
from oauth2client.service_account import ServiceAccountCredentials

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaFileUpload
from googleapiclient.http import MediaIoBaseDownload

from kedro.framework.hooks import hook_impl

import dotenv
import os 

class _3PLCenterAPI:
    """
    A class to interact with the 3PL Center API.

    Attributes
    ----------
    client_id : str
        The client ID for the API.
    client_secret : str
        The client secret for the API.
    token : str
        The access token for the API.

    Methods
    -------
    _get_access_token()
        Gets the access token for the API.
    _get_inventory_data()
        Gets the inventory data from the API.
    save_inventory_data_to_csv(filename)
        Saves the inventory data to a CSV file.
    """

    def __init__(self, client_id, client_secret):
        """
        Parameters
        ----------
        client_id : str
            The client ID for the API.
        client_secret : str
            The client secret for the API.
        """
        self.client_id = client_id
        self.client_secret = client_secret
        self.token = self._get_access_token()

    def _get_access_token(self):
        """
        Gets the access token for the API.

        Returns
        -------
        str
            The access token for the API.
        """
        url = 'https://secure-wms.com/AuthServer/api/Token'
        credentials = f"{self.client_id}:{self.client_secret}"
        credentials_bytes = credentials.encode("utf-8")
        encoded_credentials = base64.b64encode(credentials_bytes)
        encoded_credentials_str = encoded_credentials.decode("utf-8")
        headers = {
            'Host': 'secure-wms.com',
            'Connection': 'keep-alive',
            'Content-Type': 'application/json; charset=utf-8',
            'Accept': 'application/hal+json',
            'Authorization': f'Basic {encoded_credentials_str}',
            'Accept-Encoding': 'gzip,deflate,sdch',
            'Accept-Language': 'en-US,en;q=0.8'
        }
        data = {
            "grant_type": "client_credentials",
            "user_login_id": "1523"
        }
        response = requests.post(url, headers=headers, json=data)
        response_data = response.json()
        return response_data["access_token"]

    def _get_inventory_data(self):
        """
        Gets the inventory data from the API.

        Returns
        -------
        list
            A list of dictionaries containing the inventory data.
        """
        url = "https://secure-wms.com/inventory/stocksummaries"
        headers = {
            "Authorization": f"Bearer {self.token}",
            "Accept-Language": "en-US,en;q=0.8",
            "Host": "secure-wms.com",
            "Content-Type": "application/json; charset=utf-8",
            "Accept": "application/hal+json"
        }
        params = {
            "pgsiz": 500,  # Number of records per page
            "pgnum": 1,    # Start with page number 1
        }
        all_inventory_data = []
        while True:
            response = requests.get(url, headers=headers, params=params)
            if response.status_code == 200:
                data = response.json()
                all_inventory_data.extend(data["summaries"])
                if "next" in data["_links"]:
                    url = "https://secure-wms.com" + data["_links"]["next"]["href"]
                    params = None
                else:
                    break
            else:
                print(f"Request failed with status code: {response}")
                break
        return all_inventory_data

    def save_inventory_data_to_csv(self, filename=r'data\01_raw\InventoryReportTPLC.csv'):
        """
        Saves the inventory data to a CSV file.

        Parameters
        ----------
        filename : str, optional
            The filename to save the CSV file to, by default 
        """
        all_inventory_data = self._get_inventory_data()
        fieldnames = ['SKU', 'TOTAL_RECEIVED', 'ALLOCATED', 'AVAILABLE', 'onHold', 'onHand', 'facilityId']
        filename = os.path.join(os.getcwd(), filename)
        with open(filename, 'w', newline='') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            for api_output in all_inventory_data:
                sku = api_output['itemIdentifier']['sku']
                totalReceived = api_output['totalReceived']
                allocated = api_output['allocated']
                available = api_output['available']
                onHold = api_output['onHold']
                onHand = api_output['onHand']
                facilityId = api_output['facilityId']
                writer.writerow({
                    'SKU': sku,
                    'TOTAL_RECEIVED': totalReceived,
                    'ALLOCATED': allocated,
                    'AVAILABLE': available,
                    'onHold': onHold,
                    'onHand': onHand,
                    'facilityId': facilityId
                })

