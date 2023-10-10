import datetime
import pandas as pd
import requests
import base64
import csv
import json
import requests
import xml.etree.ElementTree as ET
from urllib.parse import quote_plus
 
class ThinkLogisticsAPI:
    def __init__(self, login, password):
        self.base_url = "https://api.thinklogistics.com/"
        self.auth_url = "api/v1/auth/signin"
        self.login = login
        self.password = password
        self.headers = {
            'Content-Type': 'application/json',
            'Accept': 'application/json',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        }
        self.token = None

    def authenticate(self):
        # Authenticate with the Think Logistics API
        url = self.base_url + self.auth_url
        body = {
            "Login": self.login,
            "Password": self.password,
            "ClientType": "W"
        }

        try:
            response = requests.post(url, json=body, headers=self.headers)
            response.raise_for_status()
            response_dict = json.loads(response.text)
            self.token = response_dict['Token']
            return True
        except requests.exceptions.RequestException as e:
            print(f"Authentication failed: {e}")
            return False

    def retrieve_inventory(self):
        print("Downloading inventory data from Think Logistics...")
        # Retrieve inventory data
        if not self.token:
            if not self.authenticate():
                return None

        url = f'{self.base_url}api/v1/partner/inventory/custId/BEARABY/whcode/T3'
        headers = {
            'Authorization': 'Bearer ' + self.token,
            'Content-Type': 'application/json',
        }

        params = {
            "PageIndex": 1,
            "PageSize": 100,    
            "StockCode": None,
            "AltStockCode": None,
            "StockDesc": None,
            "AltStoctDesc": None,
            "BarCode": None,
            "MiscInfo1": None,
            "MiscInfo2": None,
            "MiscInfo3": None,
            "MiscInfo4": None,
            "WithInventory": True,
            "Finish": True,
            "Raw": True,
            "Letter": True,
            "Kit": True,
            "Digital": True,
            "SortExpression": None
        }

        all_inventory = []

        while True:
            try:
                response = requests.post(url, headers=headers, json=params)
                response.raise_for_status()
                data = response.json()
                inventory_items = data

                if not inventory_items:
                    break

                all_inventory.extend(inventory_items)
                params["PageIndex"] += 1
            except requests.exceptions.RequestException as e:
                print(f"Failed to retrieve data: {e}")
                return None

        # print(f"Total inventory items retrieved: {len(all_inventory)}")
        return all_inventory

    def save_inventory_to_excel(self, inventory_data, file_name):
        if not inventory_data:
            return

        df = pd.DataFrame(inventory_data)
        df.to_csv(file_name, index=False)
        print(f"Inventory data saved to {file_name}")

