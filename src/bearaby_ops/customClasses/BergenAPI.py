import datetime
import pandas as pd
import requests
import csv
import requests
import xml.etree.ElementTree as ET
from urllib.parse import quote_plus
 
class BergenAPI:
    """Class to interact with the Rex API and fetch inventory data."""
    
    def __init__(self, web_address, username, password):
        """Initialize the BergenAPI instance with credentials and base URL."""
        self.base_url = "https://sync.rex11.com/ws/v3prod/publicapiws.asmx"
        self.headers = {'Host': 'sync.rex11.com'}
        self.web_address = web_address
        self.username = username
        self.password = password
        self.authentication_token = None

    def get_authentication_token(self):
        """Get authentication token from the API."""
        url = f"{self.base_url}/AuthenticationTokenGet"
        params = {
            "WebAddress": quote_plus(self.web_address),
            "UserName": quote_plus(self.username),
            "Password": quote_plus(self.password)
        }
        
        try:
            response = requests.get(url, params=params)
            response.raise_for_status()
            root = ET.fromstring(response.text)
            self.authentication_token = root.text
            return self.authentication_token
        except requests.exceptions.RequestException as e:
            print("An error occurred during authentication:", e)
            return None

    def get_inventory(self):
        """Fetch inventory data from the API."""
        if not self.authentication_token:
            print("Authentication token is missing. Please authenticate first.")
            return None
        
        url = f"{self.base_url}/GetInventory"
        params = {
            "AuthenticationString": self.authentication_token
        }
        
        try:
            response = requests.get(url, params=params, headers=self.headers)
            response.raise_for_status()
            return response.content
        except requests.exceptions.RequestException as e:
            print("An error occurred while fetching inventory:", e)
            return None
        
    def write_inventory_to_csv(self, response_content, csv_filename):
        """Write inventory data to a CSV file."""
        root = ET.fromstring(response_content)

        # Define the namespace
        namespace = {'ns': 'http://rex11.com/webmethods/'}

        # Find all item elements within the namespace
        items = root.findall('.//ns:item', namespace)

        # Open a CSV file for writing
        with open(csv_filename, mode='w', newline='') as csv_file:
            csv_writer = csv.writer(csv_file)

            # WAREHOUSENAME	STYLE	COLOR	SIZE	DESCRIPTION	UPCCODE	ACTUALQTY	PENDINGPICKING	AVAILABLE	SKU	ACCOUNTNAME	SEASON
            # Write CSV header
            csv_writer.writerow([
                'WAREHOUSENAME', 'STYLE', 'COLOR', 'SIZE', 'DESCRIPTION', 'UPCCODE', 'ACTUALQTY', 'PENDINGPICKING', 'AVAILABLE', 'SKU' 
            ])
            
            # Iterate through item elements and write data to CSV
            for item in items:
                warehouse = item.find('ns:Warehouse', namespace).text
                style = item.find('ns:Style', namespace).text
                color = item.find('ns:Color', namespace).text
                size = item.find('ns:Size', namespace).text
                description = item.find('ns:Description', namespace).text
                
                sku = item.find('ns:Sku', namespace).text
                upc = item.find('ns:Upc', namespace).text
                actual_quantity = item.find('ns:ActualQuantity', namespace).text
                pending_quantity = item.find('ns:PendingQuantity', namespace).text
                available = int(actual_quantity) - int(pending_quantity)
               

                if (actual_quantity == '0' and pending_quantity == '0'):
                    continue

                csv_writer.writerow([
                    warehouse, style, color, size, description, upc, actual_quantity, pending_quantity, available, sku 
                ])
              

        print(f'CSV data has been written to {csv_filename}')
        
