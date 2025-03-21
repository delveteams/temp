import datetime

import matplotlib.pyplot as plt
import pandas as pd
import plotly.express as px


def preprocess_bergenInventory_products(inventory: pd.DataFrame) -> pd.DataFrame:
    
    """ Preprocess Bergen County Inventory Data 
    
    Args:
        inventory: Bergen County Inventory Data
        Columns: WAREHOUSENAME	STYLE	COLOR	SIZE	
        DESCRIPTION	UPCCODE	ACTUALQTY	PENDINGPICKING	
        AVAILABLE	SKU	ACCOUNTNAME	SEASON
    Returns:
        
    """
    
    inventory.dropna(subset=["UPCCODE"], inplace=True)
    warehouse_mapping = {"Bergen Logistics NJ299": "BLNJ"}
    inventory["WAREHOUSEID"] = inventory["WAREHOUSENAME"].map(warehouse_mapping)    
    # if the sku is null then fill it
    inventory["SKU"] = inventory["SKU"].fillna("Missing_SKU_")  
    # inventory["SKU"] = inventory["SKU"].apply(lambda x: x.split("-")[0] if "-" in x else x)
        
    return_inventory = inventory[[ "UPCCODE", "WAREHOUSEID","SKU", "ACTUALQTY", "AVAILABLE", "PENDINGPICKING"]]
    
    return return_inventory


def preprocess_TL(thinkLogistics: pd.DataFrame, sku_preprocessed) -> pd.DataFrame:
    
    """ Preprocess Think logistics Inventory Data 
    
    Args:
        thinkLogistics: Think logistics Inventory Data
    Returns:
        
    """
    # keep the following columns: StockCode,InvtCollection,Ordered,OnHand,AllocatedQty,ReservedQty,AvailQty,Insp Available,BackOrder,InTransit,InInspection,Insp Allocated,Insp Reserved,InRMA,OnPO,Status,ProdClass,InvtClass
    thinkLogistics = thinkLogistics[["StockCode","OnHandQty","AllocatedQty","Available","InspectionQty","Status"]]
    '''
        rename the columns to "UPCCODE", "WAREHOUSENAME","SKU", "ACTUALQTY", "AVAILABLE", "PENDINGPICKING"
        StockCode -> SKU
        Warehouse -> WAREHOUSEID
        AvailQty -> AVAILABLE
        OnHand -> ACTUALQTY
        AllocatedQty -> PENDINGPICKING
    '''
    # add a column UPCCODE and fill it using the sku_preprocessed dataframe on the SKU column
    # rename upc to UPCCODE
    # Create a copy of the DataFrame before making changes
    thinkLogistics_copy = thinkLogistics.copy()

    # Rename columns in the copied DataFrame
    thinkLogistics_copy.rename(columns={"StockCode": "SKU", "Available": "AVAILABLE", "OnHandQty": "ACTUALQTY", "AllocatedQty": "PENDINGPICKING"}, inplace=True)

    # Now, you can use thinkLogistics_copy for further processing without raising the warning.

    # thinkLogistics_copy = thinkLogistics_copy[thinkLogistics_copy["InvtClass"] == 1]
    # thinkLogistics_copy.drop(columns=["InvtClass"], inplace=True)
    
    # set all the WAREHOUSEID to THINKLOGISTICS
    thinkLogistics_copy["WAREHOUSEID"] = "THINKLOGISTICS"
    
    thinkLogistics_copy["SKU"] = thinkLogistics_copy["SKU"].apply(lambda x: x[2:] if len(x) > 2 else 0)
    thinkLogistics_copy["SKU"] = thinkLogistics_copy["SKU"].apply(lambda x: x.split("-")[0] + x.split("-")[1] if len(x.split("-")) > 2 else x.split("-")[0] )
    thinkLogistics_copy = pd.merge(thinkLogistics_copy, sku_preprocessed[['SKU', 'UPC']], on='SKU', how='left')
    thinkLogistics_copy.rename(columns={"UPC": "UPCCODE"}, inplace=True)
        
    return thinkLogistics_copy


def preprocess_tplCenter(tplCenter: pd.DataFrame, sku_preprocessed) -> pd.DataFrame:
    """Preprocess Think logistics Inventory Data

    Args:
        tplCenter (pd.DataFrame): dataframe
        Columns: Hold Status	Transaction ID	ReceiveItem ID	Receipt Date	Customer	
        SKU	Available Primary	Qualifier	Expiration Date	Lot Number	Serial Number	Cost	
        Location	Primary UOM	Warehouse	 UPC

    Returns:
        pd.DataFrame: dataframe
    """
    tplCenter.columns = tplCenter.columns.str.strip()
     
    merge = pd.merge(tplCenter, sku_preprocessed[['SKU', 'UPC']], on='SKU', how='left')
    # rename upc to UPCCODE
    merge.rename(columns={"UPC": "UPCCODE"}, inplace=True)
 
    
    # rename onhand to ACTUALQTY
    merge.rename(columns={"onHand": "ACTUALQTY"}, inplace=True)
    merge["PENDINGPICKING"] = merge["ACTUALQTY"] - merge["AVAILABLE"]
    
    merge["WAREHOUSEID"] = merge["facilityId"].apply(lambda x: "3PLC LA" if x == 659 else "3PLC NJ")
    # merge["WAREHOUSEID"] = merge["facilityId"].apply(lambda x: "3PLC LA" if x == 659 else "3PLC NJ")
    
    
    return merge[[ "WAREHOUSEID", "SKU", "AVAILABLE", "UPCCODE", "ACTUALQTY", "PENDINGPICKING"]]


def preprocess_sku(SKUs: pd.DataFrame) -> pd.DataFrame:
    """Preprocess SKU table
    
    Args:
        SKUs (pd.DataFrame): dataframe
        
    Returns:
        pd.DataFrame: dataframe
        
    """
    SKUs.rename(columns={"SKU_standard": "SKU"}, inplace=True)
    SKUs.dropna(subset=["SKU"], inplace=True)
    
    SKUs.dropna(subset=["Collection"], inplace=True)
    
    # if there is "&" in SKU split the SKU and duplicate the row with SKU with second element of the split
    # SKUs["SKU"] = SKUs["SKU"].apply(lambda x: x.split("-")[0] if not "&" in x else x.split("&")[1])
    
    
    SKUs.dropna(subset=["UPC"], inplace=True)
    SKUs["UPC"] = SKUs["UPC"].astype(str)
    return SKUs

def preprocess_quota(quota: pd.DataFrame) -> pd.DataFrame:
    """
    Normalizing the quota table; process the quota table to only have SKU and Quota columns

    Args:
        quota (pd.DataFrame): df with columns: SKU, Quota, Description, Collection

    Returns:
        pd.DataFrame: df with columns: SKU, Quota
        
    """
    # if there are two "-" in SKU, then split by "-" and take the first and second element merge them together
    # else split by "-" and take the first element
    quota["SKU"] = quota["SKU"].apply(lambda x: x.split("-")[0] + x.split("-")[1] if len(x.split("-")) > 2 else x.split("-")[0] )
    # fill the null values in Quota Amount with 0
    quota["Quota Amount"] = quota["Quota Amount"].fillna(0)
    return quota[["SKU", "Quota", "Quota Amount"]]

def merge_tables(
        bergenInventoryNJ_preprocessed: pd.DataFrame,
        tplCenter_preprocessed: pd.DataFrame
) -> pd.DataFrame:
    """ Create Model Input Table
    
    Args:
        bergenInventoryNJ_preprocessed: Bergen County Inventory Data
        tplCenter_preprocessed: Bergen County Inventory Data
        thinkLogistics:
    Returns:
        
    """
    
    # set upccodeof tplCenter_preprocessed to str
    tplCenter_preprocessed["UPCCODE"] = tplCenter_preprocessed["UPCCODE"].astype(str)
    
    merged = pd.concat([
        bergenInventoryNJ_preprocessed,
        tplCenter_preprocessed,
    ])
   
    # change upc to str
    merged["UPCCODE"] = merged["UPCCODE"].astype(str)
    # set the upccode to not have decimal points in merged
    merged["UPCCODE"] = merged["UPCCODE"].apply(lambda x: x.split(".")[0])
    
    # Filter UPC values that are integers and have at least 12 digits
    merged_filtered = merged[merged["UPCCODE"].astype(str).str.match(r'^\d{12,}$')]

    # if there are repeated SKU drop one of them
    # merged_filtered.drop_duplicates(subset=["SKU"], inplace=True)

    return merged_filtered


def metrics(merged_data_: pd.DataFrame, retailQuot: pd.DataFrame, all_SKU_shopify) -> pd.DataFrame:
    """
    get the quota for retailers based on the quota table and see if we have enough inventory for them, then find the inventory
    that has the highest number of particular SKU

    Args:
        mergedTable (pd.DataFrame): Columns UPCCODE,WAREHOUSEID,SKU,ACTUALQTY,AVAILABLE,PENDINGPICKING
        retailQuot (pd.DataFrame): Columns SKU,Quota

    Returns:
        pd.DataFrame: 
    """
    # if sku is "Missing_SKU_" then replace it with the sku from all_SKU_shopify dataframe based on the upc code
    merged_data_["SKU"] = merged_data_.apply(lambda x: all_SKU_shopify[all_SKU_shopify["UPCCODE"] == x["UPCCODE"]]["SKU"].values[0] if x["SKU"] == "Missing_SKU_" else x["SKU"], axis=1)
   
    # # Merge the summary table with the retailQuot DataFrame to get the quota for each SKU
    pivot = merged_data_.pivot_table(index=['SKU', 'UPCCODE'], columns=['WAREHOUSEID'], values='AVAILABLE', aggfunc='sum', fill_value=0)
    pivot.reset_index(inplace=True)
    merged_data = pd.merge(pivot, retailQuot, on='SKU', how='left')
    merged_data["Quota"] = merged_data["Quota"].fillna(0)
    merged_data["Quota Amount"] = merged_data["Quota Amount"].fillna(0)

    
    # set the warehouse with the highest inventory as the warehouse to fulfill the order, incase of tie, choose in the following order:  BLNJ, BLPA, TPLC, SMC
    merged_data["Warehouse"] = merged_data[["BLNJ", "3PLC NJ","3PLC LA"]].idxmax(axis=1)
    
    # add 4 columns to the merged_data dataframe: "Updated_BLNJ", "Updated_BLPA", "Updated_TPLC", "Updated_SMC"
    # based on the Warehouse column, if the warehouse is the same as the column name, then subtract the Quota Amount from the inventory 
    # else keep the inventory the same
    merged_data["Updated_BLNJ"] = merged_data.apply(lambda x: x["BLNJ"] - x["Quota Amount"] if x["Warehouse"] == "BLNJ" else x["BLNJ"], axis=1)
    merged_data["Updated_3PLC NJ"] = merged_data.apply(lambda x: x["3PLC NJ"] - x["Quota Amount"] if x["Warehouse"] == "3PLC NJ" else x["3PLC NJ"], axis=1)
    merged_data["Updated_3PLC LA"] = merged_data.apply(lambda x: x["3PLC LA"] - x["Quota Amount"] if x["Warehouse"] == "3PLC LA" else x["3PLC LA"], axis=1)
    
      
    merged_data = pd.merge(merged_data, all_SKU_shopify, on="SKU", how="outer")
    merged_data = merged_data.drop_duplicates()

    merged_data['UPC'] = merged_data[['UPCCODE_x', 'UPCCODE_y']].apply(lambda row: row.dropna().iloc[0], axis=1)
    merged_data.drop(['UPCCODE_x', 'UPCCODE_y'], axis=1, inplace=True)
    
    merged_data["Total Inventory"] = merged_data["BLNJ"] + merged_data["3PLC NJ"] + merged_data["3PLC LA"]
    merged_data["Total Available"] = merged_data["Updated_3PLC NJ"] + merged_data["Updated_BLNJ"]  + merged_data["Updated_3PLC LA"]
    

    merged_data.fillna(0, inplace=True)
    
    return merged_data


def experiment_metrics(metrics_table: pd.DataFrame): 
    # return a dict with SKU as key and available as values 
    return metrics_table[["Product Description", "Total Inventory"]].set_index("Product Description").to_dict()["Total Inventory"]


def add_product_name_SKU(merged_data: pd.DataFrame, skus: pd.DataFrame, retailPrice: pd.DataFrame) -> pd.DataFrame:
    """Add the product name to the merged_data dataframe
    Args:
        merged_data : merged_data
        skus: skus dataframe
        
    Returns:
        merged_data: merged_data with product name added
        
    """
    skus = skus[["SKU", "Product Description", "Collection", "Color", "Size (Inch)", "Weight (lbs)"]]
    merged_data = pd.merge(merged_data, skus, on="SKU", how="left")
    merged_data = pd.merge(merged_data, retailPrice, on="SKU", how="left")
    merged_data.fillna(0, inplace=True)
    return merged_data[["SKU", "UPC","Color", "Size (Inch)", "Weight (lbs)", "Product Description","Collection", "BLNJ", "3PLC LA","3PLC NJ", "Quota", "Total Inventory", "Quota Amount", "Warehouse", "Updated_BLNJ", "Updated_3PLC LA","Updated_3PLC NJ",  "Total Available", "Cost"]]

def total_inventory(final_SKU_table: pd.DataFrame) -> pd.DataFrame:
    # deep copy the final_SKU_table
    new_table = final_SKU_table.copy(deep=True)
    new_table["Date"] = datetime.datetime.now().strftime("%m/%d/%Y")
    return new_table[["SKU", "Total Available", "Collection", "Date", "Color", "Weight (lbs)"]] 

# -------------------------  Plotting Functions ------------------------- #


def barplot_of_available_inventory_per_warehouse(merged_data: pd.DataFrame) ->plt:
    """Create a bar plot of the experiment metrics
    
    Args:
        experiment_metrics : merged_data
        UPCCODE,WAREHOUSEID,SKU,ACTUALQTY,AVAILABLE,PENDINGPICKING

        
    Returns:
        plt: bar plot with warehouse as x-axis and total available quantity as y-axis
        
    """
    
    # get the total available quantity for each warehouse
    warehouse_available = merged_data.groupby("WAREHOUSEID")["AVAILABLE"].sum().reset_index()
    warehouse_available.rename(columns={"AVAILABLE": "Total Available"}, inplace=True)
    # warehouse_available["Total Available"] = warehouse_available["Total Available"].apply(lambda x: math.ceil(x))
    warehouse_available.sort_values(by="Total Available", ascending=False, inplace=True)
    warehouse_available["WAREHOUSEID"] = warehouse_available["WAREHOUSEID"].apply(lambda x: x.upper())
    
    # plot the bar plot
    plt = px.bar(warehouse_available, x="WAREHOUSEID", y="Total Available", color="WAREHOUSEID", title="Total Available Quantity for Each Warehouse")
    return plt
   
   
def quota_barplot(merged_data: pd.DataFrame, skus) ->plt:
    """Create a bar plot of the total quota for each SKU
    Args:
        experiment_metrics : merged_data
        SKU,BLNJ,SMC,TPLC,Quota,Total Inventory,Quota Amount,Warehouse

        
    Returns:
        plt: bar plot with warehouse as x-axis and total available quantity as y-axis
        
    """
    
    # get the total quota for each SKU
    warehouse = merged_data.groupby("SKU")["Quota Amount"].sum().reset_index()
    warehouse.rename(columns={"Quota Amount": "Total Quota"}, inplace=True)
    warehouse.sort_values(by="Total Quota", ascending=False, inplace=True)
    
    warehouse_with_quota = warehouse[warehouse["Total Quota"] > 0]
    
    # plot the bar plot with highest 10 quota
    plt = px.bar(warehouse_with_quota, x="SKU", y="Total Quota",   title="Quota for each SKU")
    
    
    return plt


def SKU_barplot(final_SKU_table: pd.DataFrame) ->plt:
    """Create a bar plot of the total quota for each SKU
    Args:
        experiment_metrics : merged_data
        SKU,BLNJ,SMC,TPLC,Quota,Total Inventory,Quota Amount,Warehouse

        
    Returns:
        plt: bar plot with warehouse as x-axis and total available quantity as y-axis
        
    """
    
    # get the total quota for each SKU and the Product Description
    warehouse = final_SKU_table.groupby("SKU").agg({"Total Inventory": "sum", "Product Description": "first"}).reset_index()
    # warehouse.rename(columns={"Total Inventory": "Total Inventory"}, inplace=True)
    # warehouse.sort_values(by="Total Inventory", ascending=False, inplace=True)
    
    
    
    # plot the bar plot but get the Product Name instead of SKU, product name is on the skus dataframe 
    # warehouse = pd.merge(warehouse, skus, on="SKU", how="left")
    # plot the bar plot with highest 10 quota
    plt = px.bar(warehouse, x="SKU", y="Total Inventory", color="Product Description", title="SKUs with highest inventory (10)")
    
    
    return plt


def SKU_PA_barplot(final_SKU_table: pd.DataFrame) ->plt:
    """Create a bar plot of the total quota for each SKU
    Args:
        experiment_metrics : merged_data
        SKU,BLNJ,SMC,TPLC,Quota,Total Inventory,Quota Amount,Warehouse

        
    Returns:
        plt: bar plot with warehouse as x-axis and total available quantity as y-axis
        
    """
    
     # get the total quota for each SKU and the Product Description
    warehouse = final_SKU_table.groupby("SKU").agg({"Updated_BLPA": "sum", "Product Description": "first"}).reset_index()
    # warehouse.rename(columns={"Total Inventory": "Total Inventory"}, inplace=True)
    # warehouse.sort_values(by="Updated_BLPA", ascending=False, inplace=True)
    
    
    
    # plot the bar plot but get the Product Name instead of SKU, product name is on the skus dataframe 
    # warehouse = pd.merge(warehouse, skus, on="SKU", how="left")
    # plot the bar plot with highest 10 quota
    plt = px.bar(warehouse,   y="Updated_BLPA", x="Product Description", color="Product Description", title="SKUs in the PA warehouse")
    
    
    return plt


def stacked_barplot(merged_data, skus):
    """_summary_

    Args:
        merged_data (_Collection_): columns: UPCCODE,WAREHOUSEID,SKU,ACTUALQTY,AVAILABLE,PENDINGPICKING

    """
    # plt = px.bar(merged_data, x="SKU", y="AVAILABLE", color="WAREHOUSEID", barmode="stack")

    # merge the data with the skus dataframe to get the product description
    merged_data = pd.merge(merged_data, skus, on="SKU", how="left")
    # merged_data.dropna(subset=["Collection_y"], inplace=True)
    # merged_data = merged_data[merged_data["Collection_y"] != 0]
 
    plt = px.bar(merged_data, x=merged_data[ "Product Description" ], y="AVAILABLE", color="WAREHOUSEID", barmode="stack", title="Inventory in each warehouse")
    # q: change the x-axis to product description and Collection_y and y to AVAILABLE and color to WAREHOUSEID
    return plt

