"""
Generate Warehouse CSV Exports and Upload to ADLS
Creates 3 different warehouse formats with late-arriving data simulation
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
from io import StringIO
from azure.storage.filedatalake import DataLakeServiceClient
from dotenv import load_dotenv   # pip install python-dotenv
load_dotenv("../.env.development.local")
import os 

# -------------------------
# CONFIGURATION
# -------------------------

# ADLS Configuration
ADLS_ACCOUNT_NAME = os.getenv("ADLS_ACCOUNT_NAME")
ADLS_ACCOUNT_KEY = os.getenv("ADLS_ACCOUNT_KEY")
ADLS_FILESYSTEM = "shopfast-raw-data"

# ADLS paths for warehouse data
ADLS_PATHS = {
    'master_products': 'master_data/master_products.csv',
    'warehouse_east': 'warehouses/east',
    'warehouse_west': 'warehouses/west',
    'warehouse_central': 'warehouses/central'
}

# Generator settings
NUM_DAYS = 7  # Generate last 7 days of warehouse exports

# Seed for reproducibility
random.seed(44)
np.random.seed(44)

# -------------------------
# ADLS HELPER FUNCTIONS
# -------------------------

def connect_to_adls():
    """Create ADLS service client"""
    account_url = f"https://{ADLS_ACCOUNT_NAME}.dfs.core.windows.net"
    return DataLakeServiceClient(account_url=account_url, credential=ADLS_ACCOUNT_KEY)

def load_master_products_from_adls():
    """Load master_products.csv from ADLS into a pandas DataFrame"""
    print("📂 Reading master_products.csv from ADLS...")
    service_client = connect_to_adls()
    fs_client = service_client.get_file_system_client(file_system=ADLS_FILESYSTEM)
    file_client = fs_client.get_file_client(ADLS_PATHS['master_products'])
    
    download = file_client.download_file()
    raw = download.readall()
    df = pd.read_csv(StringIO(raw.decode("utf-8")))
    
    print(f"✅ Loaded {len(df)} products from ADLS")
    return df

def upload_csv_to_adls(df, adls_path, filename, separator=','):
    """
    Upload a pandas DataFrame as CSV to ADLS
    
    Args:
        df: pandas DataFrame to upload
        adls_path: Directory path in ADLS (e.g., 'warehouses/east')
        filename: Name of the file (e.g., 'wh_east_inventory_20251030.csv')
        separator: CSV delimiter (default: ',', can be '|' for pipe-delimited)
    """
    service_client = connect_to_adls()
    fs_client = service_client.get_file_system_client(file_system=ADLS_FILESYSTEM)
    
    # Full path in ADLS
    full_path = f"{adls_path}/{filename}"
    
    # Convert DataFrame to CSV string
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False, sep=separator)
    csv_data = csv_buffer.getvalue().encode('utf-8')
    
    # Upload to ADLS
    file_client = fs_client.get_file_client(full_path)
    
    # Create or overwrite the file
    file_client.create_file()
    file_client.append_data(data=csv_data, offset=0, length=len(csv_data))
    file_client.flush_data(len(csv_data))
    
    print(f"    ✅ Uploaded: {full_path} ({len(df)} rows, {len(csv_data):,} bytes)")
    
    return full_path

# -------------------------
# DATA GENERATION FUNCTIONS
# -------------------------

def distribute_inventory_across_warehouses(df_products):
    """Distribute total inventory across 3 warehouses realistically"""
    
    inventory_distribution = []
    active_products = df_products[df_products['is_active'] == True].copy()
    
    for _, product in active_products.iterrows():
        # Total inventory based on velocity
        if product['velocity_category'] == 'fast':
            total_inventory = random.randint(400, 1000)
        elif product['velocity_category'] == 'medium':
            total_inventory = random.randint(150, 500)
        else:
            total_inventory = random.randint(50, 200)
        
        # Distribution strategy:
        # East: 35%, West: 35%, Central: 30%
        east_pct = random.uniform(0.30, 0.40)
        west_pct = random.uniform(0.30, 0.40)
        central_pct = 1.0 - east_pct - west_pct
        
        east_qty = int(total_inventory * east_pct)
        west_qty = int(total_inventory * west_pct)
        central_qty = total_inventory - east_qty - west_qty
        
        inventory_distribution.append({
            'sku': product['sku'],
            'product_name': product['product_name'],
            'category': product['category'],
            'east_qty': east_qty,
            'west_qty': west_qty,
            'central_qty': central_qty,
            'total_qty': total_inventory,
            'cost': product['cost']
        })
    
    return pd.DataFrame(inventory_distribution)

def generate_warehouse_east(df_inventory_dist, export_date):
    """Generate East warehouse CSV (standard format, comma-delimited)"""
    
    records = []
    
    for _, row in df_inventory_dist.iterrows():
        quantity_on_hand = row['east_qty']
        
        # Reserved quantity (10-20% of on-hand for fast movers)
        if quantity_on_hand > 0:
            reserved_pct = random.uniform(0.05, 0.20)
            quantity_reserved = int(quantity_on_hand * reserved_pct)
        else:
            quantity_reserved = 0
        
        quantity_available = max(0, quantity_on_hand - quantity_reserved)
        
        # Storage location (Aisle-Rack-Bin)
        aisle = f"A{random.randint(1, 25):02d}"
        rack = f"R{random.randint(1, 20):02d}"
        bin_loc = f"B{random.randint(1, 30):02d}"
        location = f"{aisle}-{rack}-{bin_loc}"
        
        # Last count date (within last 3 days)
        last_count = export_date - timedelta(days=random.randint(0, 3))
        
        # Batch number
        batch_number = f"BATCH-{export_date.year}-{random.randint(1, 999):03d}"
        
        # Expiry date (only for perishable items, null for most)
        expiry_date = ""
        if row['category'] in ['Beauty', 'Home'] and random.random() < 0.2:
            expiry_date = (export_date + timedelta(days=random.randint(180, 730))).strftime('%Y-%m-%d')
        
        # Export timestamp (2 AM local time)
        export_timestamp = export_date.replace(hour=2, minute=0, second=0)
        
        records.append({
            'SKU': row['sku'],
            'ProductDescription': row['product_name'],
            'QuantityOnHand': quantity_on_hand,
            'QuantityReserved': quantity_reserved,
            'QuantityAvailable': quantity_available,
            'Location': location,
            'LastCountDate': last_count.strftime('%Y-%m-%d'),
            'BatchNumber': batch_number,
            'ExpiryDate': expiry_date,
            'WarehouseID': 'WH-EAST-01',
            'ExportTimestamp': export_timestamp.strftime('%Y-%m-%d %H:%M:%S')
        })
    
    return pd.DataFrame(records)

def generate_warehouse_west(df_inventory_dist, export_date):
    """Generate West warehouse CSV (pipe-delimited, different column names)"""
    
    records = []
    
    for _, row in df_inventory_dist.iterrows():
        on_hand_qty = row['west_qty']
        
        # Allocated quantity
        if on_hand_qty > 0:
            allocated_pct = random.uniform(0.08, 0.18)
            allocated_qty = int(on_hand_qty * allocated_pct)
        else:
            allocated_qty = 0
        
        free_qty = max(0, on_hand_qty - allocated_qty)
        
        # Zone location
        zone = f"ZONE-{random.choice(['A', 'B', 'C', 'D'])}"
        
        # Last physical count (different date format: MM/DD/YYYY)
        last_count = export_date - timedelta(days=random.randint(0, 3))
        
        # Lot number
        lot_number = f"LOT-W2-{random.randint(1, 999):03d}"
        
        # File generated at (2 AM local time, but different format: MM/DD/YYYY HH:MM:SS AM/PM)
        export_timestamp = export_date.replace(hour=2, minute=0, second=0)
        
        records.append({
            'ITEM_CODE': row['sku'],
            'ITEM_NAME': row['product_name'],
            'ON_HAND_QTY': on_hand_qty,
            'ALLOCATED_QTY': allocated_qty,
            'FREE_QTY': free_qty,
            'ZONE': zone,
            'LAST_PHYSICAL_COUNT': last_count.strftime('%m/%d/%Y'),
            'LOT_NUMBER': lot_number,
            'WAREHOUSE_CODE': 'WH-WEST-02',
            'FILE_GENERATED_AT': export_timestamp.strftime('%m/%d/%Y %I:%M:%S %p')
        })
    
    return pd.DataFrame(records)

def generate_warehouse_central(df_inventory_dist, export_date):
    """Generate Central warehouse CSV (most detailed, newest warehouse)"""
    
    records = []
    
    for _, row in df_inventory_dist.iterrows():
        physical_inventory = row['central_qty']
        
        # Committed inventory
        if physical_inventory > 0:
            committed_pct = random.uniform(0.10, 0.25)
            committed_inventory = int(physical_inventory * committed_pct)
        else:
            committed_inventory = 0
        
        available_inventory = max(0, physical_inventory - committed_inventory)
        
        # In transit from supplier (for low stock items)
        if available_inventory < 50 and random.random() < 0.3:
            in_transit_from_supplier = random.randint(50, 200)
        else:
            in_transit_from_supplier = 0
        
        # Damaged quantity (small percentage)
        damaged_qty = 0
        if physical_inventory > 0 and random.random() < 0.05:
            damaged_qty = random.randint(1, max(1, int(physical_inventory * 0.02)))
        
        # Storage location
        section = f"SEC-{random.choice(['A', 'B', 'C', 'D'])}"
        row_num = f"ROW-{random.randint(1, 30)}"
        storage_location = f"{section}-{row_num}"
        
        # Last cycle count (within last 2 days, with time)
        last_count = export_date - timedelta(days=random.randint(0, 2), 
                                            hours=random.randint(0, 23),
                                            minutes=random.randint(0, 59))
        
        # Lot/batch
        lot_batch = f"BATCH-C3-{random.randint(1, 999):03d}"
        
        # Snapshot timestamp (2 AM)
        snapshot_timestamp = export_date.replace(hour=2, minute=0, second=0)
        
        records.append({
            'sku': row['sku'],
            'product_description': row['product_name'],
            'physical_inventory': physical_inventory,
            'committed_inventory': committed_inventory,
            'available_inventory': available_inventory,
            'in_transit_from_supplier': in_transit_from_supplier,
            'damaged_qty': damaged_qty,
            'storage_location': storage_location,
            'last_cycle_count': last_count.strftime('%Y-%m-%d %H:%M:%S'),
            'lot_batch': lot_batch,
            'cost_per_unit': row['cost'],
            'warehouse_id': 'WH-CENTRAL-03',
            'snapshot_timestamp': snapshot_timestamp.strftime('%Y-%m-%d %H:%M:%S')
        })
    
    return pd.DataFrame(records)

# -------------------------
# MAIN EXECUTION
# -------------------------

def main():
    """Main execution function"""
    print("=" * 70)
    print("ShopFast Warehouse CSV Data Generator - ADLS Upload")
    print("=" * 70)
    
    # Load master products from ADLS
    df_products = load_master_products_from_adls()
    
    # Distribute inventory
    print("\n📊 Distributing inventory across 3 warehouses...")
    df_inventory_dist = distribute_inventory_across_warehouses(df_products)
    print(f"✅ Distributed inventory for {len(df_inventory_dist)} SKUs")
    print(f"\n  Total Inventory:")
    print(f"    East (WH-EAST-01): {df_inventory_dist['east_qty'].sum():,} units")
    print(f"    West (WH-WEST-02): {df_inventory_dist['west_qty'].sum():,} units")
    print(f"    Central (WH-CENTRAL-03): {df_inventory_dist['central_qty'].sum():,} units")
    print(f"    TOTAL: {df_inventory_dist['total_qty'].sum():,} units")
    
    # Generate warehouse exports for last N days
    print(f"\n📁 Generating and uploading {NUM_DAYS} days of warehouse exports to ADLS...")
    
    end_date = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    
    files_uploaded = {'east': [], 'west': [], 'central': []}
    
    for day in range(NUM_DAYS):
        export_date = end_date - timedelta(days=NUM_DAYS - day - 1)
        date_str = export_date.strftime('%Y%m%d')
        
        print(f"\n  Generating exports for {export_date.strftime('%Y-%m-%d')}...")
        
        # Add some daily variation to inventory (sales, receipts)
        df_daily = df_inventory_dist.copy()
        for col in ['east_qty', 'west_qty', 'central_qty']:
            variation = np.random.randint(-10, 20, size=len(df_daily))
            df_daily[col] = (df_daily[col] + variation).clip(lower=0)
        
        # Generate and upload East warehouse file (comma-delimited)
        df_east = generate_warehouse_east(df_daily, export_date)
        east_filename = f'wh_east_inventory_{date_str}.csv'
        east_path = upload_csv_to_adls(
            df_east, 
            ADLS_PATHS['warehouse_east'], 
            east_filename,
            separator=','
        )
        files_uploaded['east'].append(east_path)
        
        # Generate and upload West warehouse file (pipe-delimited)
        df_west = generate_warehouse_west(df_daily, export_date)
        west_filename = f'WH_WEST_STOCK_{date_str}.csv'
        west_path = upload_csv_to_adls(
            df_west, 
            ADLS_PATHS['warehouse_west'], 
            west_filename,
            separator='|'
        )
        files_uploaded['west'].append(west_path)
        
        # Generate and upload Central warehouse file (comma-delimited)
        df_central = generate_warehouse_central(df_daily, export_date)
        central_filename = f'central_warehouse_inventory_{date_str}.csv'
        central_path = upload_csv_to_adls(
            df_central, 
            ADLS_PATHS['warehouse_central'], 
            central_filename,
            separator=','
        )
        files_uploaded['central'].append(central_path)
    
    # Print summary statistics
    print("\n" + "=" * 70)
    print("WAREHOUSE DATA UPLOAD SUMMARY")
    print("=" * 70)
    
    print(f"\nFiles Uploaded to ADLS:")
    print(f"  Container: {ADLS_FILESYSTEM}")
    print(f"  East Warehouse: {len(files_uploaded['east'])} files (comma-delimited)")
    print(f"  West Warehouse: {len(files_uploaded['west'])} files (pipe-delimited)")
    print(f"  Central Warehouse: {len(files_uploaded['central'])} files (comma-delimited)")
    
    print(f"\nADLS Paths:")
    print(f"  📁 {ADLS_PATHS['warehouse_east']}/")
    print(f"  📁 {ADLS_PATHS['warehouse_west']}/")
    print(f"  📁 {ADLS_PATHS['warehouse_central']}/")
    
    # Load most recent file from ADLS for statistics
    latest_date = end_date.strftime('%Y%m%d')
    
    print(f"\nLatest Export Statistics ({end_date.strftime('%Y-%m-%d')}):")
    
    service_client = connect_to_adls()
    fs_client = service_client.get_file_system_client(file_system=ADLS_FILESYSTEM)
    
    # East warehouse stats
    east_latest_path = f"{ADLS_PATHS['warehouse_east']}/wh_east_inventory_{latest_date}.csv"
    file_client = fs_client.get_file_client(east_latest_path)
    download = file_client.download_file()
    df_east_latest = pd.read_csv(StringIO(download.readall().decode('utf-8')))
    
    print(f"\n  East Warehouse:")
    print(f"    Total SKUs: {len(df_east_latest)}")
    print(f"    Total Inventory: {df_east_latest['QuantityOnHand'].sum():,} units")
    print(f"    Reserved: {df_east_latest['QuantityReserved'].sum():,} units")
    print(f"    Available: {df_east_latest['QuantityAvailable'].sum():,} units")
    stockouts_east = len(df_east_latest[df_east_latest['QuantityAvailable'] == 0])
    print(f"    Stockouts: {stockouts_east} SKUs ({stockouts_east/len(df_east_latest)*100:.1f}%)")
    
    # West warehouse stats
    west_latest_path = f"{ADLS_PATHS['warehouse_west']}/WH_WEST_STOCK_{latest_date}.csv"
    file_client = fs_client.get_file_client(west_latest_path)
    download = file_client.download_file()
    df_west_latest = pd.read_csv(StringIO(download.readall().decode('utf-8')), sep='|')
    
    print(f"\n  West Warehouse:")
    print(f"    Total SKUs: {len(df_west_latest)}")
    print(f"    Total Inventory: {df_west_latest['ON_HAND_QTY'].sum():,} units")
    print(f"    Allocated: {df_west_latest['ALLOCATED_QTY'].sum():,} units")
    print(f"    Free: {df_west_latest['FREE_QTY'].sum():,} units")
    stockouts_west = len(df_west_latest[df_west_latest['FREE_QTY'] == 0])
    print(f"    Stockouts: {stockouts_west} SKUs ({stockouts_west/len(df_west_latest)*100:.1f}%)")
    
    # Central warehouse stats
    central_latest_path = f"{ADLS_PATHS['warehouse_central']}/central_warehouse_inventory_{latest_date}.csv"
    file_client = fs_client.get_file_client(central_latest_path)
    download = file_client.download_file()
    df_central_latest = pd.read_csv(StringIO(download.readall().decode('utf-8')))
    
    print(f"\n  Central Warehouse:")
    print(f"    Total SKUs: {len(df_central_latest)}")
    print(f"    Physical Inventory: {df_central_latest['physical_inventory'].sum():,} units")
    print(f"    Committed: {df_central_latest['committed_inventory'].sum():,} units")
    print(f"    Available: {df_central_latest['available_inventory'].sum():,} units")
    print(f"    In Transit: {df_central_latest['in_transit_from_supplier'].sum():,} units")
    print(f"    Damaged: {df_central_latest['damaged_qty'].sum():,} units")
    stockouts_central = len(df_central_latest[df_central_latest['available_inventory'] == 0])
    print(f"    Stockouts: {stockouts_central} SKUs ({stockouts_central/len(df_central_latest)*100:.1f}%)")
    
    total_inventory = (df_east_latest['QuantityOnHand'].sum() + 
                      df_west_latest['ON_HAND_QTY'].sum() + 
                      df_central_latest['physical_inventory'].sum())
    
    print(f"\n  Combined Total Inventory: {total_inventory:,} units")
    
    print("\n" + "=" * 70)
    print("⚠️  NOTE: Warehouse files simulate late-arriving data")
    print("   - Files exported at 2 AM local time")
    print("   - Assume arrival in data lake 6-8 hours later")
    print("=" * 70)
    
    print("\n✅ Warehouse CSV data successfully uploaded to ADLS!")
    print("=" * 70)

if __name__ == "__main__":
    main()