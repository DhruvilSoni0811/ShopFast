"""
Generate Warehouse CSV Exports
Creates 3 different warehouse formats with late-arriving data simulation
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
import os

# Configuration
INPUT_DIR = "GeneratedData"
OUTPUT_DIR = "GeneratedData/warehouses"
NUM_DAYS = 7  # Generate last 7 days of warehouse exports

# Seed for reproducibility
random.seed(44)
np.random.seed(44)

def load_master_products():
    """Load master product catalog"""
    master_file = os.path.join(INPUT_DIR, 'master_products.csv')
    return pd.read_csv(master_file)

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
            damaged_qty = random.randint(1, int(physical_inventory * 0.02))
        
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

def main():
    """Main execution function"""
    print("=" * 60)
    print("ShopFast Warehouse CSV Data Generator")
    print("=" * 60)
    
    # Create output directories
    os.makedirs(os.path.join(OUTPUT_DIR, 'east'), exist_ok=True)
    os.makedirs(os.path.join(OUTPUT_DIR, 'west'), exist_ok=True)
    os.makedirs(os.path.join(OUTPUT_DIR, 'central'), exist_ok=True)
    
    # Load master products
    print("\n📦 Loading master products...")
    df_products = load_master_products()
    print(f"✅ Loaded {len(df_products)} products")
    
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
    print(f"\n📁 Generating {NUM_DAYS} days of warehouse exports...")
    
    end_date = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    
    for day in range(NUM_DAYS):
        export_date = end_date - timedelta(days=NUM_DAYS - day - 1)
        date_str = export_date.strftime('%Y%m%d')
        
        print(f"\n  Generating exports for {export_date.strftime('%Y-%m-%d')}...")
        
        # Add some daily variation to inventory (sales, receipts)
        df_daily = df_inventory_dist.copy()
        for col in ['east_qty', 'west_qty', 'central_qty']:
            variation = np.random.randint(-10, 20, size=len(df_daily))
            df_daily[col] = (df_daily[col] + variation).clip(lower=0)
        
        # Generate East warehouse file
        df_east = generate_warehouse_east(df_daily, export_date)
        east_file = os.path.join(OUTPUT_DIR, 'east', f'wh_east_inventory_{date_str}.csv')
        df_east.to_csv(east_file, index=False)
        print(f"    ✅ East: {east_file}")
        
        # Generate West warehouse file (pipe-delimited)
        df_west = generate_warehouse_west(df_daily, export_date)
        west_file = os.path.join(OUTPUT_DIR, 'west', f'WH_WEST_STOCK_{date_str}.csv')
        df_west.to_csv(west_file, index=False, sep='|')
        print(f"    ✅ West: {west_file}")
        
        # Generate Central warehouse file
        df_central = generate_warehouse_central(df_daily, export_date)
        central_file = os.path.join(OUTPUT_DIR, 'central', f'central_warehouse_inventory_{date_str}.csv')
        df_central.to_csv(central_file, index=False)
        print(f"    ✅ Central: {central_file}")
    
    # Print summary statistics
    print("\n" + "=" * 60)
    print("WAREHOUSE DATA SUMMARY")
    print("=" * 60)
    
    print(f"\nFiles Generated:")
    print(f"  East Warehouse: {NUM_DAYS} files (comma-delimited)")
    print(f"  West Warehouse: {NUM_DAYS} files (pipe-delimited)")
    print(f"  Central Warehouse: {NUM_DAYS} files (comma-delimited)")
    
    # Load and analyze most recent files
    latest_date = end_date.strftime('%Y%m%d')
    
    df_east_latest = pd.read_csv(os.path.join(OUTPUT_DIR, 'east', f'wh_east_inventory_{latest_date}.csv'))
    df_west_latest = pd.read_csv(os.path.join(OUTPUT_DIR, 'west', f'WH_WEST_STOCK_{latest_date}.csv'), sep='|')
    df_central_latest = pd.read_csv(os.path.join(OUTPUT_DIR, 'central', f'central_warehouse_inventory_{latest_date}.csv'))
    
    print(f"\nLatest Export Statistics ({end_date.strftime('%Y-%m-%d')}):")
    
    print(f"\n  East Warehouse:")
    print(f"    Total SKUs: {len(df_east_latest)}")
    print(f"    Total Inventory: {df_east_latest['QuantityOnHand'].sum():,} units")
    print(f"    Reserved: {df_east_latest['QuantityReserved'].sum():,} units")
    print(f"    Available: {df_east_latest['QuantityAvailable'].sum():,} units")
    stockouts_east = len(df_east_latest[df_east_latest['QuantityAvailable'] == 0])
    print(f"    Stockouts: {stockouts_east} SKUs ({stockouts_east/len(df_east_latest)*100:.1f}%)")
    
    print(f"\n  West Warehouse:")
    print(f"    Total SKUs: {len(df_west_latest)}")
    print(f"    Total Inventory: {df_west_latest['ON_HAND_QTY'].sum():,} units")
    print(f"    Allocated: {df_west_latest['ALLOCATED_QTY'].sum():,} units")
    print(f"    Free: {df_west_latest['FREE_QTY'].sum():,} units")
    stockouts_west = len(df_west_latest[df_west_latest['FREE_QTY'] == 0])
    print(f"    Stockouts: {stockouts_west} SKUs ({stockouts_west/len(df_west_latest)*100:.1f}%)")
    
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
    
    print("\n" + "=" * 60)
    print("⚠️  NOTE: Warehouse files simulate late-arriving data")
    print("   - Files exported at 2 AM local time")
    print("   - Assume arrival in data lake 6-8 hours later")
    print("=" * 60)
    
    print("\n✅ Warehouse CSV data ready!")
    print("=" * 60)

if __name__ == "__main__":
    main()