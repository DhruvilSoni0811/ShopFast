"""
ShopFast POS Systems Data Generator - ADLS Version
Generates Manhattan store REST API responses and LA store Kafka messages
Reads from and writes to Azure Data Lake Storage (ADLS)
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
import json
import time
from dotenv import load_dotenv   # pip install python-dotenv
load_dotenv("../.env.development.local")
import os

# ADLS CONFIGURATION
ADLS_ACCOUNT_NAME = os.getenv("ADLS_ACCOUNT_NAME")
ADLS_ACCOUNT_KEY = os.getenv("ADLS_ACCOUNT_KEY")
ADLS_FILESYSTEM = "shopfast-raw-data"

# Configure Spark to access ADLS
spark.conf.set(
    f"fs.azure.account.key.{ADLS_ACCOUNT_NAME}.dfs.core.windows.net",
    ADLS_ACCOUNT_KEY
)

# ADLS Paths
ADLS_BASE_PATH = f"abfss://{ADLS_FILESYSTEM}@{ADLS_ACCOUNT_NAME}.dfs.core.windows.net"
INPUT_PATH = f"{ADLS_BASE_PATH}/master_data"
OUTPUT_PATH = f"{ADLS_BASE_PATH}/pos"

# ============================================
# GENERATION CONFIGURATION
# ============================================
NUM_DAYS = 7  # Generate last 7 days
TRANSACTIONS_PER_DAY_MANHATTAN = (40, 80)  # Slower than online
TRANSACTIONS_PER_DAY_LA = (60, 120)  # Busier store

# Seed for reproducibility
random.seed(45)
np.random.seed(45)

# ============================================
# HELPER FUNCTIONS
# ============================================

def generate_object_id():
    """Generate MongoDB-like ObjectId without bson library"""
    timestamp = int(time.time())
    random_bytes = ''.join(f'{random.randint(0, 255):02x}' for _ in range(8))
    return f"{timestamp:08x}{random_bytes}"

def load_master_products():
    """Load master product catalog from ADLS"""
    master_file = f"{INPUT_PATH}/master_products.csv"
    print(f"   Reading from: {master_file}")
    
    try:
        df = spark.read.csv(master_file, header=True, inferSchema=True).toPandas()
        return df
    except Exception as e:
        raise FileNotFoundError(f"❌ Cannot read {master_file}\nError: {str(e)}\nPlease ensure master_products.csv exists in ADLS!")

def save_json_to_adls(data, path):
    """Save JSON data to ADLS using Spark"""
    # Convert to JSON string
    json_str = json.dumps(data, indent=2)
    
    # Create RDD and save
    rdd = spark.sparkContext.parallelize([json_str])
    rdd.saveAsTextFile(path)

def save_json_array_to_adls(data_list, path):
    """Save JSON array to ADLS as a single file"""
    # Convert to JSON lines (one object per line for easier Spark reading)
    json_lines = [json.dumps(item) for item in data_list]
    
    # Create DataFrame and save as single JSON file
    df = spark.createDataFrame([(line,) for line in json_lines], ["value"])
    df.coalesce(1).write.mode("overwrite").text(path + "_temp")
    
    # Read back and save as proper JSON
    temp_files = dbutils.fs.ls(path + "_temp")
    json_file = [f.path for f in temp_files if f.path.endswith('.txt')][0]
    
    # Copy to final location
    dbutils.fs.cp(json_file, path)
    dbutils.fs.rm(path + "_temp", True)

# ============================================
# MANHATTAN STORE - REST API GENERATOR
# ============================================

def generate_manhattan_inventory_snapshots(df_products, num_days):
    """Generate Manhattan store REST API inventory snapshots (every 5 minutes)"""
    
    snapshots = []
    active_products = df_products[df_products['is_active'] == True].copy()
    
    # Only stock ~40% of SKUs in physical store
    store_products = active_products.sample(n=int(len(active_products) * 0.4))
    
    end_date = datetime.now()
    
    print(f"   Generating snapshots for {len(store_products)} SKUs...")
    
    # Generate snapshots every 5 minutes for last N days
    for day in range(num_days):
        current_date = end_date - timedelta(days=num_days - day - 1)
        
        # Store hours: 9 AM - 9 PM
        for hour in range(9, 21):
            for minute in range(0, 60, 5):  # Every 5 minutes
                snapshot_time = current_date.replace(hour=hour, minute=minute, second=0)
                
                inventory_snapshot = []
                
                for _, product in store_products.iterrows():
                    # Base inventory for store
                    if product['velocity_category'] == 'fast':
                        base_floor = random.randint(5, 15)
                        base_backroom = random.randint(15, 35)
                    elif product['velocity_category'] == 'medium':
                        base_floor = random.randint(3, 10)
                        base_backroom = random.randint(10, 25)
                    else:
                        base_floor = random.randint(2, 6)
                        base_backroom = random.randint(5, 15)
                    
                    # Simulate sales throughout the day (inventory decreases)
                    time_factor = (hour - 9) / 12  # 0.0 at 9 AM, 1.0 at 9 PM
                    floor_reduction = int(base_floor * time_factor * 0.3)
                    backroom_reduction = int(base_backroom * time_factor * 0.2)
                    
                    quantity_on_floor = max(0, base_floor - floor_reduction)
                    quantity_in_backroom = max(0, base_backroom - backroom_reduction)
                    quantity_total = quantity_on_floor + quantity_in_backroom
                    
                    # Reserved for online pickup (BOPIS - Buy Online Pick up In Store)
                    reserved_for_online_pickup = 0
                    if quantity_total > 5 and random.random() < 0.15:
                        reserved_for_online_pickup = random.randint(1, min(3, quantity_total))
                    
                    # Last sold timestamp
                    if quantity_on_floor < base_floor:
                        minutes_ago = random.randint(5, 60)
                        last_sold = (snapshot_time - timedelta(minutes=minutes_ago)).isoformat()
                    else:
                        last_sold = None
                    
                    # Promotion
                    on_promotion = random.random() < 0.15
                    promo_price = round(product['price'] * random.uniform(0.75, 0.90), 2) if on_promotion else None
                    
                    inventory_snapshot.append({
                        'sku': product['sku'],
                        'product_name': product['product_name'],
                        'quantity_on_floor': int(quantity_on_floor),
                        'quantity_in_backroom': int(quantity_in_backroom),
                        'quantity_total': int(quantity_total),
                        'reserved_for_online_pickup': int(reserved_for_online_pickup),
                        'last_sold': last_sold,
                        'price': float(product['price']),
                        'on_promotion': bool(on_promotion),
                        'promo_price': float(promo_price) if promo_price else None
                    })
                
                # Last physical count (done overnight)
                last_physical_count = current_date.replace(hour=21, minute=0, second=0)
                
                # Create API response
                api_response = {
                    'store_id': 'STORE-NYC-01',
                    'store_name': 'ShopFast Manhattan Flagship',
                    'location': {
                        'address': '500 5th Avenue',
                        'city': 'New York',
                        'state': 'NY',
                        'zip': '10110'
                    },
                    'inventory_snapshot': inventory_snapshot,
                    'timestamp': snapshot_time.isoformat(),
                    'last_physical_count': last_physical_count.isoformat()
                }
                
                snapshots.append(api_response)
    
    return snapshots

# ============================================
# LA STORE - KAFKA STREAM GENERATOR
# ============================================

def generate_la_transactions(df_products, num_days):
    """Generate LA store Kafka transaction stream"""
    
    transactions = []
    transaction_items = []
    adjustments = []
    
    active_products = df_products[df_products['is_active'] == True].copy()
    
    # Only stock ~45% of SKUs in LA store
    store_products = active_products.sample(n=int(len(active_products) * 0.45))
    
    # Weight by velocity
    velocity_weights = {'fast': 5, 'medium': 2, 'slow': 1}
    store_products['weight'] = store_products['velocity_category'].map(velocity_weights)
    
    end_date = datetime.now()
    
    # Cashier and register IDs
    cashiers = [f"EMP-LA-{i:03d}" for i in range(1, 15)]
    registers = [f"REG-{i:02d}" for i in range(1, 7)]
    
    transaction_counter = 1
    
    print(f"   Generating transactions for {len(store_products)} SKUs...")
    
    for day in range(num_days):
        current_date = end_date - timedelta(days=num_days - day - 1)
        
        # Number of transactions for the day
        is_weekend = current_date.weekday() >= 5
        num_transactions = random.randint(
            TRANSACTIONS_PER_DAY_LA[0] if not is_weekend else int(TRANSACTIONS_PER_DAY_LA[0] * 1.4),
            TRANSACTIONS_PER_DAY_LA[1] if not is_weekend else int(TRANSACTIONS_PER_DAY_LA[1] * 1.4)
        )
        
        for _ in range(num_transactions):
            # Transaction timing (store hours: 9 AM - 9 PM, peak 12-2 PM and 5-7 PM)
            hour_weights = [0, 0, 0, 0, 0, 0, 0, 0, 0, 2, 3, 4, 6, 5, 4, 3, 4, 6, 7, 5, 3, 0, 0, 0]
            hour = random.choices(range(24), weights=hour_weights)[0]
            minute = random.randint(0, 59)
            second = random.randint(0, 59)
            
            transaction_time = current_date.replace(hour=hour, minute=minute, second=second)
            
            # Event type (mostly sales, occasional returns)
            event_type = random.choices(['sale', 'return'], weights=[0.95, 0.05])[0]
            
            # Transaction ID
            transaction_id = f"TXN-LA-{transaction_time.strftime('%Y%m%d')}-{transaction_counter:04d}"
            event_id = f"evt_{generate_object_id()}"
            
            # Number of items (in-store purchases tend to be smaller)
            num_items = random.choices([1, 2, 3, 4, 5], weights=[0.5, 0.3, 0.15, 0.04, 0.01])[0]
            
            # Select products
            selected_products = store_products.sample(n=num_items, weights='weight', replace=False)
            
            # Generate items
            total_amount = 0
            
            for _, product in selected_products.iterrows():
                quantity_sold = random.choices([1, 2, 3], weights=[0.8, 0.15, 0.05])[0]
                
                # For returns, negative quantity
                if event_type == 'return':
                    quantity_sold = -quantity_sold
                
                unit_price = float(product['price'])
                
                # Discount (less common in store)
                discount = 0.0
                if random.random() < 0.1:
                    discount = round(unit_price * random.uniform(0.05, 0.15), 2)
                
                # Inventory impact
                inventory_impact = -quantity_sold  # Negative for sales, positive for returns
                
                total_amount += (unit_price - discount) * abs(quantity_sold)
                
                # For flattened version
                transaction_items.append({
                    'event_id': event_id,
                    'sku': product['sku'],
                    'product_name': product['product_name'],
                    'quantity_sold': int(quantity_sold),
                    'unit_price': float(unit_price),
                    'discount': float(discount),
                    'inventory_impact': int(inventory_impact)
                })
            
            # Payment
            payment_method = random.choices(
                ['credit_card', 'debit_card', 'cash', 'mobile_payment'],
                weights=[0.45, 0.30, 0.15, 0.10]
            )[0]
            
            tender_types = {
                'credit_card': ['VISA', 'MASTERCARD', 'AMEX', 'DISCOVER'],
                'debit_card': ['VISA', 'MASTERCARD'],
                'cash': ['USD'],
                'mobile_payment': ['APPLE_PAY', 'GOOGLE_PAY']
            }
            tender_type = random.choice(tender_types[payment_method])
            
            # Cashier and register
            cashier_id = random.choice(cashiers)
            register_id = random.choice(registers)
            
            # Create transaction event
            transaction = {
                'event_id': event_id,
                'event_type': event_type,
                'store_id': 'STORE-LA-02',
                'store_name': 'ShopFast Los Angeles',
                'transaction_id': transaction_id,
                'timestamp': transaction_time.isoformat(),
                'payment_method': payment_method,
                'payment_amount': round(total_amount, 2),
                'tender_type': tender_type,
                'cashier_id': cashier_id,
                'register_id': register_id
            }
            
            transactions.append(transaction)
            transaction_counter += 1
    
    # Generate inventory adjustment events (damage, theft, corrections)
    num_adjustments = num_days * 3  # ~3 adjustments per day
    
    print(f"   Generating inventory adjustments...")
    
    for _ in range(num_adjustments):
        days_ago = random.randint(0, num_days - 1)
        adjustment_date = end_date - timedelta(days=days_ago)
        
        # Adjustments typically happen during store close or opening
        hour = random.choice([8, 21, 22])
        minute = random.randint(0, 59)
        adjustment_time = adjustment_date.replace(hour=hour, minute=minute, second=0)
        
        # Reason for adjustment
        reason = random.choice([
            'damaged_goods',
            'theft',
            'physical_count_correction',
            'expired_product',
            'return_to_vendor'
        ])
        
        # Select product
        product = store_products.sample(n=1).iloc[0]
        
        # Adjustment quantity (usually negative, occasionally positive for corrections)
        if reason == 'physical_count_correction':
            adjustment_quantity = random.randint(-10, 15)
        else:
            adjustment_quantity = -random.randint(1, 8)
        
        # New quantity after adjustment
        new_quantity = random.randint(20, 100)
        
        adjustment = {
            'event_id': f"evt_{generate_object_id()}",
            'event_type': 'inventory_adjustment',
            'store_id': 'STORE-LA-02',
            'timestamp': adjustment_time.isoformat(),
            'reason': reason,
            'adjusted_by': random.choice(cashiers),
            'sku': product['sku'],
            'adjustment_quantity': int(adjustment_quantity),
            'new_quantity': int(new_quantity),
            'notes': f"Adjustment: {reason.replace('_', ' ').title()}"
        }
        
        adjustments.append(adjustment)
    
    return transactions, transaction_items, adjustments

# ============================================
# MAIN EXECUTION FUNCTION
# ============================================

def main():
    """Main execution function"""
    print("=" * 60)
    print("ShopFast POS Systems Data Generator")
    print("ADLS Storage Version")
    print("=" * 60)
    
    print(f"\n🔧 ADLS Configuration:")
    print(f"   Account: {ADLS_ACCOUNT_NAME}")
    print(f"   Filesystem: {ADLS_FILESYSTEM}")
    print(f"   Base Path: {ADLS_BASE_PATH}")
    
    # Load master products from ADLS
    print("\n📦 Loading master products from ADLS...")
    df_products = load_master_products()
    print(f"✅ Loaded {len(df_products)} products")
    
    # Generate Manhattan store inventory snapshots (REST API)
    print(f"\n🏪 Generating Manhattan store inventory snapshots (REST API)...")
    print(f"   Simulating 5-minute polling for {NUM_DAYS} days...")
    manhattan_snapshots = generate_manhattan_inventory_snapshots(df_products, NUM_DAYS)
    print(f"✅ Generated {len(manhattan_snapshots)} inventory snapshots")
    
    # Generate LA store transactions (Kafka stream)
    print(f"\n🏪 Generating LA store transactions (Kafka stream)...")
    la_transactions, la_items, la_adjustments = generate_la_transactions(df_products, NUM_DAYS)
    print(f"✅ Generated {len(la_transactions)} transactions")
    print(f"✅ Generated {len(la_adjustments)} inventory adjustments")
    
    # Save Manhattan data to ADLS
    print("\n💾 Saving Manhattan store data to ADLS...")
    
    # Group snapshots by day and save
    snapshots_by_day = {}
    for snapshot in manhattan_snapshots:
        day_key = snapshot['timestamp'][:10].replace('-', '')
        if day_key not in snapshots_by_day:
            snapshots_by_day[day_key] = []
        snapshots_by_day[day_key].append(snapshot)
    
    for day_key, day_snapshots in snapshots_by_day.items():
        output_path = f"{OUTPUT_PATH}/manhattan/inventory_snapshots_{day_key}.json"
        
        # Convert to Spark DataFrame and save
        df_snapshots = spark.createDataFrame([json.dumps(s) for s in day_snapshots], "string")
        df_snapshots.coalesce(1).write.mode("overwrite").text(output_path + "_temp")
        
        # Find the actual file and rename
        temp_files = dbutils.fs.ls(output_path + "_temp")
        part_file = [f.path for f in temp_files if 'part-' in f.path][0]
        dbutils.fs.cp(part_file, output_path)
        dbutils.fs.rm(output_path + "_temp", True)
        
        print(f"   ✅ inventory_snapshots_{day_key}.json ({len(day_snapshots)} snapshots)")
    
    # Save LA store data to ADLS
    print("\n💾 Saving LA store data to ADLS...")
    
    # Save transactions
    output_path = f"{OUTPUT_PATH}/la/transactions.json"
    df_txn = spark.createDataFrame([json.dumps(t) for t in la_transactions], "string")
    df_txn.coalesce(1).write.mode("overwrite").text(output_path + "_temp")
    temp_files = dbutils.fs.ls(output_path + "_temp")
    part_file = [f.path for f in temp_files if 'part-' in f.path][0]
    dbutils.fs.cp(part_file, output_path)
    dbutils.fs.rm(output_path + "_temp", True)
    print(f"   ✅ transactions.json")
    
    # Save transaction items
    output_path = f"{OUTPUT_PATH}/la/transaction_items.json"
    df_items = spark.createDataFrame([json.dumps(i) for i in la_items], "string")
    df_items.coalesce(1).write.mode("overwrite").text(output_path + "_temp")
    temp_files = dbutils.fs.ls(output_path + "_temp")
    part_file = [f.path for f in temp_files if 'part-' in f.path][0]
    dbutils.fs.cp(part_file, output_path)
    dbutils.fs.rm(output_path + "_temp", True)
    print(f"   ✅ transaction_items.json")
    
    # Save adjustments
    output_path = f"{OUTPUT_PATH}/la/inventory_adjustments.json"
    df_adj = spark.createDataFrame([json.dumps(a) for a in la_adjustments], "string")
    df_adj.coalesce(1).write.mode("overwrite").text(output_path + "_temp")
    temp_files = dbutils.fs.ls(output_path + "_temp")
    part_file = [f.path for f in temp_files if 'part-' in f.path][0]
    dbutils.fs.cp(part_file, output_path)
    dbutils.fs.rm(output_path + "_temp", True)
    print(f"   ✅ inventory_adjustments.json")
    
    # Print summary statistics
    print("\n" + "=" * 60)
    print("POS SYSTEMS DATA SUMMARY")
    print("=" * 60)
    
    print(f"\nManhattan Store (STORE-NYC-01):")
    print(f"  Data Type: REST API responses (JSON)")
    print(f"  Polling Frequency: Every 5 minutes")
    print(f"  Total Snapshots: {len(manhattan_snapshots)}")
    print(f"  Date Range: {NUM_DAYS} days")
    
    # Analyze latest snapshot
    if manhattan_snapshots:
        latest_manhattan = manhattan_snapshots[-1]
        total_inventory = sum(item['quantity_total'] for item in latest_manhattan['inventory_snapshot'])
        on_floor = sum(item['quantity_on_floor'] for item in latest_manhattan['inventory_snapshot'])
        in_backroom = sum(item['quantity_in_backroom'] for item in latest_manhattan['inventory_snapshot'])
        reserved = sum(item['reserved_for_online_pickup'] for item in latest_manhattan['inventory_snapshot'])
        
        print(f"  SKUs in Store: {len(latest_manhattan['inventory_snapshot'])}")
        print(f"  Latest Total Inventory: {total_inventory} units")
        print(f"    On Floor: {on_floor} units")
        print(f"    In Backroom: {in_backroom} units")
        print(f"    Reserved for Pickup: {reserved} units")
    
    print(f"\nLA Store (STORE-LA-02):")
    print(f"  Data Type: Kafka stream (JSON)")
    print(f"  Frequency: Real-time")
    print(f"  Total Transactions: {len(la_transactions)}")
    
    # Analyze transactions
    sales = [t for t in la_transactions if t['event_type'] == 'sale']
    returns = [t for t in la_transactions if t['event_type'] == 'return']
    
    print(f"    Sales: {len(sales)}")
    print(f"    Returns: {len(returns)}")
    
    total_revenue = sum(t['payment_amount'] for t in sales)
    avg_transaction = total_revenue / len(sales) if sales else 0
    
    print(f"  Total Revenue: ${total_revenue:,.2f}")
    print(f"  Average Transaction: ${avg_transaction:.2f}")
    
    # Payment methods
    payment_methods = {}
    for t in la_transactions:
        pm = t['payment_method']
        payment_methods[pm] = payment_methods.get(pm, 0) + 1
    
    print(f"\n  Payment Methods:")
    for method, count in sorted(payment_methods.items()):
        print(f"    {method}: {count}")
    
    print(f"\n  Inventory Adjustments: {len(la_adjustments)}")
    
    # Adjustment reasons
    reasons = {}
    for adj in la_adjustments:
        reason = adj['reason']
        reasons[reason] = reasons.get(reason, 0) + 1
    
    print(f"    Reasons:")
    for reason, count in sorted(reasons.items()):
        print(f"      {reason}: {count}")
    
    print("\n" + "=" * 60)
    print("✅ POS systems data generation complete!")
    print("=" * 60)
    
    print("\n📊 Data Characteristics:")
    print("  • Manhattan: Near real-time (5 min lag)")
    print("  • LA: Real-time streaming via Kafka")
    print("  • Different data structures for each store")
    print("  • Includes sales, returns, and adjustments")
    print("\n📁 Output Location (ADLS):")
    print(f"  {OUTPUT_PATH}")
    print("=" * 60)

# ============================================
# RUN THE GENERATOR
# ============================================
if __name__ == "__main__":
    main()