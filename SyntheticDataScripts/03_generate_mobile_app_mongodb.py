#!/usr/bin/env python3
"""
generate_mobile_app_mongodb.py

End-to-end mobile-app synthetic data generator:
- Reads master_products.csv from ADLS Gen2
- Generates denormalized app_orders, flattened app_order_items,
  cart events, and inventory_sync
- Inserts the generated documents into MongoDB Atlas
"""

import os
import json
import random
from io import StringIO
from datetime import datetime, timedelta
from urllib.parse import quote_plus  # ✅ ADDED: Missing import

import pandas as pd
import numpy as np
from bson import ObjectId
from pymongo import MongoClient, errors
from azure.storage.filedatalake import DataLakeServiceClient
from dotenv import load_dotenv   # pip install python-dotenv
load_dotenv("../.env.development.local")

# -------------------------
# CONFIGURATION
# -------------------------

# ADLS
ADLS_ACCOUNT_NAME = os.getenv("ADLS_ACCOUNT_NAME")
ADLS_ACCOUNT_KEY = os.getenv("ADLS_ACCOUNT_KEY")
ADLS_FILESYSTEM = "shopfast-raw-data"
ADLS_MASTER_PRODUCTS_PATH = "master_data/master_products.csv"
ADLS_ENDPOINT = f"https://{ADLS_ACCOUNT_NAME}.dfs.core.windows.net/"

# MongoDB
MONGO_USERNAME = os.getenv("MONGO_USERNAME")
MONGO_PASSWORD = os.getenv("MONGO_PASSWORD")
MONGO_CLUSTER = os.getenv("MONGO_CLUSTER")
DB_NAME = os.getenv("MONGO_DB")

encoded_password = quote_plus(MONGO_PASSWORD)
encoded_username = quote_plus(MONGO_USERNAME)

MONGO_URI = (
    f"mongodb+srv://{encoded_username}:{encoded_password}"
    f"@{MONGO_CLUSTER}/"
    f"?retryWrites=true&w=majority&appName=ShopFastMoblieAppMongoCluster"
)

# ✅ COLLECTION DEFINITIONS (moved to config section)
COLLECTIONS = {
    "orders": "app_orders",
    "order_items": "app_order_items",
    "cart_events": "app_cart_events",
    "inventory_sync": "app_inventory_sync"
}

# Generator settings
NUM_DAYS = 30
NUM_APP_CUSTOMERS = 800
ORDERS_PER_DAY_RANGE = (30, 100)

# Reproducibility
random.seed(43)
np.random.seed(43)

# Insert batch size for MongoDB
MONGO_BATCH_SIZE = 1000


# -------------------------
# HELPERS
# -------------------------

def generate_object_id():
    return str(ObjectId())


def connect_to_adls():
    account_url = f"https://{ADLS_ACCOUNT_NAME}.dfs.core.windows.net"
    return DataLakeServiceClient(account_url=account_url, credential=ADLS_ACCOUNT_KEY)


def load_master_products_from_adls():
    """Load master_products.csv from ADLS into a pandas DataFrame."""
    print("📂 Reading master_products.csv from ADLS...")
    service_client = connect_to_adls()
    fs_client = service_client.get_file_system_client(file_system=ADLS_FILESYSTEM)
    file_client = fs_client.get_file_client(ADLS_MASTER_PRODUCTS_PATH)
    download = file_client.download_file()
    raw = download.readall()
    df = pd.read_csv(StringIO(raw.decode("utf-8")))
    # Defensive: ensure expected columns exist (minimal)
    expected_cols = {'product_id', 'sku', 'product_name', 'category', 'price', 'velocity_category', 'is_active', 'reorder_point'}
    missing = expected_cols - set(df.columns)
    if missing:
        print(f"⚠️ Warning: master_products.csv missing columns: {missing}")
    print(f"✅ Loaded {len(df)} products from ADLS")
    return df


def connect_to_mongodb():
    """Connect to MongoDB Atlas and return database object."""
    try:
        client = MongoClient(
            MONGO_URI,
            tls=True,
            tlsAllowInvalidCertificates=False,
            serverSelectionTimeoutMS=10000
        )
        # Test the connection
        client.admin.command('ping')
        db = client[DB_NAME]
        print(f"✅ Connected to MongoDB database: {DB_NAME}")
        
        # List collections to verify
        collections = db.list_collection_names()
        if collections:
            print(f"📚 Existing collections: {collections}")
        
        return db
        
    except errors.ServerSelectionTimeoutError as e:
        print(f"❌ MongoDB Connection Error: {e}")
        print("\n🔍 Troubleshooting steps:")
        print("1. Verify username and password in Atlas dashboard")
        print("2. Check IP whitelist (0.0.0.0/0 should allow all)")
        print("3. Ensure database user has correct permissions")
        print("4. Check if cluster is active and not paused")
        raise RuntimeError(f"Could not connect to MongoDB: {e}")


def batch_insert(collection, docs, batch_size=MONGO_BATCH_SIZE):
    """Insert docs in batches to avoid giant single insert for big lists."""
    if not docs:
        return 0
    inserted = 0
    for i in range(0, len(docs), batch_size):
        chunk = docs[i:i + batch_size]
        collection.insert_many(chunk)
        inserted += len(chunk)
    return inserted


# -------------------------
# DATA GENERATION
# -------------------------

def generate_app_customers(num_customers):
    first_names = ['Sarah', 'John', 'Emma', 'Michael', 'Olivia', 'David', 'Sophia', 'James',
                   'Ava', 'Robert', 'Isabella', 'William', 'Mia', 'Daniel', 'Emily']
    last_names = ['Johnson', 'Smith', 'Williams', 'Brown', 'Jones', 'Garcia', 'Miller',
                  'Davis', 'Martinez', 'Hernandez', 'Lopez', 'Wilson', 'Anderson', 'Taylor']
    customers = []
    for i in range(num_customers):
        first = random.choice(first_names)
        last = random.choice(last_names)
        customers.append({
            'customer_id': f"CUST-{20000 + i:05d}",
            'name': f"{first} {last}",
            'email': f"{first.lower()}.{last.lower()}{random.randint(1,999)}@email.com",
            'phone': f"+1-555-{random.randint(1000,9999)}"
        })
    return customers


def generate_app_orders(df_products, customers, num_days):
    """Return: (orders_list, flattened_order_items_list)"""
    orders = []
    order_items_flat = []
    order_id_counter = 1

    active_products = df_products[df_products['is_active'] == True].copy()
    if 'velocity_category' not in active_products.columns:
        active_products['velocity_category'] = 'medium'
    velocity_weights = {'fast': 5, 'medium': 2, 'slow': 1}
    active_products['weight'] = active_products['velocity_category'].map(velocity_weights).fillna(1)

    end_date = datetime.now()

    app_versions = ['3.2.1', '3.2.0', '3.1.9', '3.1.8']
    device_types = ['iOS', 'Android']

    for day in range(num_days):
        current_date = end_date - timedelta(days=num_days - day - 1)
        is_weekend = current_date.weekday() >= 5
        num_orders = random.randint(
            ORDERS_PER_DAY_RANGE[0] if not is_weekend else int(ORDERS_PER_DAY_RANGE[0] * 1.3),
            ORDERS_PER_DAY_RANGE[1] if not is_weekend else int(ORDERS_PER_DAY_RANGE[1] * 1.3)
        )

        for _ in range(num_orders):
            # hour distribution (mobile peaks)
            hour_weights = [1, 1, 1, 1, 1, 1, 1, 2, 3, 4, 4, 5, 6, 5, 4, 3, 4, 5, 7, 8, 7, 6, 4, 2]
            hour = random.choices(range(24), weights=hour_weights)[0]
            minute = random.randint(0, 59)
            second = random.randint(0, 59)
            order_time = current_date.replace(hour=hour, minute=minute, second=second)

            order_id = f"APP-{order_time.strftime('%Y%m%d')}-{order_id_counter:03d}"
            customer = random.choice(customers)

            status = random.choices(['processing', 'confirmed', 'shipped', 'delivered', 'cancelled'],
                                    weights=[0.25, 0.15, 0.20, 0.35, 0.05])[0]
            num_items = random.choices([1, 2, 3], weights=[0.5, 0.35, 0.15])[0]

            # sample products
            if len(active_products) >= num_items:
                selected_products = active_products.sample(n=num_items, weights='weight', replace=False)
            else:
                selected_products = active_products.sample(n=num_items, replace=True)

            items = []
            total_amount = 0.0

            for _, product in selected_products.iterrows():
                quantity = random.choices([1, 2, 3], weights=[0.7, 0.25, 0.05])[0]
                unit_price = float(product.get('price', 10.0))

                size = random.choice(['XS', 'S', 'M', 'L', 'XL']) if product.get('category') == 'Fashion' else None
                color = random.choice(['Black', 'White', 'Blue', 'Red', 'Gray']) if product.get('category') in ['Fashion', 'Electronics'] else None

                item_doc = {
                    '_id': generate_object_id(),
                    'order_id': order_id,
                    'sku': product.get('sku'),
                    'product_name': product.get('product_name'),
                    'quantity': int(quantity),
                    'unit_price': unit_price,
                    'size': size,
                    'color': color
                }
                order_items_flat.append(item_doc)

                items.append({
                    'sku': product.get('sku'),
                    'product_name': product.get('product_name'),
                    'quantity': int(quantity),
                    'unit_price': unit_price,
                    'size': size,
                    'color': color
                })
                total_amount += unit_price * quantity

            payment_method = random.choices(['apple_pay', 'google_pay', 'credit_card', 'paypal'],
                                            weights=[0.35, 0.25, 0.25, 0.15])[0]
            payment = {
                'method': payment_method,
                'transaction_id': f"{payment_method.upper()[:3]}-TXN-{random.randint(100000, 999999)}",
                'amount': round(total_amount, 2),
                'currency': 'USD',
                'status': 'completed' if status != 'cancelled' else 'refunded'
            }

            shipping = {
                'address': {
                    'street': f"{random.randint(100, 9999)} {random.choice(['Main', 'Oak', 'Maple', 'Pine', 'Cedar'])} St",
                    'city': random.choice(['San Francisco', 'Los Angeles', 'New York', 'Chicago', 'Seattle', 'Boston']),
                    'state': random.choice(['CA', 'NY', 'IL', 'WA', 'MA', 'TX']),
                    'zip': f"{random.randint(10000, 99999)}"
                },
                'method': random.choice(['standard', 'express', 'overnight']),
                'tracking_number': f"TRK-{random.randint(1000000000, 9999999999)}" if status in ['shipped', 'delivered'] else None
            }

            promo_code = random.choice(['WINTER20', 'SAVE10', 'MOBILE15', 'WELCOME25', None, None, None])
            updated_time = order_time + timedelta(minutes=random.randint(1, 45))

            order_doc = {
                '_id': generate_object_id(),
                'order_id': order_id,
                'customer_id': customer['customer_id'],
                'customer_name': customer['name'],
                'customer_email': customer['email'],
                'customer_phone': customer['phone'],
                'app_version': random.choice(app_versions),
                'device_type': random.choice(device_types),
                'order_date': order_time,
                'status': status,
                'items': items,  # nested
                'payment': {
                    'method': payment['method'],
                    'transaction_id': payment['transaction_id'],
                    'amount': payment['amount'],
                    'currency': payment['currency'],
                    'status': payment['status']
                },
                'shipping_street': shipping['address']['street'],
                'shipping_city': shipping['address']['city'],
                'shipping_state': shipping['address']['state'],
                'shipping_zip': shipping['address']['zip'],
                'shipping_method': shipping['method'],
                'tracking_number': shipping['tracking_number'],
                'session_id': f"sess_{generate_object_id()[:16]}",
                'promo_code': promo_code,
                'created_at': order_time,
                'updated_at': updated_time
            }

            orders.append(order_doc)
            order_id_counter += 1

    return orders, order_items_flat


def generate_cart_events(df_products, customers, num_days):
    events = []
    active_products = df_products[df_products['is_active'] == True].copy()
    if active_products.empty:
        return events

    end_date = datetime.now()
    # ~5 cart events per day per 10 customers (approx)
    num_events = max(100, num_days * len(customers) // 2)

    for _ in range(num_events):
        days_ago = random.randint(0, num_days - 1)
        event_date = end_date - timedelta(days=days_ago)
        hour = random.randint(8, 23)
        minute = random.randint(0, 59)
        second = random.randint(0, 59)
        event_time = event_date.replace(hour=hour, minute=minute, second=second)

        customer = random.choice(customers)
        product = active_products.sample(n=1).iloc[0]

        event_type = random.choices(['add_to_cart', 'remove_from_cart'], weights=[0.8, 0.2])[0]
        quantity = random.choices([1, 2, 3], weights=[0.7, 0.2, 0.1])[0]
        inventory_reserved = event_type == 'add_to_cart' and random.random() > 0.3
        expires_at = event_time + timedelta(minutes=30)

        event = {
            '_id': generate_object_id(),
            'event_type': event_type,
            'customer_id': customer['customer_id'],
            'session_id': f"sess_{generate_object_id()[:16]}",
            'sku': product['sku'],
            'quantity': int(quantity),
            'timestamp': event_time,
            'inventory_reserved': bool(inventory_reserved),
            'expires_at': expires_at
        }
        events.append(event)

    return events


def generate_inventory_sync(df_products):
    inventory_sync = []
    active_products = df_products[df_products['is_active'] == True].copy()
    if active_products.empty:
        return inventory_sync

    last_sync = datetime.now() - timedelta(minutes=random.randint(10, 20))
    for _, product in active_products.iterrows():
        v = product.get('velocity_category', 'medium')
        if v == 'fast':
            base_qty = random.randint(80, 250)
        elif v == 'medium':
            base_qty = random.randint(40, 120)
        else:
            base_qty = random.randint(15, 70)

        if random.random() < 0.07:
            available_qty = 0
            display_status = 'out_of_stock'
        elif random.random() < 0.12:
            available_qty = random.randint(1, 5)
            display_status = 'low_stock'
        else:
            available_qty = base_qty
            display_status = 'in_stock'

        source_warehouse = random.choice(['WH-EAST-01', 'WH-WEST-02', 'WH-CENTRAL-03'])
        estimated_delivery = datetime.now() + timedelta(days=random.randint(2, 5))

        inv_doc = {
            '_id': generate_object_id(),
            'sku': product.get('sku'),
            'available_quantity': int(available_qty),
            'display_status': display_status,
            'last_synced': last_sync,
            'source_warehouse': source_warehouse,
            'estimated_delivery': estimated_delivery
        }
        inventory_sync.append(inv_doc)

    return inventory_sync


# -------------------------
# MAIN
# -------------------------

def main():
    print("=" * 70)
    print("ShopFast Mobile App MongoDB Data Generator - START")
    print("=" * 70)

    # Load products from ADLS
    df_products = load_master_products_from_adls()

    # Generate customers and data
    customers = generate_app_customers(NUM_APP_CUSTOMERS)
    print(f"👥 Generated {len(customers)} customers")

    print(f"🛍️ Generating mobile orders for {NUM_DAYS} days...")
    orders, order_items_flat = generate_app_orders(df_products, customers, NUM_DAYS)
    print(f"✅ Generated {len(orders)} orders and {len(order_items_flat)} flattened order items")

    print("🛒 Generating cart events...")
    cart_events = generate_cart_events(df_products, customers, NUM_DAYS)
    print(f"✅ Generated {len(cart_events)} cart events")

    print("📦 Generating inventory sync snapshot...")
    inventory_sync = generate_inventory_sync(df_products)
    print(f"✅ Generated inventory for {len(inventory_sync)} SKUs")

    # Connect to MongoDB and insert
    db = connect_to_mongodb()

    print("\n📤 Clearing existing collections (for clean runs)...")
    db[COLLECTIONS["orders"]].delete_many({})
    db[COLLECTIONS["order_items"]].delete_many({})
    db[COLLECTIONS["cart_events"]].delete_many({})
    db[COLLECTIONS["inventory_sync"]].delete_many({})

    print("📤 Inserting generated data into MongoDB (batched)...")
    inserted_orders = batch_insert(db[COLLECTIONS["orders"]], orders)
    inserted_items = batch_insert(db[COLLECTIONS["order_items"]], order_items_flat)
    inserted_cart = batch_insert(db[COLLECTIONS["cart_events"]], cart_events)
    inserted_inventory = batch_insert(db[COLLECTIONS["inventory_sync"]], inventory_sync)

    print(f"\n✅ Inserted: orders={inserted_orders}, order_items={inserted_items}, cart_events={inserted_cart}, inventory_sync={inserted_inventory}")
    print("\n🎉 Mobile App MongoDB data generation completed successfully!")
    print("=" * 70)


if __name__ == "__main__":
    main()