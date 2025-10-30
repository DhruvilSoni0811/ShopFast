"""
Generate Website PostgreSQL Data
Now reads master_products.csv from Azure Data Lake (ADLS Gen2)
and inserts generated data into PostgreSQL (Aiven).
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
import os
from io import StringIO
from azure.storage.blob import BlobServiceClient
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv   # pip install python-dotenv
load_dotenv("../.env.development.local")


# Azure Storage
STORAGE_ACCOUNT_NAME = os.getenv("ADLS_ACCOUNT_NAME")
STORAGE_ACCOUNT_KEY = os.getenv("ADLS_ACCOUNT_KEY")
CONTAINER_NAME = "shopfast-raw-data"
BASE_PATH = "master_data"

# PostgreSQL
PG_CONN_STR = (
    f"postgres://{os.getenv('POSTGRES_USER')}:{os.getenv('POSTGRES_PASSWORD')}"
    f"@{os.getenv('POSTGRES_HOST')}:{os.getenv('POSTGRES_PORT')}"
    f"/{os.getenv('POSTGRES_DB')}?sslmode={os.getenv('POSTGRES_SSLMODE')}"
)

# ==============================
# Synthetic Data Config
# ==============================
NUM_DAYS = 30
NUM_CUSTOMERS = 1000
ORDERS_PER_DAY_RANGE = (50, 150)

# Seed for reproducibility
random.seed(42)
np.random.seed(42)


# ----------------------------------------------------
# 1️⃣ Load Master Products from ADLS Gen2
# ----------------------------------------------------
def load_master_products_from_adls():
    blob_name = f"{BASE_PATH}/master_products.csv"
    blob_service_client = BlobServiceClient(
        f"https://{STORAGE_ACCOUNT_NAME}.blob.core.windows.net",
        credential=STORAGE_ACCOUNT_KEY
    )
    blob_client = blob_service_client.get_blob_client(container=CONTAINER_NAME, blob=blob_name)
    csv_data = blob_client.download_blob().readall().decode('utf-8')
    df = pd.read_csv(StringIO(csv_data))
    print(f"✅ Loaded {len(df)} products from ADLS")
    return df


# ----------------------------------------------------
# 2️⃣ Synthetic Data Generation Logic (same as before)
# ----------------------------------------------------
def generate_customers(num_customers):
    return [f"CUST-{i:05d}" for i in range(10000, 10000 + num_customers)]

def generate_orders(df_products, customers, num_days):
    orders, order_items = [], []
    order_id_counter = 1
    item_id_counter = 1

    active_products = df_products[df_products['is_active'] == True].copy()
    velocity_weights = {'fast': 5, 'medium': 2, 'slow': 1}
    active_products['weight'] = active_products['velocity_category'].map(velocity_weights)
    end_date = datetime.now()

    for day in range(num_days):
        current_date = end_date - timedelta(days=num_days - day - 1)
        is_weekend = current_date.weekday() >= 5
        num_orders = random.randint(
            ORDERS_PER_DAY_RANGE[0] if not is_weekend else int(ORDERS_PER_DAY_RANGE[0] * 1.5),
            ORDERS_PER_DAY_RANGE[1] if not is_weekend else int(ORDERS_PER_DAY_RANGE[1] * 1.5)
        )

        for _ in range(num_orders):
            hour_weights = [1,1,1,1,1,1,1,1,2,3,4,5,5,4,4,5,6,7,6,4,3,2,1,1]
            hour = random.choices(range(24), weights=hour_weights)[0]
            minute, second = random.randint(0,59), random.randint(0,59)
            order_time = current_date.replace(hour=hour, minute=minute, second=second)

            order_id = f"WEB-{order_time.strftime('%Y%m%d')}-{order_id_counter:03d}"
            customer_id = random.choice(customers)
            order_status = random.choices(['confirmed','processing','shipped','delivered','cancelled'],
                                          weights=[0.15,0.2,0.25,0.35,0.05])[0]
            payment_method = random.choices(['credit_card','debit_card','paypal','apple_pay','google_pay'],
                                            weights=[0.45,0.25,0.15,0.10,0.05])[0]
            num_items = random.choices([1,2,3,4,5], weights=[0.4,0.3,0.2,0.07,0.03])[0]
            selected_products = active_products.sample(n=num_items, weights='weight', replace=False)

            total_amount = 0
            for _, product in selected_products.iterrows():
                quantity = random.choices([1,2,3,4], weights=[0.6,0.25,0.10,0.05])[0]
                unit_price = product['price']
                discount = round(unit_price * random.uniform(0.05,0.2),2) if random.random()<0.2 else 0
                if order_status=='cancelled': fulfillment_status='cancelled'
                elif order_status=='confirmed': fulfillment_status='pending'
                elif order_status=='processing': fulfillment_status='allocated'
                else: fulfillment_status='fulfilled'
                warehouse_allocated = random.choice(['WH-EAST-01','WH-WEST-02','WH-CENTRAL-03']) \
                                      if fulfillment_status in ['allocated','fulfilled'] else None
                order_items.append((item_id_counter, order_id, product['sku'], product['product_name'],
                                    quantity, unit_price, discount, fulfillment_status,
                                    warehouse_allocated, order_time))
                total_amount += (unit_price - discount) * quantity
                item_id_counter += 1

            updated_time = order_time + timedelta(minutes=random.randint(1,30))
            orders.append((order_id, customer_id, order_time, order_status,
                           round(total_amount,2), payment_method,
                           random.randint(1000,9999), order_time, updated_time))
            order_id_counter += 1

    return pd.DataFrame(orders, columns=[
        'order_id','customer_id','order_date','order_status','total_amount',
        'payment_method','shipping_address_id','created_at','updated_at'
    ]), pd.DataFrame(order_items, columns=[
        'order_item_id','order_id','sku','product_name','quantity','unit_price',
        'discount_applied','fulfillment_status','warehouse_allocated','created_at'
    ])

def generate_inventory(df_products, df_order_items):
    inventory = []
    inventory_id_counter = 1
    reserved_by_sku = df_order_items[df_order_items['fulfillment_status'].isin(['pending','allocated'])]\
                        .groupby('sku')['quantity'].sum().to_dict()
    for _, product in df_products.iterrows():
        if not product['is_active']: continue
        if product['velocity_category']=='fast': base_qty=random.randint(100,300)
        elif product['velocity_category']=='medium': base_qty=random.randint(50,150)
        else: base_qty=random.randint(20,80)
        reserved_qty = reserved_by_sku.get(product['sku'],0)
        if random.random()<0.05: available_qty=0
        elif random.random()<0.10: available_qty=random.randint(1,product['reorder_point'])
        else: available_qty=base_qty
        inventory.append((inventory_id_counter, product['sku'], available_qty,
                          int(reserved_qty), datetime.now()))
        inventory_id_counter += 1
    return pd.DataFrame(inventory, columns=[
        'inventory_id','sku','available_qty','reserved_qty','last_updated'
    ])

def generate_products_table(df_products):
    return df_products[df_products['is_active']==True][[
        'product_id','sku','product_name','category','subcategory','price','cost',
        'supplier_id','reorder_point','lead_time_days','is_active','created_at'
    ]]


# ----------------------------------------------------
# 3️⃣ PostgreSQL Insertion
# ----------------------------------------------------
def insert_dataframe(conn, df, table_name):
    if df.empty:
        print(f"⚠️ No data to insert into {table_name}")
        return

    cols = ','.join(df.columns)
    values = [tuple(x) for x in df.to_numpy()]
    placeholders = ','.join(['%s'] * len(df.columns))
    insert_query = f"INSERT INTO {table_name} ({cols}) VALUES %s"
    cur = conn.cursor()
    execute_values(cur, insert_query, values)
    conn.commit()
    print(f"✅ Inserted {len(df)} rows into {table_name}")


# ----------------------------------------------------
# 4️⃣ Main
# ----------------------------------------------------
def main():
    print("="*60)
    print("ShopFast Website PostgreSQL Data Generator")
    print("="*60)

    # Load products
    df_products = load_master_products_from_adls()

    customers = generate_customers(NUM_CUSTOMERS)
    df_orders, df_order_items = generate_orders(df_products, customers, NUM_DAYS)
    df_inventory = generate_inventory(df_products, df_order_items)
    df_products_table = generate_products_table(df_products)

    # PostgreSQL insertion
    conn = psycopg2.connect(PG_CONN_STR)
    insert_dataframe(conn, df_products_table, "web_products")
    insert_dataframe(conn, df_orders, "web_orders")
    insert_dataframe(conn, df_order_items, "web_order_items")
    insert_dataframe(conn, df_inventory, "web_inventory")
    conn.close()

    print("\n✅ Data generation and load complete!")


if __name__ == "__main__":
    main()
