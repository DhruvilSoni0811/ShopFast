"""
ShopFast POS Systems Data Generator - ADLS Version
- Manhattan inventory snapshots generated with Spark (fast, scalable)
- LA transactions still in Python (as before)
- Rolling JSON writer with 10 MB max file size (no partial records)
- TRUE APPEND: new files created on each run, no overwrites
- JSON format for Manhattan snapshots is kept EXACT as before
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
import math

from pyspark.sql import functions as F

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

# (keeping Manhattan tuple, but current logic doesn’t use it directly)
TRANSACTIONS_PER_DAY_MANHATTAN = (800, 1600)

# FIXED: your upper bound was 250 (bug), changed to 2500
TRANSACTIONS_PER_DAY_LA = (1500, 2500)

# Rolling config
MAX_FILE_BYTES = 10 * 1024 * 1024  # 10 MB
ZERO_PAD_WIDTH = 5  # _00001.json style

# Seed for reproducibility
random.seed(45)
np.random.seed(45)

# ============================================
# HELPERS: Data generation
# ============================================
def generate_object_id():
    """Generate MongoDB-like ObjectId without bson library"""
    timestamp = int(time.time())
    random_bytes = ''.join(f'{random.randint(0, 255):02x}' for _ in range(8))
    return f"{timestamp:08x}{random_bytes}"

def load_master_products():
    """Load master product catalog from ADLS into pandas (for LA generator)"""
    master_file = f"{INPUT_PATH}/master_products.csv"
    print(f"   Reading from: {master_file}")
    try:
        df = spark.read.csv(master_file, header=True, inferSchema=True).toPandas()
        return df
    except Exception as e:
        raise FileNotFoundError(
            f"❌ Cannot read {master_file}\nError: {str(e)}\nPlease ensure master_products.csv exists in ADLS!"
        )

# ============================================
# HELPERS: Rolling JSON writer (TRUE APPEND)
# ============================================
def _ensure_parent_exists(path):
    """
    Ensure parent directory exists for dbutils.fs.put() — dbutils.fs.put will
    create the file path, but mkdirs on parent is safe.
    """
    parent = "/".join(path.split("/")[:-1])
    try:
        dbutils.fs.mkdirs(parent)
    except Exception:
        pass

def get_starting_index(path_prefix):
    """
    Detect existing files and return the next index (ensures append, no overwrite).
    Example:
      existing: transactions_00001.json, transactions_00002.json
      -> returns 3
    """
    try:
        directory = os.path.dirname(path_prefix)
        prefix = os.path.basename(path_prefix)
        files = dbutils.fs.ls(directory)

        nums = []
        for f in files:
            name = f.name
            # match: prefix_00001.json
            if name.startswith(prefix + "_") and name.endswith(".json"):
                idx_str = name[len(prefix) + 1 : -5]  # between "_" and ".json"
                if idx_str.isdigit():
                    nums.append(int(idx_str))

        if nums:
            return max(nums) + 1
        return 1
    except Exception as e:
        print(f"   ⚠️ Index detection failed for {path_prefix}: {e}")
        return 1

def write_json_rolling(path_prefix, records, max_bytes=MAX_FILE_BYTES, zero_pad=ZERO_PAD_WIDTH):
    """
    Write a list of JSON-serializable `records` into one or more JSON files
    at ADLS path: {path_prefix}_{index:0{zero_pad}d}.json

    - Each output file is a valid JSON array: [obj, obj, ...]
    - Rollover happens if adding the next complete record would exceed max_bytes.
    - No record is ever split across files.
    - Index continues from existing files so old files are NEVER overwritten.
    """
    if not records:
        return []

    file_index = get_starting_index(path_prefix)
    written_files = []

    current_items = []  # list of JSON strings
    current_size = 2    # bytes for '[' and ']'

    for rec in records:
        item_str = json.dumps(rec, ensure_ascii=False)
        item_bytes = len(item_str.encode('utf-8'))
        comma_bytes = 1 if current_items else 0

        # Need to flush current file before adding this record
        if current_items and (current_size + comma_bytes + item_bytes > max_bytes):
            filename = f"{path_prefix}_{file_index:0{zero_pad}d}.json"
            _write_array_to_adls(filename, current_items)
            written_files.append(filename)

            file_index += 1
            current_items = []
            current_size = 2
            comma_bytes = 0

        current_items.append(item_str)
        current_size += comma_bytes + item_bytes

    # Flush remaining records
    if current_items:
        filename = f"{path_prefix}_{file_index:0{zero_pad}d}.json"
        _write_array_to_adls(filename, current_items)
        written_files.append(filename)

    return written_files

def _write_array_to_adls(final_path, json_item_strings):
    """
    Writes provided JSON object strings as a single JSON array into ADLS at final_path.
    """
    _ensure_parent_exists(final_path)
    body = "[" + ",".join(json_item_strings) + "]"

    try:
        # overwrite=False to be extra safe; filenames are unique by index.
        dbutils.fs.put(final_path, body, overwrite=False)
    except Exception as e:
        print(f"   ⚠️ dbutils.fs.put failed for {final_path}: {e} — using Spark fallback")
        temp_dir = final_path + "_temp_" + str(int(time.time() * 1000))
        try:
            df = spark.createDataFrame([(body,)], ["value"])
            df.coalesce(1).write.mode("overwrite").text(temp_dir)
            temp_files = dbutils.fs.ls(temp_dir)
            part_file = [f.path for f in temp_files if 'part-' in f.path][0]
            dbutils.fs.cp(part_file, final_path)
            dbutils.fs.rm(temp_dir, True)
        except Exception as e2:
            raise RuntimeError(f"Failed to write file via both dbutils.fs.put and Spark fallback: {e2}")

# ============================================
# MANHATTAN STORE - REST API GENERATOR (SPARK)
# ============================================
def generate_manhattan_inventory_snapshots(df_products, num_days):
    """
    Spark-based generator for Manhattan inventory snapshots.

    Keeps the JSON format EXACTLY as before:
    {
      "store_id": "STORE-NYC-01",
      "store_name": "...",
      "location": { ... },
      "inventory_snapshot": [ { sku-level fields }, ... ],
      "timestamp": "...",
      "last_physical_count": "..."
    }
    """

    # Filter active products in pandas, convert to Spark
    active_pdf = df_products[df_products['is_active'] == True].copy()
    if active_pdf.empty:
        print("   ⚠️ No active products found for Manhattan store.")
        return []

    products_sdf = spark.createDataFrame(active_pdf)

    # ~80% of SKUs in Manhattan store (approximate sampling)
    store_products_sdf = products_sdf.sample(
        withReplacement=False, fraction=0.8, seed=45
    ).cache()

    sku_count = store_products_sdf.count()
    print(f"   Generating Manhattan snapshots in Spark for approx {sku_count} SKUs...")

    # Build snapshot timestamps (small, fine to do in driver)
    end_date = datetime.now()
    snapshot_times = []
    for day in range(num_days):
        current_date = end_date - timedelta(days=num_days - day - 1)
        for hour in range(9, 21):      # 9 AM to 9 PM
            for minute in range(0, 60, 1):  # every 1 minute
                snapshot_times.append(
                    current_date.replace(hour=hour, minute=minute, second=0, microsecond=0)
                )

    times_df = spark.createDataFrame(
        [(t,) for t in snapshot_times],
        schema=["snapshot_time"]
    )

    # Cross join: (snapshot_time x store_products)
    base_df = times_df.crossJoin(store_products_sdf)

    # ==== Compute inventory quantities, promotions, etc. ====
    # Aliases for convenience
    vc = F.col("velocity_category")
    price_col = F.col("price")

    # Base floor/backroom by velocity with randomization
    rand_fast1  = F.rand(101)
    rand_med1   = F.rand(102)
    rand_slow1  = F.rand(103)
    rand_fast2  = F.rand(201)
    rand_med2   = F.rand(202)
    rand_slow2  = F.rand(203)

    # randint(a,b) → floor(rand()*(b-a+1)) + a
    base_floor = (
        F.when(vc == "fast",   F.floor(rand_fast1 * (15 - 5 + 1)) + 5)
         .when(vc == "medium", F.floor(rand_med1  * (10 - 3 + 1)) + 3)
         .otherwise(           F.floor(rand_slow1 * (6  - 2 + 1)) + 2)
    ).cast("int")

    base_backroom = (
        F.when(vc == "fast",   F.floor(rand_fast2 * (35 - 15 + 1)) + 15)
         .when(vc == "medium", F.floor(rand_med2  * (25 - 10 + 1)) + 10)
         .otherwise(           F.floor(rand_slow2 * (15 - 5  + 1)) + 5)
    ).cast("int")

    hour_col = F.hour("snapshot_time")
    time_factor = (hour_col - F.lit(9.0)) / F.lit(12.0)

    floor_reduction = F.floor(base_floor * time_factor * F.lit(0.3)).cast("int")
    backroom_reduction = F.floor(base_backroom * time_factor * F.lit(0.2)).cast("int")

    q_floor = F.greatest(F.lit(0), base_floor - floor_reduction)
    q_back  = F.greatest(F.lit(0), base_backroom - backroom_reduction)
    q_total = q_floor + q_back

    # Reserved for online pickup
    rand_flag = F.rand(301)
    rand_qty  = F.rand(302)

    flag_reserved = (q_total > 5) & (rand_flag < 0.15)
    max_reserved = F.least(F.lit(3), q_total)
    reserved_qty = F.when(
        flag_reserved,
        (F.floor(rand_qty * (max_reserved - 1 + 1)) + 1).cast("int")
    ).otherwise(F.lit(0))

    # last_sold only if q_floor < base_floor
    rand_min_ago = F.rand(401)
    minutes_ago = (F.floor(rand_min_ago * (60 - 5 + 1)) + 5).cast("int")

    snapshot_epoch = F.col("snapshot_time").cast("long")
    last_sold_ts = snapshot_epoch - (minutes_ago * 60)
    last_sold_dt = F.from_unixtime(last_sold_ts)

    last_sold_str = F.when(
        q_floor < base_floor,
        F.date_format(last_sold_dt, "yyyy-MM-dd'T'HH:mm:ss")
    ).otherwise(F.lit(None))

    # Promotions
    rand_promo = F.rand(501)
    promo_flag = rand_promo < 0.15
    rand_promo_factor = F.rand(502)  # between 0.75 and 0.90
    promo_factor = rand_promo_factor * F.lit(0.15) + F.lit(0.75)
    promo_price = F.when(promo_flag, F.round(price_col * promo_factor, 2)).otherwise(F.lit(None))

    inv_df = (
        base_df
        .withColumn("base_floor", base_floor)
        .withColumn("base_backroom", base_backroom)
        .withColumn("quantity_on_floor", q_floor.cast("int"))
        .withColumn("quantity_in_backroom", q_back.cast("int"))
        .withColumn("quantity_total", q_total.cast("int"))
        .withColumn("reserved_for_online_pickup", reserved_qty.cast("int"))
        .withColumn("last_sold", last_sold_str)
        .withColumn("on_promotion", promo_flag.cast("boolean"))
        .withColumn("promo_price", promo_price.cast("double"))
    )

    # Collect inventory_snapshot as array of structs per snapshot_time
    inventory_struct = F.struct(
        F.col("sku"),
        F.col("product_name"),
        F.col("quantity_on_floor"),
        F.col("quantity_in_backroom"),
        F.col("quantity_total"),
        F.col("reserved_for_online_pickup"),
        F.col("last_sold"),
        price_col.cast("double").alias("price"),
        F.col("on_promotion"),
        F.col("promo_price")
    )

    snapshots_df = (
        inv_df
        .groupBy("snapshot_time")
        .agg(F.collect_list(inventory_struct).alias("inventory_snapshot"))
    )

    # last_physical_count: same date at 21:00:00
    last_physical_ts = F.to_timestamp(
        F.concat(
            F.date_format("snapshot_time", "yyyy-MM-dd"),
            F.lit(" 21:00:00")
        ),
        "yyyy-MM-dd HH:mm:ss"
    )
    last_physical_str = F.date_format(last_physical_ts, "yyyy-MM-dd'T'HH:mm:ss")

    # Build final API-level struct matching original JSON
    api_struct = F.struct(
        F.lit("STORE-NYC-01").alias("store_id"),
        F.lit("ShopFast Manhattan Flagship").alias("store_name"),
        F.struct(
            F.lit("500 5th Avenue").alias("address"),
            F.lit("New York").alias("city"),
            F.lit("NY").alias("state"),
            F.lit("10110").alias("zip")
        ).alias("location"),
        F.col("inventory_snapshot"),
        F.date_format("snapshot_time", "yyyy-MM-dd'T'HH:mm:ss").alias("timestamp"),
        last_physical_str.alias("last_physical_count")
    ).alias("api")

    final_df = snapshots_df.select(api_struct)

    # Collect to driver as list of Python dicts (same structure as before)
    records = [row.api.asDict(recursive=True) for row in final_df.collect()]
    print(f"   ✅ Spark Manhattan snapshots generated: {len(records)}")
    return records

# ============================================
# LA STORE - KAFKA STREAM GENERATOR (unchanged logic)
# ============================================
def generate_la_transactions(df_products, num_days):
    transactions = []
    transaction_items = []
    adjustments = []

    active_products = df_products[df_products['is_active'] == True].copy()
    store_products = active_products.sample(n=int(len(active_products) * 0.45))
    velocity_weights = {'fast': 5, 'medium': 2, 'slow': 1}
    store_products['weight'] = store_products['velocity_category'].map(velocity_weights)

    end_date = datetime.now()
    cashiers = [f"EMP-LA-{i:03d}" for i in range(1, 15)]
    registers = [f"REG-{i:02d}" for i in range(1, 7)]
    transaction_counter = 1

    print(f"   Generating LA transactions for {len(store_products)} SKUs...")

    for day in range(num_days):
        current_date = end_date - timedelta(days=num_days - day - 1)
        is_weekend = current_date.weekday() >= 5
        num_transactions = random.randint(
            TRANSACTIONS_PER_DAY_LA[0] if not is_weekend else int(TRANSACTIONS_PER_DAY_LA[0] * 1.4),
            TRANSACTIONS_PER_DAY_LA[1] if not is_weekend else int(TRANSACTIONS_PER_DAY_LA[1] * 1.4)
        )

        for _ in range(num_transactions):
            hour_weights = [0, 0, 0, 0, 0, 0, 0, 0, 0, 2, 3, 4, 6, 5, 4, 3, 4, 6, 7, 5, 3, 0, 0, 0]
            hour = random.choices(range(24), weights=hour_weights)[0]
            minute = random.randint(0, 59)
            second = random.randint(0, 59)
            transaction_time = current_date.replace(hour=hour, minute=minute, second=second)

            event_type = random.choices(['sale', 'return'], weights=[0.95, 0.05])[0]
            transaction_id = f"TXN-LA-{transaction_time.strftime('%Y%m%d')}-{transaction_counter:04d}"
            event_id = f"evt_{generate_object_id()}"

            num_items = random.choices(
                [1, 2, 3, 4, 5],
                weights=[0.4, 0.3, 0.2, 0.07, 0.03]
            )[0]

            selected_products = store_products.sample(n=num_items, weights='weight', replace=False)

            total_amount = 0
            for _, product in selected_products.iterrows():
                quantity_sold = random.choices([1, 2, 3], weights=[0.8, 0.15, 0.05])[0]
                if event_type == 'return':
                    quantity_sold = -quantity_sold

                unit_price = float(product['price'])
                discount = 0.0
                if random.random() < 0.1:
                    discount = round(unit_price * random.uniform(0.05, 0.15), 2)

                inventory_impact = -quantity_sold
                total_amount += (unit_price - discount) * abs(quantity_sold)

                transaction_items.append({
                    'event_id': event_id,
                    'sku': product['sku'],
                    'product_name': product['product_name'],
                    'quantity_sold': int(quantity_sold),
                    'unit_price': float(unit_price),
                    'discount': float(discount),
                    'inventory_impact': int(inventory_impact)
                })

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
            cashier_id = random.choice(cashiers)
            register_id = random.choice(registers)

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

    # Inventory adjustments
    num_adjustments = num_days * 10
    print(f"   Generating {num_adjustments} LA inventory adjustments...")
    for _ in range(num_adjustments):
        days_ago = random.randint(0, num_days - 1)
        adjustment_date = end_date - timedelta(days=days_ago)
        hour = random.choice([8, 21, 22])
        minute = random.randint(0, 59)
        adjustment_time = adjustment_date.replace(hour=hour, minute=minute, second=0)

        reason = random.choice([
            'damaged_goods',
            'theft',
            'physical_count_correction',
            'expired_product',
            'return_to_vendor'
        ])

        product = store_products.sample(n=1).iloc[0]

        if reason == 'physical_count_correction':
            adjustment_quantity = random.randint(-10, 15)
        else:
            adjustment_quantity = -random.randint(1, 8)

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
    print("=" * 60)
    print("ShopFast POS Systems Data Generator (Spark Manhattan + rolling JSON)")
    print("=" * 60)

    print(f"\n🔧 ADLS Configuration:")
    print(f"   Account: {ADLS_ACCOUNT_NAME}")
    print(f"   Filesystem: {ADLS_FILESYSTEM}")
    print(f"   Base Path: {ADLS_BASE_PATH}")

    # Load master products
    print("\n📦 Loading master products from ADLS...")
    df_products = load_master_products()
    print(f"✅ Loaded {len(df_products)} products")

    # Manhattan store (Spark)
    print(f"\n🏪 Generating Manhattan store inventory snapshots (REST API)...")
    print(f"   Simulating 1-minute polling for {NUM_DAYS} days...")
    manhattan_snapshots = generate_manhattan_inventory_snapshots(df_products, NUM_DAYS)
    print(f"✅ Generated {len(manhattan_snapshots)} inventory snapshots")

    # LA store (Python)
    print(f"\n🏪 Generating LA store transactions (Kafka stream)...")
    la_transactions, la_items, la_adjustments = generate_la_transactions(df_products, NUM_DAYS)
    print(f"✅ Generated {len(la_transactions)} transactions")
    print(f"✅ Generated {len(la_adjustments)} inventory adjustments")

    # Save Manhattan snapshots by day
    print("\n💾 Saving Manhattan store data to ADLS (rolling append)...")
    snapshots_by_day = {}
    for snapshot in manhattan_snapshots:
        day_key = snapshot['timestamp'][:10].replace('-', '')
        snapshots_by_day.setdefault(day_key, []).append(snapshot)

    for day_key, day_snapshots in snapshots_by_day.items():
        path_prefix = f"{OUTPUT_PATH}/manhattan/inventory_snapshots_{day_key}"
        written = write_json_rolling(path_prefix, day_snapshots, max_bytes=MAX_FILE_BYTES, zero_pad=ZERO_PAD_WIDTH)
        print(f"   ✅ {day_key}: {len(day_snapshots)} snapshots → {len(written)} file(s)")
        for p in written:
            print(f"      • {p}")

    # Save LA data
    print("\n💾 Saving LA store data to ADLS (rolling append)...")

    # Transactions
    path_prefix_txn = f"{OUTPUT_PATH}/la/transactions"
    written_txn = write_json_rolling(path_prefix_txn, la_transactions, max_bytes=MAX_FILE_BYTES, zero_pad=ZERO_PAD_WIDTH)
    print(f"   ✅ LA transactions → {len(written_txn)} file(s)")
    for p in written_txn:
        print(f"      • {p}")

    # Transaction items
    path_prefix_items = f"{OUTPUT_PATH}/la/transaction_items"
    written_items = write_json_rolling(path_prefix_items, la_items, max_bytes=MAX_FILE_BYTES, zero_pad=ZERO_PAD_WIDTH)
    print(f"   ✅ LA transaction_items → {len(written_items)} file(s)")
    for p in written_items:
        print(f"      • {p}")

    # Adjustments
    path_prefix_adj = f"{OUTPUT_PATH}/la/inventory_adjustments"
    written_adj = write_json_rolling(path_prefix_adj, la_adjustments, max_bytes=MAX_FILE_BYTES, zero_pad=ZERO_PAD_WIDTH)
    print(f"   ✅ LA inventory_adjustments → {len(written_adj)} file(s)")
    for p in written_adj:
        print(f"      • {p}")

    # Summary
    print("\n" + "=" * 60)
    print("POS SYSTEMS DATA SUMMARY")
    print("=" * 60)

    print(f"\nManhattan Store (STORE-NYC-01):")
    print(f"  Data Type: REST API responses (JSON)")
    print(f"  Polling Frequency: Every 1 minute")
    print(f"  Total Snapshots: {len(manhattan_snapshots)}")
    print(f"  Date Range: {NUM_DAYS} days")

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
    print(f"  Total Transactions: {len(la_transactions)}")
    sales = [t for t in la_transactions if t['event_type'] == 'sale']
    returns = [t for t in la_transactions if t['event_type'] == 'return']
    print(f"    Sales: {len(sales)}")
    print(f"    Returns: {len(returns)}")
    total_revenue = sum(t['payment_amount'] for t in sales)
    avg_transaction = total_revenue / len(sales) if sales else 0
    print(f"  Total Revenue: ${total_revenue:,.2f}")
    print(f"  Average Transaction: ${avg_transaction:.2f}")

    payment_methods = {}
    for t in la_transactions:
        pm = t['payment_method']
        payment_methods[pm] = payment_methods.get(pm, 0) + 1

    print(f"\n  Payment Methods:")
    for method, count in sorted(payment_methods.items()):
        print(f"    {method}: {count}")

    print(f"\n  Inventory Adjustments: {len(la_adjustments)}")
    reasons = {}
    for adj in la_adjustments:
        reason = adj['reason']
        reasons[reason] = reasons.get(reason, 0) + 1
    print(f"    Reasons:")
    for reason, count in sorted(reasons.items()):
        print(f"      {reason}: {count}")

    print("\n" + "=" * 60)
    print("✅ POS systems data generation complete! (Spark Manhattan, append mode)")
    print("=" * 60)
    print("\n📁 Output Location (ADLS):")
    print(f"  {OUTPUT_PATH}")
    print("=" * 60)

# ============================================
# RUN THE GENERATOR
# ============================================
if __name__ == "__main__":
    main()
