"""
Generate Website PostgreSQL Data
Creates orders, order_items, inventory, and products tables
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
import os
import json

# Configuration
INPUT_DIR = "GeneratedData"
OUTPUT_DIR = "GeneratedData/website"
NUM_DAYS = 30  # Generate 30 days of data
NUM_CUSTOMERS = 1000
ORDERS_PER_DAY_RANGE = (50, 150)  # Random orders per day

# Seed for reproducibility
random.seed(42)
np.random.seed(42)

def load_master_products():
    """Load master product catalog"""
    master_file = os.path.join(INPUT_DIR, 'master_products.csv')
    if not os.path.exists(master_file):
        raise FileNotFoundError(f"Master products file not found: {master_file}\nRun 01_generate_master_products.py first!")
    return pd.read_csv(master_file)

def generate_customers(num_customers):
    """Generate customer IDs"""
    return [f"CUST-{i:05d}" for i in range(10000, 10000 + num_customers)]

def generate_orders(df_products, customers, num_days):
    """Generate website orders with realistic patterns"""
    
    orders = []
    order_items = []
    order_id_counter = 1
    item_id_counter = 1
    
    # Get active products only
    active_products = df_products[df_products['is_active'] == True].copy()
    
    # Weight products by velocity for realistic sales
    velocity_weights = {
        'fast': 5,
        'medium': 2,
        'slow': 1
    }
    active_products['weight'] = active_products['velocity_category'].map(velocity_weights)
    
    end_date = datetime.now()
    
    for day in range(num_days):
        current_date = end_date - timedelta(days=num_days - day - 1)
        
        # More orders on weekends
        is_weekend = current_date.weekday() >= 5
        num_orders = random.randint(
            ORDERS_PER_DAY_RANGE[0] if not is_weekend else int(ORDERS_PER_DAY_RANGE[0] * 1.5),
            ORDERS_PER_DAY_RANGE[1] if not is_weekend else int(ORDERS_PER_DAY_RANGE[1] * 1.5)
        )
        
        for _ in range(num_orders):
            # Order timing - peak during business hours (10 AM - 8 PM)
            hour_weights = [1, 1, 1, 1, 1, 1, 1, 1, 2, 3, 4, 5, 5, 4, 4, 5, 6, 7, 6, 4, 3, 2, 1, 1]
            hour = random.choices(range(24), weights=hour_weights)[0]
            minute = random.randint(0, 59)
            second = random.randint(0, 59)
            
            order_time = current_date.replace(hour=hour, minute=minute, second=second)
            
            # Generate order
            order_id = f"WEB-{order_time.strftime('%Y%m%d')}-{order_id_counter:03d}"
            customer_id = random.choice(customers)
            
            # Order status distribution
            order_status = random.choices(
                ['confirmed', 'processing', 'shipped', 'delivered', 'cancelled'],
                weights=[0.15, 0.20, 0.25, 0.35, 0.05]  # 5% cancelled (the problem!)
            )[0]
            
            # Payment method
            payment_method = random.choices(
                ['credit_card', 'debit_card', 'paypal', 'apple_pay', 'google_pay'],
                weights=[0.45, 0.25, 0.15, 0.10, 0.05]
            )[0]
            
            # Number of items in order (1-5 items)
            num_items = random.choices([1, 2, 3, 4, 5], weights=[0.4, 0.3, 0.2, 0.07, 0.03])[0]
            
            # Select products for this order
            selected_products = active_products.sample(
                n=num_items,
                weights='weight',
                replace=False
            )
            
            # Generate order items
            total_amount = 0
            for _, product in selected_products.iterrows():
                quantity = random.choices([1, 2, 3, 4], weights=[0.6, 0.25, 0.10, 0.05])[0]
                unit_price = product['price']
                
                # Apply discount (20% chance)
                discount = 0
                if random.random() < 0.2:
                    discount = round(unit_price * random.uniform(0.05, 0.20), 2)
                
                # Fulfillment status
                if order_status == 'cancelled':
                    fulfillment_status = 'cancelled'
                elif order_status == 'confirmed':
                    fulfillment_status = 'pending'
                elif order_status == 'processing':
                    fulfillment_status = 'allocated'
                else:
                    fulfillment_status = 'fulfilled'
                
                # Warehouse allocation
                warehouse_allocated = random.choice([
                    'WH-EAST-01', 'WH-WEST-02', 'WH-CENTRAL-03'
                ]) if fulfillment_status in ['allocated', 'fulfilled'] else None
                
                order_items.append({
                    'order_item_id': item_id_counter,
                    'order_id': order_id,
                    'sku': product['sku'],
                    'product_name': product['product_name'],
                    'quantity': quantity,
                    'unit_price': unit_price,
                    'discount_applied': discount,
                    'fulfillment_status': fulfillment_status,
                    'warehouse_allocated': warehouse_allocated,
                    'created_at': order_time.strftime('%Y-%m-%d %H:%M:%S')
                })
                
                total_amount += (unit_price - discount) * quantity
                item_id_counter += 1
            
            # Create order record
            updated_time = order_time + timedelta(minutes=random.randint(1, 30))
            
            orders.append({
                'order_id': order_id,
                'customer_id': customer_id,
                'order_date': order_time.strftime('%Y-%m-%d %H:%M:%S'),
                'order_status': order_status,
                'total_amount': round(total_amount, 2),
                'payment_method': payment_method,
                'shipping_address_id': random.randint(1000, 9999),
                'created_at': order_time.strftime('%Y-%m-%d %H:%M:%S'),
                'updated_at': updated_time.strftime('%Y-%m-%d %H:%M:%S')
            })
            
            order_id_counter += 1
    
    return pd.DataFrame(orders), pd.DataFrame(order_items)

def generate_inventory(df_products, df_order_items):
    """Generate current inventory based on products and orders"""
    
    inventory = []
    inventory_id_counter = 1
    
    # Calculate reserved quantities from pending/processing orders
    reserved_by_sku = df_order_items[
        df_order_items['fulfillment_status'].isin(['pending', 'allocated'])
    ].groupby('sku')['quantity'].sum().to_dict()
    
    for _, product in df_products.iterrows():
        if not product['is_active']:
            continue
        
        # Base inventory based on velocity
        if product['velocity_category'] == 'fast':
            base_qty = random.randint(100, 300)
        elif product['velocity_category'] == 'medium':
            base_qty = random.randint(50, 150)
        else:
            base_qty = random.randint(20, 80)
        
        # Reserved quantity
        reserved_qty = reserved_by_sku.get(product['sku'], 0)
        
        # Add some variability and potential stockouts
        if random.random() < 0.05:  # 5% chance of stockout
            available_qty = 0
        elif random.random() < 0.10:  # 10% chance of low stock
            available_qty = random.randint(1, product['reorder_point'])
        else:
            available_qty = base_qty
        
        inventory.append({
            'inventory_id': inventory_id_counter,
            'sku': product['sku'],
            'available_qty': available_qty,
            'reserved_qty': int(reserved_qty),
            'last_updated': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        })
        
        inventory_id_counter += 1
    
    return pd.DataFrame(inventory)

def generate_products_table(df_products):
    """Generate products table for PostgreSQL"""
    
    products = df_products[df_products['is_active'] == True].copy()
    
    # Select only columns needed for PostgreSQL products table
    products_table = products[[
        'product_id', 'sku', 'product_name', 'category', 'subcategory',
        'price', 'cost', 'supplier_id', 'reorder_point', 'lead_time_days',
        'is_active', 'created_at'
    ]].copy()
    
    return products_table

def main():
    """Main execution function"""
    print("=" * 60)
    print("ShopFast Website PostgreSQL Data Generator")
    print("=" * 60)
    
    # Create output directory
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    # Load master products
    print("\n📦 Loading master products...")
    df_products = load_master_products()
    print(f"✅ Loaded {len(df_products)} products")
    
    # Generate customers
    print(f"\n👥 Generating {NUM_CUSTOMERS} customers...")
    customers = generate_customers(NUM_CUSTOMERS)
    print(f"✅ Generated {len(customers)} customer IDs")
    
    # Generate orders
    print(f"\n🛒 Generating {NUM_DAYS} days of orders...")
    df_orders, df_order_items = generate_orders(df_products, customers, NUM_DAYS)
    print(f"✅ Generated {len(df_orders)} orders with {len(df_order_items)} line items")
    
    # Generate inventory
    print("\n📊 Generating inventory snapshot...")
    df_inventory = generate_inventory(df_products, df_order_items)
    print(f"✅ Generated inventory for {len(df_inventory)} SKUs")
    
    # Generate products table
    print("\n📋 Preparing products table...")
    df_products_table = generate_products_table(df_products)
    print(f"✅ Prepared {len(df_products_table)} active products")
    
    # Save to CSV files
    print("\n💾 Saving data files...")
    df_orders.to_csv(os.path.join(OUTPUT_DIR, 'src_web_orders.csv'), index=False)
    df_order_items.to_csv(os.path.join(OUTPUT_DIR, 'src_web_order_items.csv'), index=False)
    df_inventory.to_csv(os.path.join(OUTPUT_DIR, 'src_web_inventory.csv'), index=False)
    df_products_table.to_csv(os.path.join(OUTPUT_DIR, 'src_web_products.csv'), index=False)
    
    print(f"✅ Saved all files to: {OUTPUT_DIR}")
    
    # Print summary statistics
    print("\n" + "=" * 60)
    print("WEBSITE DATA SUMMARY")
    print("=" * 60)
    
    print(f"\nOrders Statistics:")
    print(f"  Total Orders: {len(df_orders)}")
    print(f"  Date Range: {df_orders['order_date'].min()} to {df_orders['order_date'].max()}")
    print(f"\n  Order Status Distribution:")
    print(df_orders['order_status'].value_counts())
    
    cancelled_orders = df_orders[df_orders['order_status'] == 'cancelled']
    cancellation_rate = len(cancelled_orders) / len(df_orders) * 100
    print(f"\n  ⚠️  Cancellation Rate: {cancellation_rate:.1f}% ({len(cancelled_orders)} orders)")
    
    print(f"\n  Payment Methods:")
    print(df_orders['payment_method'].value_counts())
    
    print(f"\n  Total Revenue: ${df_orders['total_amount'].sum():,.2f}")
    print(f"  Average Order Value: ${df_orders['total_amount'].mean():.2f}")
    
    print(f"\nInventory Statistics:")
    stockouts = len(df_inventory[df_inventory['available_qty'] == 0])
    low_stock = len(df_inventory[df_inventory['available_qty'] <= df_inventory['available_qty'].quantile(0.1)])
    
    print(f"  Total SKUs: {len(df_inventory)}")
    print(f"  Stockouts: {stockouts} ({stockouts/len(df_inventory)*100:.1f}%)")
    print(f"  Low Stock: {low_stock} ({low_stock/len(df_inventory)*100:.1f}%)")
    print(f"  Total Reserved: {df_inventory['reserved_qty'].sum()} units")
    
    print("\n" + "=" * 60)
    print("✅ Website PostgreSQL data ready!")
    print("=" * 60)

if __name__ == "__main__":
    main()