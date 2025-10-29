"""
Generate Mobile App MongoDB Data
Creates denormalized app_orders, cart_events, and inventory_sync collections
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
import os
import json
from bson import ObjectId

# Configuration
INPUT_DIR = "GeneratedData"
OUTPUT_DIR = "GeneratedData/mobile_app"
NUM_DAYS = 30
NUM_APP_CUSTOMERS = 800
ORDERS_PER_DAY_RANGE = (30, 100)  # Mobile has slightly fewer orders than web

# Seed for reproducibility
random.seed(43)
np.random.seed(43)

def generate_object_id():
    """Generate MongoDB-like ObjectId"""
    return str(ObjectId())

def load_master_products():
    """Load master product catalog"""
    master_file = os.path.join(INPUT_DIR, 'master_products.csv')
    return pd.read_csv(master_file)

def generate_app_customers(num_customers):
    """Generate mobile app customer data"""
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
    """Generate mobile app orders with denormalized structure"""
    
    orders = []
    order_items_flat = []
    order_id_counter = 1
    
    active_products = df_products[df_products['is_active'] == True].copy()
    
    # Weight by velocity
    velocity_weights = {'fast': 5, 'medium': 2, 'slow': 1}
    active_products['weight'] = active_products['velocity_category'].map(velocity_weights)
    
    end_date = datetime.now()
    
    # Mobile app versions and devices
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
            # Mobile orders peak in evening (6 PM - 11 PM) and lunch (12 PM - 2 PM)
            hour_weights = [1, 1, 1, 1, 1, 1, 1, 2, 3, 4, 4, 5, 6, 5, 4, 3, 4, 5, 7, 8, 7, 6, 4, 2]
            hour = random.choices(range(24), weights=hour_weights)[0]
            minute = random.randint(0, 59)
            second = random.randint(0, 59)
            
            order_time = current_date.replace(hour=hour, minute=minute, second=second)
            
            # Generate order
            order_id = f"APP-{order_time.strftime('%Y%m%d')}-{order_id_counter:03d}"
            customer = random.choice(customers)
            
            # Order status
            status = random.choices(
                ['processing', 'confirmed', 'shipped', 'delivered', 'cancelled'],
                weights=[0.25, 0.15, 0.20, 0.35, 0.05]
            )[0]
            
            # Number of items (mobile users tend to buy fewer items)
            num_items = random.choices([1, 2, 3], weights=[0.5, 0.35, 0.15])[0]
            
            # Select products
            selected_products = active_products.sample(n=num_items, weights='weight', replace=False)
            
            # Generate items
            items = []
            total_amount = 0
            
            for _, product in selected_products.iterrows():
                quantity = random.choices([1, 2, 3], weights=[0.7, 0.25, 0.05])[0]
                unit_price = product['price']
                
                # Size and color for fashion items
                size = random.choice(['XS', 'S', 'M', 'L', 'XL']) if product['category'] == 'Fashion' else None
                color = random.choice(['Black', 'White', 'Blue', 'Red', 'Gray']) if product['category'] in ['Fashion', 'Electronics'] else None
                
                item = {
                    'sku': product['sku'],
                    'product_name': product['product_name'],
                    'quantity': quantity,
                    'unit_price': float(unit_price),
                    'size': size,
                    'color': color
                }
                items.append(item)
                total_amount += unit_price * quantity
                
                # For flattened version
                order_items_flat.append({
                    '_id': generate_object_id(),
                    'order_id': order_id,
                    'sku': product['sku'],
                    'product_name': product['product_name'],
                    'quantity': quantity,
                    'unit_price': float(unit_price),
                    'size': size,
                    'color': color
                })
            
            # Payment method (mobile favors digital wallets)
            payment_method = random.choices(
                ['apple_pay', 'google_pay', 'credit_card', 'paypal'],
                weights=[0.35, 0.25, 0.25, 0.15]
            )[0]
            
            # Payment details
            payment = {
                'method': payment_method,
                'transaction_id': f"{payment_method.upper()[:3]}-TXN-{random.randint(100000, 999999)}",
                'amount': round(total_amount, 2),
                'currency': 'USD',
                'status': 'completed' if status != 'cancelled' else 'refunded'
            }
            
            # Shipping details
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
            
            # Promo code (30% of orders)
            promo_code = random.choice(['WINTER20', 'SAVE10', 'MOBILE15', 'WELCOME25', None, None, None])
            
            updated_time = order_time + timedelta(minutes=random.randint(1, 45))
            
            # Create denormalized order document
            order = {
                '_id': generate_object_id(),
                'order_id': order_id,
                'customer': {
                    'customer_id': customer['customer_id'],
                    'name': customer['name'],
                    'email': customer['email'],
                    'phone': customer['phone'],
                    'app_version': random.choice(app_versions),
                    'device_type': random.choice(device_types)
                },
                'order_date': order_time.isoformat(),
                'status': status,
                'items': items,
                'payment': payment,
                'shipping': shipping,
                'metadata': {
                    'created_at': order_time.isoformat(),
                    'updated_at': updated_time.isoformat(),
                    'session_id': f"sess_{generate_object_id()[:16]}",
                    'promo_code': promo_code
                }
            }
            
            orders.append(order)
            order_id_counter += 1
    
    return orders, order_items_flat

def generate_cart_events(df_products, customers, num_days):
    """Generate cart add/remove events"""
    
    events = []
    active_products = df_products[df_products['is_active'] == True].copy()
    end_date = datetime.now()
    
    # Generate ~5 cart events per day per 10 customers (cart abandonment!)
    num_events = num_days * len(customers) // 2
    
    for _ in range(num_events):
        # Random timestamp
        days_ago = random.randint(0, num_days - 1)
        event_date = end_date - timedelta(days=days_ago)
        hour = random.randint(8, 23)
        minute = random.randint(0, 59)
        second = random.randint(0, 59)
        event_time = event_date.replace(hour=hour, minute=minute, second=second)
        
        customer = random.choice(customers)
        product = active_products.sample(n=1).iloc[0]
        
        # Event type (more adds than removes)
        event_type = random.choices(['add_to_cart', 'remove_from_cart'], weights=[0.8, 0.2])[0]
        
        # Quantity
        quantity = random.choices([1, 2, 3], weights=[0.7, 0.2, 0.1])[0]
        
        # Inventory reservation (only for add_to_cart)
        inventory_reserved = event_type == 'add_to_cart' and random.random() > 0.3
        
        # Expiration (30 minutes for cart holds)
        expires_at = event_time + timedelta(minutes=30)
        
        event = {
            '_id': generate_object_id(),
            'event_type': event_type,
            'customer_id': customer['customer_id'],
            'session_id': f"sess_{generate_object_id()[:16]}",
            'sku': product['sku'],
            'quantity': quantity,
            'timestamp': event_time.isoformat(),
            'inventory_reserved': inventory_reserved,
            'expires_at': expires_at.isoformat()
        }
        
        events.append(event)
    
    return events

def generate_inventory_sync(df_products):
    """Generate mobile app's cached inventory view"""
    
    inventory_sync = []
    active_products = df_products[df_products['is_active'] == True].copy()
    
    # Last sync was ~15 minutes ago
    last_sync = datetime.now() - timedelta(minutes=random.randint(10, 20))
    
    for _, product in active_products.iterrows():
        # Cached quantity (may be stale compared to actual inventory)
        if product['velocity_category'] == 'fast':
            base_qty = random.randint(80, 250)
        elif product['velocity_category'] == 'medium':
            base_qty = random.randint(40, 120)
        else:
            base_qty = random.randint(15, 70)
        
        # 7% chance of showing out of stock
        if random.random() < 0.07:
            available_qty = 0
            display_status = 'out_of_stock'
        elif random.random() < 0.12:
            available_qty = random.randint(1, 5)
            display_status = 'low_stock'
        else:
            available_qty = base_qty
            display_status = 'in_stock'
        
        # Source warehouse
        source_warehouse = random.choice(['WH-EAST-01', 'WH-WEST-02', 'WH-CENTRAL-03'])
        
        # Estimated delivery
        estimated_delivery = (datetime.now() + timedelta(days=random.randint(2, 5))).date().isoformat()
        
        inventory_sync.append({
            '_id': generate_object_id(),
            'sku': product['sku'],
            'available_quantity': available_qty,
            'display_status': display_status,
            'last_synced': last_sync.isoformat(),
            'source_warehouse': source_warehouse,
            'estimated_delivery': estimated_delivery
        })
    
    return inventory_sync

def main():
    """Main execution function"""
    print("=" * 60)
    print("ShopFast Mobile App MongoDB Data Generator")
    print("=" * 60)
    
    # Create output directory
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    # Load master products
    print("\n📦 Loading master products...")
    df_products = load_master_products()
    print(f"✅ Loaded {len(df_products)} products")
    
    # Generate customers
    print(f"\n👥 Generating {NUM_APP_CUSTOMERS} mobile app customers...")
    customers = generate_app_customers(NUM_APP_CUSTOMERS)
    print(f"✅ Generated {len(customers)} customers")
    
    # Generate orders
    print(f"\n📱 Generating {NUM_DAYS} days of mobile app orders...")
    orders, order_items_flat = generate_app_orders(df_products, customers, NUM_DAYS)
    print(f"✅ Generated {len(orders)} orders")
    
    # Generate cart events
    print("\n🛒 Generating cart events...")
    cart_events = generate_cart_events(df_products, customers, NUM_DAYS)
    print(f"✅ Generated {len(cart_events)} cart events")
    
    # Generate inventory sync
    print("\n📊 Generating inventory sync cache...")
    inventory_sync = generate_inventory_sync(df_products)
    print(f"✅ Generated inventory sync for {len(inventory_sync)} SKUs")
    
    # Save to JSON files (MongoDB format)
    print("\n💾 Saving data files...")
    
    with open(os.path.join(OUTPUT_DIR, 'src_app_orders.json'), 'w') as f:
        json.dump(orders, f, indent=2)
    
    with open(os.path.join(OUTPUT_DIR, 'src_app_order_items.json'), 'w') as f:
        json.dump(order_items_flat, f, indent=2)
    
    with open(os.path.join(OUTPUT_DIR, 'src_app_cart_events.json'), 'w') as f:
        json.dump(cart_events, f, indent=2)
    
    with open(os.path.join(OUTPUT_DIR, 'src_app_inventory_sync.json'), 'w') as f:
        json.dump(inventory_sync, f, indent=2)
    
    print(f"✅ Saved all files to: {OUTPUT_DIR}")
    
    # Print summary statistics
    print("\n" + "=" * 60)
    print("MOBILE APP DATA SUMMARY")
    print("=" * 60)
    
    print(f"\nOrders Statistics:")
    print(f"  Total Orders: {len(orders)}")
    
    status_counts = {}
    for order in orders:
        status = order['status']
        status_counts[status] = status_counts.get(status, 0) + 1
    
    print(f"\n  Order Status Distribution:")
    for status, count in sorted(status_counts.items()):
        print(f"    {status}: {count}")
    
    cancelled = status_counts.get('cancelled', 0)
    cancellation_rate = cancelled / len(orders) * 100
    print(f"\n  ⚠️  Cancellation Rate: {cancellation_rate:.1f}% ({cancelled} orders)")
    
    payment_methods = {}
    device_types = {}
    for order in orders:
        pm = order['payment']['method']
        payment_methods[pm] = payment_methods.get(pm, 0) + 1
        dt = order['customer']['device_type']
        device_types[dt] = device_types.get(dt, 0) + 1
    
    print(f"\n  Payment Methods:")
    for method, count in sorted(payment_methods.items()):
        print(f"    {method}: {count}")
    
    print(f"\n  Device Types:")
    for device, count in sorted(device_types.items()):
        print(f"    {device}: {count}")
    
    total_revenue = sum(order['payment']['amount'] for order in orders)
    avg_order = total_revenue / len(orders)
    print(f"\n  Total Revenue: ${total_revenue:,.2f}")
    print(f"  Average Order Value: ${avg_order:.2f}")
    
    print(f"\nCart Events:")
    event_types = {}
    for event in cart_events:
        et = event['event_type']
        event_types[et] = event_types.get(et, 0) + 1
    
    for event_type, count in sorted(event_types.items()):
        print(f"  {event_type}: {count}")
    
    reserved_count = sum(1 for e in cart_events if e['inventory_reserved'])
    print(f"  Inventory Reserved: {reserved_count}")
    
    print(f"\nInventory Sync:")
    status_counts = {}
    for inv in inventory_sync:
        status = inv['display_status']
        status_counts[status] = status_counts.get(status, 0) + 1
    
    for status, count in sorted(status_counts.items()):
        print(f"  {status}: {count}")
    
    print("\n" + "=" * 60)
    print("✅ Mobile app MongoDB data ready!")
    print("=" * 60)

if __name__ == "__main__":
    main()