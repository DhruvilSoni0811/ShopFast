"""
Generate Master Product Catalog
Creates the foundational SKU list and writes to Azure ADLS Gen2
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
import os
from azure.storage.filedatalake import DataLakeServiceClient
from dotenv import load_dotenv
load_dotenv("../.env.development.local")

# ADLS Gen 2 Configuration
STORAGE_ACCOUNT_NAME = os.getenv("ADLS_ACCOUNT_NAME")
STORAGE_ACCOUNT_KEY = os.getenv("ADLS_ACCOUNT_KEY")
CONTAINER_NAME = "shopfast-raw-data"
BASE_PATH = "master_data"

# Configuration
OUTPUT_DIR = "GeneratedData"  # Local backup
NUM_PRODUCTS = 250

# Seed for reproducibility
random.seed(42)
np.random.seed(42)

def get_adls_client():
    """Initialize Azure Data Lake Storage Gen2 client"""
    try:
        # Method 1: Using Account Key
        service_client = DataLakeServiceClient(
            account_url=f"https://{STORAGE_ACCOUNT_NAME}.dfs.core.windows.net",
            credential=STORAGE_ACCOUNT_KEY
        )
        
        return service_client
    except Exception as e:
        print(f"❌ Error connecting to ADLS: {str(e)}")
        raise

def upload_to_adls(df, file_name, container_name, folder_path):
    """
    Upload DataFrame to Azure ADLS Gen2 as CSV
    
    Parameters:
    - df: pandas DataFrame to upload
    - file_name: name of the file (e.g., 'master_products.csv')
    - container_name: ADLS container/filesystem name
    - folder_path: folder path within container (e.g., 'master_data')
    """
    try:
        # Get ADLS client
        service_client = get_adls_client()
        
        # Get filesystem (container) client
        file_system_client = service_client.get_file_system_client(file_system=container_name)
        
        # Create container if it doesn't exist
        try:
            file_system_client.create_file_system()
            print(f"✅ Created container: {container_name}")
        except Exception as e:
            if "ContainerAlreadyExists" in str(e) or "FilesystemAlreadyExists" in str(e):
                print(f"ℹ️  Container already exists: {container_name}")
            else:
                print(f"⚠️  Error creating container: {str(e)}")
        
        # Create directory client
        directory_client = file_system_client.get_directory_client(folder_path)
        
        # Create directory if it doesn't exist
        try:
            directory_client.create_directory()
            print(f"✅ Created directory: {folder_path}")
        except Exception as e:
            if "PathAlreadyExists" in str(e):
                print(f"ℹ️  Directory already exists: {folder_path}")
            else:
                print(f"⚠️  Error creating directory: {str(e)}")
        
        # Get file client
        file_client = directory_client.get_file_client(file_name)
        
        # Convert DataFrame to CSV string
        csv_data = df.to_csv(index=False)
        
        # Upload file (overwrite if exists)
        file_client.upload_data(csv_data, overwrite=True)
        
        # Construct the full ADLS path
        adls_path = f"abfss://{container_name}@{STORAGE_ACCOUNT_NAME}.dfs.core.windows.net/{folder_path}/{file_name}"
        
        print(f" Uploaded to ADLS: {adls_path}")
        print(f"   File size: {len(csv_data)} bytes")
        
        return adls_path
        
    except Exception as e:
        print(f"❌ Error uploading to ADLS: {str(e)}")
        raise

def generate_master_products():
    """Generate master product catalog with realistic attributes"""
    
    # Product categories and subcategories
    categories = {
        'Electronics': {
            'subcategories': ['Computer Accessories', 'Audio', 'Cameras', 'Gaming', 'Wearables'],
            'price_range': (15, 500),
            'cost_margin': 0.45,
            'lead_time': (5, 14)
        },
        'Fashion': {
            'subcategories': ['T-Shirts', 'Jeans', 'Dresses', 'Shoes', 'Accessories'],
            'price_range': (10, 150),
            'cost_margin': 0.35,
            'lead_time': (7, 21)
        },
        'Home': {
            'subcategories': ['Kitchen', 'Bedroom', 'Bath', 'Decor', 'Storage'],
            'price_range': (8, 200),
            'cost_margin': 0.40,
            'lead_time': (10, 30)
        },
        'Sports': {
            'subcategories': ['Fitness', 'Outdoor', 'Team Sports', 'Water Sports', 'Yoga'],
            'price_range': (12, 300),
            'cost_margin': 0.42,
            'lead_time': (7, 20)
        },
        'Beauty': {
            'subcategories': ['Skincare', 'Makeup', 'Haircare', 'Fragrance', 'Tools'],
            'price_range': (5, 120),
            'cost_margin': 0.30,
            'lead_time': (5, 15)
        }
    }
    
    # Product name templates
    product_templates = {
        'Electronics': {
            'Computer Accessories': ['Wireless Mouse', 'Keyboard', 'USB Hub', 'Webcam', 'Mouse Pad'],
            'Audio': ['Bluetooth Speaker', 'Headphones', 'Earbuds', 'Microphone', 'Sound Bar'],
            'Cameras': ['Digital Camera', 'Action Camera', 'Tripod', 'Camera Bag', 'Lens Kit'],
            'Gaming': ['Gaming Mouse', 'Controller', 'Headset', 'Keyboard', 'Chair'],
            'Wearables': ['Fitness Tracker', 'Smart Watch', 'Activity Band', 'Heart Rate Monitor', 'Sleep Tracker']
        },
        'Fashion': {
            'T-Shirts': ['Cotton T-Shirt', 'V-Neck Tee', 'Graphic Tee', 'Long Sleeve Shirt', 'Tank Top'],
            'Jeans': ['Denim Jeans', 'Skinny Jeans', 'Boot Cut Jeans', 'Relaxed Fit Jeans', 'Distressed Jeans'],
            'Dresses': ['Summer Dress', 'Evening Gown', 'Casual Dress', 'Maxi Dress', 'Cocktail Dress'],
            'Shoes': ['Running Shoes', 'Sneakers', 'Boots', 'Sandals', 'Loafers'],
            'Accessories': ['Belt', 'Scarf', 'Hat', 'Sunglasses', 'Wallet']
        },
        'Home': {
            'Kitchen': ['Coffee Mug', 'Cutting Board', 'Mixing Bowl', 'Storage Container', 'Utensil Set'],
            'Bedroom': ['Pillow', 'Bed Sheet Set', 'Comforter', 'Throw Blanket', 'Mattress Pad'],
            'Bath': ['Towel Set', 'Shower Curtain', 'Bath Mat', 'Soap Dispenser', 'Storage Caddy'],
            'Decor': ['Wall Art', 'Picture Frame', 'Vase', 'Candle Holder', 'Decorative Pillow'],
            'Storage': ['Storage Bin', 'Organizer', 'Shelving Unit', 'Closet System', 'Basket']
        },
        'Sports': {
            'Fitness': ['Yoga Mat', 'Resistance Bands', 'Dumbbells', 'Exercise Ball', 'Jump Rope'],
            'Outdoor': ['Hiking Backpack', 'Camping Tent', 'Sleeping Bag', 'Water Bottle', 'Flashlight'],
            'Team Sports': ['Basketball', 'Soccer Ball', 'Volleyball', 'Baseball Glove', 'Tennis Racket'],
            'Water Sports': ['Swim Goggles', 'Snorkel Set', 'Life Jacket', 'Pool Float', 'Diving Mask'],
            'Yoga': ['Yoga Block', 'Yoga Strap', 'Meditation Cushion', 'Foam Roller', 'Exercise Mat']
        },
        'Beauty': {
            'Skincare': ['Face Cream', 'Cleanser', 'Serum', 'Moisturizer', 'Eye Cream'],
            'Makeup': ['Lipstick', 'Foundation', 'Mascara', 'Eyeshadow Palette', 'Blush'],
            'Haircare': ['Shampoo', 'Conditioner', 'Hair Mask', 'Styling Gel', 'Hair Oil'],
            'Fragrance': ['Perfume', 'Body Spray', 'Cologne', 'Room Spray', 'Scented Candle'],
            'Tools': ['Makeup Brush Set', 'Hair Dryer', 'Flat Iron', 'Curling Iron', 'Nail Kit']
        }
    }
    
    # Color and size variations
    colors = ['Black', 'White', 'Blue', 'Red', 'Green', 'Gray', 'Pink', 'Purple', 'Brown', 'Navy']
    sizes = ['XS', 'S', 'M', 'L', 'XL', 'XXL', 'One Size']
    
    # Suppliers
    suppliers = [
        'SUP-TECH-01', 'SUP-TECH-02', 'SUP-FASHION-01', 'SUP-FASHION-02',
        'SUP-HOME-01', 'SUP-SPORTS-01', 'SUP-BEAUTY-01', 'SUP-GLOBAL-01',
        'SUP-DIRECT-01', 'SUP-IMPORT-01'
    ]
    
    products = []
    sku_counter = 1
    
    # Distribute products across categories
    products_per_category = NUM_PRODUCTS // len(categories)
    
    for category, cat_info in categories.items():
        subcats = cat_info['subcategories']
        products_per_subcat = products_per_category // len(subcats)
        
        for subcategory in subcats:
            for i in range(products_per_subcat):
                # Generate SKU
                cat_code = category[:4].upper()
                subcat_code = subcategory[:2].upper()
                sku = f"{cat_code}-{subcat_code}-{sku_counter:03d}"
                
                # Select product template
                product_base = random.choice(product_templates[category][subcategory])
                
                # Add variations
                if category == 'Fashion':
                    color = random.choice(colors)
                    size = random.choice(sizes[:6])
                    product_name = f"{product_base} {color} {size}"
                elif category == 'Electronics':
                    color = random.choice(colors[:5])
                    product_name = f"{product_base} - {color}"
                else:
                    product_name = product_base
                
                # Generate price
                price_min, price_max = cat_info['price_range']
                price = round(random.uniform(price_min, price_max), 2)
                
                # Calculate cost
                cost = round(price * cat_info['cost_margin'], 2)
                
                # Generate supply chain attributes
                lead_time_min, lead_time_max = cat_info['lead_time']
                lead_time_days = random.randint(lead_time_min, lead_time_max)
                
                # Reorder point
                base_reorder = random.choice([30, 50, 75, 100, 150])
                reorder_point = base_reorder
                
                # Supplier
                supplier_id = random.choice(suppliers)
                
                # Product status
                is_active = random.random() > 0.05
                is_seasonal = category in ['Fashion', 'Sports'] and random.random() > 0.7
                
                # Created date
                days_ago = random.randint(0, 730)
                created_at = datetime.now() - timedelta(days=days_ago)
                
                products.append({
                    'product_id': sku_counter,
                    'sku': sku,
                    'product_name': product_name,
                    'category': category,
                    'subcategory': subcategory,
                    'price': price,
                    'cost': cost,
                    'supplier_id': supplier_id,
                    'reorder_point': reorder_point,
                    'lead_time_days': lead_time_days,
                    'is_active': is_active,
                    'is_seasonal': is_seasonal,
                    'created_at': created_at.strftime('%Y-%m-%d %H:%M:%S')
                })
                
                sku_counter += 1
    
    # Create DataFrame
    df_products = pd.DataFrame(products)
    
    # Add ABC classification
    df_products['abc_classification'] = pd.qcut(
        df_products['price'], 
        q=3, 
        labels=['C', 'B', 'A']
    )
    
    # Add velocity classification
    df_products['velocity_category'] = np.random.choice(
        ['fast', 'medium', 'slow'],
        size=len(df_products),
        p=[0.2, 0.5, 0.3]
    )
    
    return df_products

def main():
    """Main execution function"""
    print("=" * 70)
    print("ShopFast Master Product Catalog Generator")
    print("Target: Azure ADLS Gen2")
    print("=" * 70)
    
    # Create local backup directory
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    print(f"\n📦 Generating {NUM_PRODUCTS} products...")
    df_products = generate_master_products()
    print(f"✅ Generated {len(df_products)} products")
    
    # Save to local CSV (backup)
    print("\n💾 Saving local backup...")
    local_file = os.path.join(OUTPUT_DIR, 'master_products.csv')
    df_products.to_csv(local_file, index=False)
    print(f"✅ Local backup saved: {local_file}")
    
    # Upload to ADLS Gen2
    print("\n☁️  Uploading to Azure ADLS Gen2...")
    try:
        adls_path = upload_to_adls(
            df=df_products,
            file_name='master_products.csv',
            container_name=CONTAINER_NAME,
            folder_path=BASE_PATH
        )
        print(f"✅ Successfully uploaded to ADLS")
        print(f"   ADLS Path: {adls_path}")
    except Exception as e:
        print(f"❌ Failed to upload to ADLS: {str(e)}")
        print("   Local backup is available at:", local_file)
    
    # Print summary statistics
    print("\n" + "=" * 70)
    print("PRODUCT SUMMARY")
    print("=" * 70)
    print(f"\nProducts by Category:")
    print(df_products['category'].value_counts())
    
    print(f"\nPrice Range:")
    print(f"  Min: ${df_products['price'].min():.2f}")
    print(f"  Max: ${df_products['price'].max():.2f}")
    print(f"  Avg: ${df_products['price'].mean():.2f}")
    
    print(f"\nVelocity Distribution:")
    print(df_products['velocity_category'].value_counts())
    
    print(f"\nABC Classification:")
    print(df_products['abc_classification'].value_counts())
    
    print(f"\nActive Products: {df_products['is_active'].sum()} ({df_products['is_active'].mean()*100:.1f}%)")
    print(f"Seasonal Products: {df_products['is_seasonal'].sum()} ({df_products['is_seasonal'].mean()*100:.1f}%)")
    
    print("\n" + "=" * 70)
    print("✅ Master product catalog ready!")
    print("=" * 70)
    
    print("\n📍 Next Steps:")
    print("   1. Verify file in Azure Portal or Storage Explorer")
    print("   2. Create external table in Databricks/Synapse:")
    print(f"      CREATE EXTERNAL TABLE master_products")
    print(f"      LOCATION 'abfss://{CONTAINER_NAME}@{STORAGE_ACCOUNT_NAME}.dfs.core.windows.net/{BASE_PATH}'")
    print("   3. Run other data generators to populate source systems")
    print("=" * 70)

if __name__ == "__main__":
    main()