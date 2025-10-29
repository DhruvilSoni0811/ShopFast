# Source Tables Description

## 1. Website (PostgreSQL)
  1. src_web_orders - Customer orders from e-commerce website - Source for order processing and inventory allocation
  2. src_web_order_items - Line items of web orders with SKU details - Drives inventory reservation and fulfillment
  3. src_web_inventory - Real-time inventory view on website - Shows what customers see as "in stock"
  4. src_web_products - Master product catalog with pricing/suppliers - Source of truth for product attributes

## 2. Mobile App (MongoDB)
  1. src_app_orders - Denormalized mobile orders with nested data - Mobile channel order source
  2. src_app_order_items - Flattened mobile order line items - Extracted from nested MongoDB arrays
  3. src_app_cart_events - Real-time cart add/remove events - Tracks temporary inventory holds
  4. src_app_inventory_sync - Mobile app's cached inventory view - Synced every 30 min from warehouses

## 3. Warehouses (CSV Files)
  1. src_wh_east_inventory - East warehouse daily export (comma-delimeted) - Physical inventory counts, arrives 6-8 hrs late
  2. src_wh_west_inventory - West warehouse daily export (pipe-delimeted) - Different schema, requires harmonization
  3. src_wh_central_inventory - Central warehouse export (most detailed) - Includes damaged goods and in-transit quantities

## 4. Retail Stores (POS)
  1. src_pos_manhattan_inventory - Manhattan store inventory via REST API - Polled every 5 minutes for near real-time data
  2. src_pos_la_transactions - LA store transaction stream from KAFKA - Real-time sales events as they happen
  3. src_pos_la_transaction_items - Line items from LA transactions - Individual SKUs sold in each transaction
  4. src_pos_la_inventory_adjustments - Manual inventory adjustements (damage, theft) - Records reasons for inventory discrepancies

