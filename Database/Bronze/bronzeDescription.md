# Bronze Layer Output Tables Description

## 1. Website Bronze
  1. bronze_web_orders - Raw orders with CDC metadata - Tracks INSERT/UPDATE/DELETE operations
  2. bronze_web_order_items - Raw order items with CDC tracking - Change data capture from PostgreSQL
  3. bronze_web_inventory - Raw inventory snapshots with CDC - Real-time inventory updates
  4. bronze_web_products - Raw product catalog with CDC - Tracks product atrribute change

## 2. Mobile App Bronze
  1. bronze_app_orders - Raw MongoDB orders with resume tokens - MongoDB change stream ingestion
  2. bronze_app_order_items - Flattened items with parent reference - Array elements extracted to rows
  3. bronze_app_cart_events - Raw cart events from change stream - Temporary inventory hold tracking
  4. bronze_app_inventory_sync - Raw app inventory cache snapshots - 30-minute sync cycle tracking

## 3. Warehouse Bronze
  1. bronze_wh_east_inventory - Raw East warehouse CSV with file metadata - AutoLoader tracks file name and row number
  2. bronze_wh_west_inventory - Raw West warehouse CSV (pipe-delimited) - Schema differences preserved in bronze
  3. bronze_wh_central_inventory - Raw Central warehouse CSV (most detailed) - Complete warehouse snaphshot with costs

## 4. POS Bronze
  1. bronze_pos_manhattan_inventory - Raw Manhattan API responses with latency - Tracks API call metadata and timing
  2. bronze_pos_la_transactions - Raw Kafka transaction events with offsets - Real-time streams with Kafka metadata
  3. bronze_pos_la_transaction_items - Raw transaction items from Kafka - Line items with inventory impact
  4. bronze_pos_la_inventory_adjustments - Raw adjustment events from Kafka - Damage, theft, and correction events