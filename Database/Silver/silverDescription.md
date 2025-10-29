# Source Tables Description

## 1. Unified Orders
  1. silver_orders - Harmonized orders from web + mobile - Single view of all orders accross channels
  2. silver_order_items - Unified line items with standard schema - Consistent SKU-level order details

## 2. Unified Inventory
  1. silver_inventory_unified - Reconciled inventory across all 5 locations - Single source of truth for current inventory
  2. silver_inventory_discrepancies - Detected mismatches between sources - Flags conflicts for investigation

## 3. Product Master
  1. silver_products - Harmonized product catalog = Clean, validated product master data

## 4. Transaction Stream
  1. silver_transactions - Unified sales from web, app, and POS - All transactions in standard format
  2. silver_transactions_items - Unified line items with inventory impact - Shows how each sale affects inventory

## 5. Inventory Movements
  1. silver_inventory_movements - Complete audit trail of all movements - Sales, returns, transfers, adjustments

## 6. Data Quality
  1. silver_data_quality_checks - Results from Great Expectations test - Tracks data quality metrics over time 
  2. silver_late_arriving_data - Tracks warehouse data arriving 6-8 hrs late - Triggers reprocessing of affected records