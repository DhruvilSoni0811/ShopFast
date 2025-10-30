#  Gold Layer Output Tables Description

## 1. Dimensions
  1. dim_product - Product dimension with SCD Type 2 - Tracks product attribute changes over time
  2. dim_product_price_history - Price changes with SCD Type 2 - Historical pricing for margin analysis
  3. dim_location - Warehouses and stores dimension - Geography and capacity attributes
  4. dim_date - Date dimension with business calender - Time-based analysis and holidays
  5. dim_customer - Customer dimension with segments - Customer analytics and lifetime value

## 2. Core Facts
  1. fact_inventory_snapshot - Point-In-Time inventory state - Daily inventory positions and ATP calculation
  2. fact_sales_transactions - All sales at line-item grain - Revenue, margin, and sales analysis
  3. fact_sales_velocity - Rolling sales velocity and forecasts - Predicts stockouts and recommends reorders
  4. fact_stockout_events - Every stockout with revenue impact - Tracks $2M annual loss problem
  5. fact_inventory_movements - Complete movement audit trail - Full traceablilty of inventory changes

## 3. Aggregates
  1. agg_daily_inventory_summary - Daily rollup by location - Fast dashboards without scanning facts
  2. agg_product_performance_monthly - Monthly product metrics and rankings - Executive reporting and trend analysis

## 4. Real-Time Alerts
  1. gold_inventory_alerts - Active alerts for low stock/stockouts - Powers real-time 
  2. gold_business_metrics - KPIs like cancellation rate and sync latency - Monitors solution effictiveness