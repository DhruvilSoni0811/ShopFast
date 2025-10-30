# Solution Architecture

## Bronze Layer:
1. Ingest from 5 different sources every 15 minutes
2. Kafka streams for real-time POS data
3. CDC (Change data capture) from PostgreSQL
4. API polling from MongoDB Atlas

## Silver Layer:
1. Reconcile inventory across all locations
2. Detect discrepancies (system vs actual stock)
3. Data quality checks (negative inventory, duplicate SKUs)
4. Calculate "available to promise" inventory

## Gold Layer:
1. Fact_Inventory_Snapshot: SKU, location, quantity, timestamp, reserved_qty
2. Fact_Sales_Velocity: SKU, rolling_7day_sales, predicted_stockout_date
3. Dim_Product: Enhanced with supplier_lead_time, reorder_point
4. Fact_Stackout_Events: Track every stockout with revenue_impact

## Advanced Features
1. Slowly Changing Dimensions (SCD Type 2) for product prices
2. Late-arriving data handling (warehouse reports arrive next day)
3. Real-time alerting: Trigger notifications when inventory < reorder_point
4. ML Integration: Predict demand spikes (holidays, promotions)

## Databricks (Unity Catalog, Delta Lake, AutoLoader)
1. Apache Kafka (real-time streams)
2. dbt (transformation orchestration)
3. Great Expectations (data quality)
4. Airflow (batch orchestration)