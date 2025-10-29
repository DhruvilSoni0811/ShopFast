# ShopFast
Implemented a real-time inventory synchronization pipeline for ShopFast by integrating data from PostgreSQL, MongoDB, CSVs, and POS systems using Databricks, Kafka, and Delta Lake. This solution reduced order cancellations from 15% to 3% and improved inventory sync latency from 2 hours to just 5 minutes, recovering $1.6M in annual revenue.

#Real-Time Inventory Sync: Solving the Overselling Problem

##Business Problem
ShopFast operates across:
1. Website (PostgreSQL)
2. Mobile App (MongoDB)
3. 3 Warehouses (CSV Exports)
4. 2 Retail Stores (POS Systems)

##Pain Point:
- Selling items that are out of stock -> 15% cancellation rate -> $2M annual revenue loss


