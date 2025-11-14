-- =========================================
-- FIVETRAN POSTGRESQL CONFIGURATION SCRIPT
-- PostgreSQL to Databricks via Fivetran
-- =========================================

-- Step 1: Create dedicated Fivetran user
CREATE USER fivetran WITH PASSWORD 'Logincred0811#';

-- Step 2: Grant connection privilege
GRANT CONNECT ON DATABASE shopfast TO fivetran;

-- Step 3: Grant usage on schema (replace 'public' with your schema name if different)
GRANT USAGE ON SCHEMA public TO fivetran;

-- Step 4: Grant SELECT on specific tables
GRANT SELECT ON TABLE web_products TO fivetran;
GRANT SELECT ON TABLE web_orders TO fivetran;
GRANT SELECT ON TABLE web_order_items TO fivetran;
GRANT SELECT ON TABLE web_inventory TO fivetran;

-- Step 5: Grant SELECT on all future tables in schema (optional but recommended)
ALTER DEFAULT PRIVILEGES IN SCHEMA public
GRANT SELECT ON TABLES TO fivetran;

-- Step 6: Enable logical replication (REQUIRED for incremental sync)
-- This allows Fivetran to capture changes efficiently
ALTER TABLE web_products REPLICA IDENTITY FULL;
ALTER TABLE web_orders REPLICA IDENTITY FULL;
ALTER TABLE web_order_items REPLICA IDENTITY FULL;
ALTER TABLE web_inventory REPLICA IDENTITY FULL;

-- Step 7: Create replication slot for Fivetran (if using log-based incremental)
DO $$
BEGIN
    IF (SELECT setting FROM pg_settings WHERE name = 'wal_level') != 'logical' THEN
        RAISE EXCEPTION 'wal_level must be set to "logical" in postgresql.conf. Current value: %', 
            (SELECT setting FROM pg_settings WHERE name = 'wal_level');
    END IF;
END $$;

-- Run this only if you want log-based incremental replication
SELECT pg_create_logical_replication_slot('fivetran_slot', 'pgoutput');

-- Step 8: Grant replication privileges to fivetran user
ALTER USER fivetran WITH REPLICATION;

-- Step 9: Verify configuration
-- Check user permissions
SELECT grantee, privilege_type
FROM information_schema.table_privileges
WHERE grantee = 'fivetran';

-- Check replication slots
SELECT slot_name, slot_type, active
FROM pg_replication_slots
WHERE slot_name = 'fivetran_slot';

-- =========================================
-- POSTGRESQL.CONF REQUIREMENTS
-- =========================================
-- Ensure these settings are in postgresql.conf (requires restart):
-- 
-- wal_level = logical
-- max_replication_slots = 5  (or higher)
-- max_wal_senders = 5  (or higher)
-- 
-- To check current settings:
SHOW wal_level;
SHOW max_replication_slots;
SHOW max_wal_senders;

-- =========================================
-- PG_HBA.CONF REQUIREMENTS
-- =========================================
-- Add this line to pg_hba.conf to allow Fivetran IP addresses:
-- 
-- host    your_database_name    fivetran    0.0.0.0/0    md5
-- 
-- Or for specific Fivetran IPs (recommended):
-- host    your_database_name    fivetran    52.0.2.4/32    md5
-- 
-- After editing, reload PostgreSQL: SELECT pg_reload_conf();

-- =========================================
-- VERIFICATION QUERIES
-- =========================================

-- Test connection as fivetran user (run in new session)
-- psql -U fivetran -d your_database_name -h your_host

-- Verify table access
SELECT COUNT(*) FROM web_products;
SELECT COUNT(*) FROM web_orders;
SELECT COUNT(*) FROM web_order_items;
SELECT COUNT(*) FROM web_inventory;

-- =========================================
-- FIVETRAN CONNECTOR SETUP INFORMATION
-- =========================================
-- After running this script, use these values in Fivetran:
-- 
-- Host: your_postgres_host
-- Port: 5432 (default)
-- Database: your_database_name
-- User: fivetran
-- Password: your_secure_password_here
-- Schema: public (or your schema name)
-- 
-- Sync Mode: Log-based Incremental (recommended)
-- Replication Slot: fivetran_slot
-- Publication: Not needed for pgoutput
-- 
-- Tables to sync:
-- - web_products
-- - web_orders
-- - web_order_items
-- - web_inventory
-- =========================================

-- =========================================
-- CLEANUP (if you need to remove Fivetran setup)
-- =========================================
-- SELECT pg_drop_replication_slot('fivetran_slot');
-- DROP USER fivetran;
-- =========================================

SELECT slot_name, active FROM pg_replication_slots;


CREATE PUBLICATION fivetran_publication
FOR TABLE
    web_products,
    web_orders,
    web_order_items,
    web_inventory;
