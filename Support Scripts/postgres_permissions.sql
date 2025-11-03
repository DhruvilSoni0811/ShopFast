-- Create dedicated user for shopfast_fivetran
CREATE USER shopfast_fivetran WITH PASSWORD 'Logincred0811#';

-- Grant connection
GRANT CONNECT ON DATABASE defaultdb TO shopfast_fivetran;

-- Grant schema usage
GRANT USAGE ON SCHEMA public TO shopfast_fivetran;

-- Grant SELECT on all tables
GRANT SELECT ON ALL TABLES IN SCHEMA public TO shopfast_fivetran;

-- Grant SELECT on sequences
GRANT SELECT ON ALL SEQUENCES IN SCHEMA public TO shopfast_fivetran;

-- For future tables
ALTER DEFAULT PRIVILEGES IN SCHEMA public 
GRANT SELECT ON TABLES TO shopfast_fivetran;

ALTER DEFAULT PRIVILEGES IN SCHEMA public 
GRANT SELECT ON SEQUENCES TO shopfast_fivetran;

-- Grant REPLICATION (required for CDC)
ALTER USER shopfast_fivetran WITH REPLICATION;

SELECT tablename 
FROM pg_tables 
WHERE schemaname = 'public';

-- Create publication with explicit table list
CREATE PUBLICATION fivetran_publication FOR TABLE 
    public.web_inventory,
    public.web_orders,
    public.web_order_items,
	public.web_products;


-- Verify publication
SELECT * FROM pg_publication WHERE pubname = 'fivetran_publication';

-- Check which tables are in the publication
SELECT * FROM pg_publication_tables WHERE pubname = 'fivetran_publication';