CREATE TABLE web_products (
    product_id VARCHAR(50),
    sku VARCHAR(50),
    product_name TEXT,
    category TEXT,
    subcategory TEXT,
    price NUMERIC,
    cost NUMERIC,
    supplier_id VARCHAR(50),
    reorder_point INT,
    lead_time_days INT,
    is_active BOOLEAN,
    created_at TIMESTAMP
);

CREATE TABLE web_orders (
    order_id VARCHAR(50) PRIMARY KEY,
    customer_id VARCHAR(50),
    order_date TIMESTAMP,
    order_status VARCHAR(50),
    total_amount NUMERIC,
    payment_method VARCHAR(50),
    shipping_address_id INT,
    created_at TIMESTAMP,
    updated_at TIMESTAMP
);

CREATE TABLE web_order_items (
    order_item_id INT PRIMARY KEY,
    order_id VARCHAR(50),
    sku VARCHAR(50),
    product_name TEXT,
    quantity INT,
    unit_price NUMERIC,
    discount_applied NUMERIC,
    fulfillment_status VARCHAR(50),
    warehouse_allocated VARCHAR(50),
    created_at TIMESTAMP
);

CREATE TABLE web_inventory (
    inventory_id INT PRIMARY KEY,
    sku VARCHAR(50),
    available_qty INT,
    reserved_qty INT,
    last_updated TIMESTAMP
);


-- 🧩 Fix web_order_items
ALTER TABLE web_order_items
ALTER COLUMN order_item_id TYPE varchar(50) USING order_item_id::varchar;

-- 🧩 Fix web_inventory
ALTER TABLE web_inventory
ALTER COLUMN inventory_id TYPE varchar(50) USING inventory_id::varchar;

-- 🧩 Optional consistency: use VARCHAR for IDs everywhere
ALTER TABLE web_products
ALTER COLUMN product_id TYPE varchar(50) USING product_id::varchar;

ALTER TABLE web_orders
ALTER COLUMN order_id TYPE varchar(50) USING order_id::varchar;
