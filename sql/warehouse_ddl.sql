-- 1. Dimension: Customers
CREATE TABLE IF NOT EXISTS dim_customers (
    customer_id VARCHAR PRIMARY KEY,
    customer_unique_id VARCHAR,
    customer_zip_code_prefix VARCHAR,
    customer_city VARCHAR,
    customer_state VARCHAR
);

-- 2. Dimension: Products
CREATE TABLE IF NOT EXISTS dim_products (
    product_id VARCHAR PRIMARY KEY,
    product_category_name VARCHAR,
    product_name_lenght INT,
    product_description_lenght INT,
    product_photos_qty INT,
    product_weight_g NUMERIC,
    product_length_cm NUMERIC,
    product_height_cm NUMERIC,
    product_width_cm NUMERIC
);

-- 3. Dimension: Sellers
CREATE TABLE IF NOT EXISTS dim_sellers (
    seller_id VARCHAR PRIMARY KEY,
    seller_zip_code_prefix VARCHAR,
    seller_city VARCHAR,
    seller_state VARCHAR
);

-- 4. Fact Table: Order Items (The core transactional grain)
CREATE TABLE IF NOT EXISTS fact_order_items (
    order_id VARCHAR,
    order_item_id INT,
    customer_id VARCHAR REFERENCES dim_customers(customer_id),
    product_id VARCHAR REFERENCES dim_products(product_id),
    seller_id VARCHAR REFERENCES dim_sellers(seller_id),
    payment_type VARCHAR,
    payment_sequential INT,
    payment_installments INT,
    price NUMERIC,
    freight_value NUMERIC,
    payment_value NUMERIC,
    shipping_limit_date TIMESTAMP,
    order_purchase_timestamp TIMESTAMP,
    order_approved_at TIMESTAMP,
    order_delivered_carrier_date TIMESTAMP,
    order_delivered_customer_date TIMESTAMP,
    order_estimated_delivery_date TIMESTAMP,
    order_status VARCHAR,
    ds DATE,
    PRIMARY KEY (order_id, order_item_id, payment_sequential, ds)
);