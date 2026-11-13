import os
import duckdb
from dotenv import load_dotenv

load_dotenv()

IN_DOCKER = os.path.exists("/.dockerenv")

MINIO_ENDPOINT = os.getenv(
    "MINIO_ENDPOINT_IN_DOCKER" if IN_DOCKER else "MINIO_ENDPOINT"
)
AWS_ACCESS_KEY_ID = os.getenv("MINIO_ROOT_USER")
AWS_SECRET_ACCESS_KEY = os.getenv("MINIO_ROOT_PASSWORD")
BUCKET_NAME = os.getenv("S3_BUCKET")

PG_HOST = os.getenv(
    "WAREHOUSE_HOST_IN_DOCKER" if IN_DOCKER else "WAREHOUSE_HOST"
)
PG_PORT =  os.getenv(
    "WAREHOUSE_PORT_IN_DOCKER" if IN_DOCKER else "WAREHOUSE_PORT"
)
PG_USER = os.getenv("WAREHOUSE_USER")
PG_PASSWORD = os.getenv("WAREHOUSE_PASSWORD")
PG_DB = os.getenv("WAREHOUSE_DB")

def run_star_schema_load(target_ds: str):
    print(f"📦 Starting Star Schema load for date: {target_ds} using native DuckDB")
    
    con = duckdb.connect(database=':memory:')

    # 1. Configure extensions (now unblocked!)
    con.execute(f"""
        INSTALL httpfs;
        LOAD httpfs;
        INSTALL postgres;
        LOAD postgres;

        SET s3_endpoint='{MINIO_ENDPOINT}';
        SET s3_access_key_id='{AWS_ACCESS_KEY_ID}';
        SET s3_secret_access_key='{AWS_SECRET_ACCESS_KEY}';
        SET s3_use_ssl=false;
        SET s3_url_style='path';
    """)

    # 2. Attach Postgres warehouse
    pg_conn_str = f"dbname={PG_DB} user={PG_USER} password={PG_PASSWORD} host={PG_HOST} port={PG_PORT}"
    con.execute(f"ATTACH '{pg_conn_str}' AS pg (TYPE POSTGRES);")

    # 3. Read and execute DDL from file
    ddl_path = "sql/warehouse_ddl.sql"
    if os.path.exists(ddl_path):
        print(f"🏗️ Executing DDL schema from {ddl_path}...")
        with open(ddl_path, "r") as f:
            ddl_sql = f.read()
        
        ddl_sql_namespaced = (
            ddl_sql.replace("CREATE TABLE IF NOT EXISTS ", "CREATE TABLE IF NOT EXISTS pg.public.")
                   .replace("REFERENCES dim_", "REFERENCES pg.public.dim_")
        )
        con.execute(ddl_sql_namespaced)

    fact_pk_columns = [
        row[0]
        for row in con.execute("""
            SELECT kcu.column_name
            FROM pg.information_schema.table_constraints AS tc
            JOIN pg.information_schema.key_column_usage AS kcu
              USING (constraint_catalog, constraint_schema, constraint_name, table_schema, table_name)
            WHERE tc.table_schema = 'public'
              AND tc.table_name = 'fact_order_items'
              AND tc.constraint_type = 'PRIMARY KEY'
            ORDER BY kcu.ordinal_position
        """).fetchall()
    ]
    expected_fact_pk_columns = ["order_id", "order_item_id", "payment_sequential", "ds"]
    if fact_pk_columns != expected_fact_pk_columns:
        con.execute("""
            SELECT * FROM postgres_execute(
                'pg',
                'ALTER TABLE public.fact_order_items
                 DROP CONSTRAINT IF EXISTS fact_order_items_pkey,
                 ADD CONSTRAINT fact_order_items_pkey
                 PRIMARY KEY (order_id, order_item_id, payment_sequential, ds)'
            )
        """).fetchall()

    curated_s3_path = f"s3://{BUCKET_NAME}/curated/ds={target_ds}/orders.parquet"

    # 4. Upsert Dimensions directly via SQL
    print(f"👥 Upserting dim_customers, dim_products, dim_sellers...")
    
    con.execute(f"""
        INSERT INTO pg.public.dim_customers (customer_id, customer_unique_id, customer_zip_code_prefix, customer_city, customer_state)
        SELECT DISTINCT customer_id, customer_unique_id, customer_zip_code_prefix, customer_city, customer_state
        FROM read_parquet('{curated_s3_path}')
        WHERE customer_id IS NOT NULL
        ON CONFLICT (customer_id) DO UPDATE SET
            customer_city = EXCLUDED.customer_city,
            customer_state = EXCLUDED.customer_state;
    """)

    con.execute(f"""
        INSERT INTO pg.public.dim_products (product_id, product_category_name, product_name_lenght, product_description_lenght, product_photos_qty, product_weight_g, product_length_cm, product_height_cm, product_width_cm)
        SELECT DISTINCT product_id, product_category_name, product_name_lenght, product_description_lenght, product_photos_qty, 
               CAST(product_weight_g AS NUMERIC), CAST(product_length_cm AS NUMERIC), CAST(product_height_cm AS NUMERIC), CAST(product_width_cm AS NUMERIC)
        FROM read_parquet('{curated_s3_path}')
        WHERE product_id IS NOT NULL
        ON CONFLICT (product_id) DO UPDATE SET
            product_category_name = EXCLUDED.product_category_name;
    """)

    con.execute(f"""
        INSERT INTO pg.public.dim_sellers (seller_id, seller_zip_code_prefix, seller_city, seller_state)
        SELECT DISTINCT seller_id, seller_zip_code_prefix, seller_city, seller_state
        FROM read_parquet('{curated_s3_path}')
        WHERE seller_id IS NOT NULL
        ON CONFLICT (seller_id) DO UPDATE SET
            seller_city = EXCLUDED.seller_city,
            seller_state = EXCLUDED.seller_state;
    """)

    # 5. Load Fact Table (Idempotent Delete + Insert)
    print(f"📊 Loading fact_order_items for ds={target_ds}...")
    con.execute(f"""
        DELETE FROM pg.public.fact_order_items WHERE ds = '{target_ds}';

        INSERT INTO pg.public.fact_order_items
        SELECT 
            order_id,
            order_item_id,
            customer_id,
            product_id,
            seller_id,
            payment_type,
            payment_sequential,
            payment_installments,
            CAST(price AS NUMERIC) AS price,
            CAST(freight_value AS NUMERIC) AS freight_value,
            CAST(payment_value AS NUMERIC) AS payment_value,
            shipping_limit_date,
            order_purchase_timestamp,
            order_approved_at,
            order_delivered_carrier_date,
            order_delivered_customer_date,
            order_estimated_delivery_date,
            order_status,
            CAST('{target_ds}' AS DATE) AS ds
        FROM read_parquet('{curated_s3_path}');
    """)

    print(f"🎉 Successfully loaded Star Schema for {target_ds} using native DuckDB compute!")

if __name__ == "__main__":
    run_star_schema_load("2017-10-02")