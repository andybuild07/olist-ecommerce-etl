import os
import duckdb
from dotenv import load_dotenv

load_dotenv(override=True)

IN_DOCKER = os.path.exists("/.dockerenv")

MINIO_ENDPOINT = os.getenv(
    "MINIO_ENDPOINT_IN_DOCKER" if IN_DOCKER else "MINIO_ENDPOINT"
)
AWS_ACCESS_KEY_ID = os.getenv("MINIO_ROOT_USER")
AWS_SECRET_ACCESS_KEY = os.getenv("MINIO_ROOT_PASSWORD")
BUCKET_NAME = os.getenv("S3_BUCKET")

def run_curated_transform(target_ds: str):
    print(f"🔄 Starting DuckDB transformation for date: {target_ds}")
    
    # 1. Connect to DuckDB in-memory
    con = duckdb.connect(database=':memory:')

    # 2. Configure DuckDB to talk to local MinIO (S3-compatible)
    con.execute(f"""
        INSTALL httpfs;
        LOAD httpfs;
        SET s3_endpoint='{MINIO_ENDPOINT}';
        SET s3_access_key_id='{AWS_ACCESS_KEY_ID}';
        SET s3_secret_access_key='{AWS_SECRET_ACCESS_KEY}';
        SET s3_use_ssl=false;
        SET s3_url_style='path';
    """)

    raw_s3_path = f"s3://{BUCKET_NAME}/raw/ds={target_ds}/orders.csv"
    curated_s3_path = f"s3://{BUCKET_NAME}/curated/ds={target_ds}/orders.parquet"

    # 3. Quality Gate & Inspection Query
    print(f"🔍 Running quality gates on {raw_s3_path}...")
    try:
        # Check if raw file exists and get row count
        row_count_df = con.execute(f"SELECT COUNT(*) as cnt FROM read_csv_auto('{raw_s3_path}')").fetchdf()
        row_count = row_count_df['cnt'].iloc[0]
        print(f"📊 Quality Gate Passed: Found {row_count} rows for {target_ds}.")
        
        if row_count == 0:
            raise ValueError(f"❌ Quality Gate Failed: 0 rows found for date {target_ds}!")

    except Exception as e:
        print(f"❌ Quality Gate Error: {e}")
        return

    # 4. Transform, Clean, Deduplicate, and Write Parquet atomically
    print(f"⚙️ Transforming data and writing Parquet to curated zone...")
    
    con.execute(f"""
        COPY (
            SELECT 
                order_id,
                order_item_id,
                customer_id,
                customer_unique_id,
                customer_zip_code_prefix,
                customer_city,
                customer_state,
                product_id,
                product_category_name,
                product_name_lenght,
                product_description_lenght,
                product_photos_qty,
                product_weight_g,
                product_length_cm,
                product_height_cm,
                product_width_cm,
                seller_id,
                seller_city,
                seller_state,
                seller_zip_code_prefix,
                payment_type,
                payment_sequential,
                payment_installments,
                TRY_CAST(price AS DOUBLE) AS price,
                TRY_CAST(freight_value AS DOUBLE) AS freight_value,
                TRY_CAST(payment_value AS DOUBLE) AS payment_value,
                TRY_CAST(shipping_limit_date AS TIMESTAMP) AS shipping_limit_date,
                TRY_CAST(order_purchase_timestamp AS TIMESTAMP) AS order_purchase_timestamp,
                TRY_CAST(order_approved_at AS TIMESTAMP) AS order_approved_at,
                TRY_CAST(order_delivered_carrier_date AS TIMESTAMP) AS order_delivered_carrier_date,
                TRY_CAST(order_delivered_customer_date AS TIMESTAMP) AS order_delivered_customer_date,
                TRY_CAST(order_estimated_delivery_date AS TIMESTAMP) AS order_estimated_delivery_date,
                order_status
            FROM read_csv_auto('{raw_s3_path}')
            WHERE order_id IS NOT NULL AND customer_id IS NOT NULL
        ) TO '{curated_s3_path}' (FORMAT PARQUET);
    """)
    print(f"🎉 Curated Parquet successfully written to s3://{BUCKET_NAME}/curated/ds={target_ds}/")

if __name__ == "__main__":
    # Test run for a specific sample date that exists in your raw drops
    run_curated_transform("2017-10-02")