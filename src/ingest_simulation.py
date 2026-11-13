import os
import boto3
import pandas as pd
from dotenv import load_dotenv
from botocore.config import Config

# Load variables from the .env file in the root directory
load_dotenv()

# Read configurations securely from environment variables
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
AWS_ACCESS_KEY_ID = os.getenv("MINIO_ROOT_USER")
AWS_SECRET_ACCESS_KEY = os.getenv("MINIO_ROOT_PASSWORD")
BUCKET_NAME = os.getenv("S3_BUCKET", "olist-lake")

def get_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name="us-east-1", # MinIO requires a dummy region name
        config=Config(s3={"addressing_style": "path"}) # Crucial: forces path-style URLs to avoid localhost subdomain resolution errors
    )

def simulate_daily_drops():
    print("🚀 Starting daily drop simulation...")
    
    orders_path = "data/source/olist_orders_dataset.csv"
    if not os.path.exists(orders_path):
        print(f"❌ Error: Could not find {orders_path}. Make sure your CSVs are in data/source/!")
        return

    df = pd.read_csv(orders_path, engine="c")
    df["order_purchase_timestamp"] = pd.to_datetime(df["order_purchase_timestamp"])
    df["ds"] = df["order_purchase_timestamp"].dt.strftime("%Y-%m-%d")

    s3 = get_s3_client()
    grouped = df.groupby("ds")
    
    for ds, group_df in grouped:
        s3_key = f"raw/ds={ds}/orders.csv"
        csv_buffer = group_df.drop(columns=["ds"]).to_csv(index=False)
        
        s3.put_object(
            Bucket=BUCKET_NAME,
            Key=s3_key,
            Body=csv_buffer
        )
        print(f"✅ Landed raw drop for date: {ds} ({len(group_df)} rows) -> s3://{BUCKET_NAME}/{s3_key}")

    print("🎉 Daily drop simulation complete! Check your MinIO console.")

if __name__ == "__main__":
    simulate_daily_drops()