import os
from pathlib import Path

import boto3
from dotenv import load_dotenv

load_dotenv()


def upload_ecommerce_data():
    aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID")
    aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")
    aws_region = os.getenv("AWS_REGION")
    bucket_name = os.getenv("S3_BUCKET_NAME") or os.getenv("AWS_S3_BUCKET")

    missing_values = []
    if not aws_access_key_id:
        missing_values.append("AWS_ACCESS_KEY_ID")
    if not aws_secret_access_key:
        missing_values.append("AWS_SECRET_ACCESS_KEY")
    if not aws_region:
        missing_values.append("AWS_REGION")
    if not bucket_name:
        missing_values.append("S3_BUCKET_NAME or AWS_S3_BUCKET")

    if missing_values:
        print(f"Missing required environment values: {', '.join(missing_values)}")
        return

    s3_client = boto3.client(
        "s3",
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        region_name=aws_region,
    )

    script_dir = Path(__file__).resolve().parent

    data_files = [
        "distribution_centers.csv",
        "events.csv",
        "inventory_items.csv",
        "order_items.csv",
        "orders.csv",
        "products.csv",
        "users.csv",
    ]

    print(f"Starting upload to bucket: {bucket_name}")

    for file_name in data_files:
        file_path = script_dir / file_name
        if file_path.exists():
            s3_path = f"raw/{file_name}"
            print(f"Uploading {file_name} -> {s3_path}")
            try:
                s3_client.upload_file(str(file_path), bucket_name, s3_path)
                print(f"Uploaded {file_name} successfully")
            except Exception as e:
                print(f"Failed to upload {file_name}: {e}")
        else:
            print(f"File not found: {file_path}")


if __name__ == "__main__":
    upload_ecommerce_data()
