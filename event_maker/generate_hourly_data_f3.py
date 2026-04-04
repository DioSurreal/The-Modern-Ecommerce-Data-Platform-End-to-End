import argparse
import os
import random
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd


def _load_env_file():
    candidates = [
        Path.cwd() / ".env",
        Path(__file__).resolve().parent / ".env",
        Path(__file__).resolve().parent.parent / ".env",
    ]
    for env_path in candidates:
        if not env_path.exists():
            continue
        for raw_line in env_path.read_text(encoding="utf-8").splitlines():
            line = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))
        return env_path
    return None


def _env_bool(name, default=False):
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "y", "on"}


def _build_s3_client(region):
    try:
        import boto3
    except ModuleNotFoundError as exc:
        raise RuntimeError("boto3 is required for S3 upload. Install with: pip install boto3") from exc

    kwargs = {}
    access_key = os.getenv("F3_AWS_ACCESS_KEY_ID") or os.getenv("AWS_ACCESS_KEY_ID")
    secret_key = os.getenv("F3_AWS_SECRET_ACCESS_KEY") or os.getenv("AWS_SECRET_ACCESS_KEY")
    if access_key and secret_key:
        kwargs["aws_access_key_id"] = access_key
        kwargs["aws_secret_access_key"] = secret_key
    if region:
        kwargs["region_name"] = region

    return boto3.client("s3", **kwargs)


def _upload_file_to_s3(local_path, bucket, key, region=None):
    client = _build_s3_client(region)
    client.upload_file(str(local_path), bucket, key)
    print(f"Uploaded to s3://{bucket}/{key}")


def _format_ts(value):
    return value.strftime("%Y-%m-%d %H:%M:%S")


def generate_data(target_date, target_hour):
    timestamp = datetime.strptime(f"{target_date} {target_hour}:00:00", "%Y-%m-%d %H:%M:%S")
    num_records = random.randint(15, 30)

    order_items = []
    inventory_items = []
    product_categories = ["Jeans", "Jackets", "Shoes", "Accessories", "Tops"]
    product_brands = ["Levis", "Nike", "Adidas", "Zara", "Uniqlo"]
    product_departments = ["Men", "Women"]

    for _ in range(num_records):
        row_id = random.randint(10_000_000, 99_999_999)
        order_id = random.randint(1_000_000, 9_999_999)
        user_id = random.randint(1, 1000)
        product_id = random.randint(1, 500)
        inv_id = random.randint(1, 10000)
        created_ts = timestamp + timedelta(minutes=random.randint(0, 59))
        shipped_ts = created_ts + timedelta(hours=random.randint(1, 6))
        delivered_ts = shipped_ts + timedelta(hours=random.randint(4, 48))
        returned_ts = delivered_ts + timedelta(days=random.randint(1, 7))
        sale_price = round(random.uniform(20.0, 500.0), 2)

        status = random.choices(
            ["Complete", "Shipped", "Delivered", "Returned"],
            weights=[35, 25, 30, 10],
            k=1,
        )[0]

        order_items.append(
            {
                "id": row_id,
                "order_id": order_id,
                "user_id": user_id,
                "product_id": product_id,
                "inventory_item_id": inv_id,
                "status": status,
                "created_at": _format_ts(created_ts),
                "shipped_at": _format_ts(shipped_ts) if status in {"Shipped", "Delivered", "Returned"} else "",
                "delivered_at": _format_ts(delivered_ts) if status in {"Delivered", "Returned"} else "",
                "returned_at": _format_ts(returned_ts) if status == "Returned" else "",
                "sale_price": sale_price,
            }
        )

        product_category = random.choice(product_categories)
        product_brand = random.choice(product_brands)
        product_department = random.choice(product_departments)
        product_name = f"{product_brand} {product_category}"
        product_retail_price = round(sale_price * random.uniform(1.1, 1.6), 2)
        product_sku = f"SKU-{product_id}-{random.randint(1000, 9999)}"
        product_distribution_center_id = random.randint(1, 10)
        inventory_created_ts = created_ts - timedelta(days=random.randint(1, 30))

        inventory_items.append(
            {
                "id": inv_id,
                "product_id": product_id,
                "created_at": _format_ts(inventory_created_ts),
                "sold_at": _format_ts(created_ts),
                "cost": round(sale_price * 0.6, 2),
                "product_category": product_category,
                "product_name": product_name,
                "product_brand": product_brand,
                "product_retail_price": product_retail_price,
                "product_department": product_department,
                "product_sku": product_sku,
                "product_distribution_center_id": product_distribution_center_id,
            }
        )

    order_items_df = pd.DataFrame(
        order_items,
        columns=[
            "id",
            "order_id",
            "user_id",
            "product_id",
            "inventory_item_id",
            "status",
            "created_at",
            "shipped_at",
            "delivered_at",
            "returned_at",
            "sale_price",
        ],
    )
    inventory_items_df = pd.DataFrame(
        inventory_items,
        columns=[
            "id",
            "product_id",
            "created_at",
            "sold_at",
            "cost",
            "product_category",
            "product_name",
            "product_brand",
            "product_retail_price",
            "product_department",
            "product_sku",
            "product_distribution_center_id",
        ],
    )
    return order_items_df, inventory_items_df


def save_local_files(order_items_df, inventory_df, target_date, target_hour):
    base_dir = Path("data") / "landing" / target_date / target_hour
    base_dir.mkdir(parents=True, exist_ok=True)

    order_items_file = base_dir / f"order_items{target_hour}.csv"
    inventory_file = base_dir / f"inventory_items{target_hour}.csv"
    order_items_df.to_csv(order_items_file, index=False)
    inventory_df.to_csv(inventory_file, index=False)
    print(f"Saved local files under {base_dir}")
    return order_items_file, inventory_file


def upload_generated_files(order_items_file, inventory_file, target_date, target_hour, bucket, prefix, region):
    prefix = prefix.strip("/")
    order_items_key = f"{prefix}/{target_date}/{target_hour}/{order_items_file.name}"
    inventory_key = f"{prefix}/{target_date}/{target_hour}/{inventory_file.name}"
    _upload_file_to_s3(order_items_file, bucket, order_items_key, region)
    _upload_file_to_s3(inventory_file, bucket, inventory_key, region)


if __name__ == "__main__":
    loaded_env = _load_env_file()
    now = datetime.now()
    default_upload = _env_bool("F3_UPLOAD_S3", False)

    parser = argparse.ArgumentParser()
    parser.add_argument("--date", default=now.strftime("%Y-%m-%d"), help="Target date in YYYY-MM-DD")
    parser.add_argument("--hour", default=now.strftime("%H"), help="Target hour in HH")
    parser.add_argument("--upload-s3", dest="upload_s3", action="store_true", default=default_upload, help="Upload generated files to S3")
    parser.add_argument("--no-upload-s3", dest="upload_s3", action="store_false", help="Disable S3 upload")
    parser.add_argument("--bucket", default=os.getenv("F3_S3_BUCKET") or os.getenv("AWS_S3_BUCKET"), help="S3 bucket name")
    parser.add_argument("--s3-prefix", default=os.getenv("F3_S3_PREFIX", "landing"), help="S3 key prefix")
    parser.add_argument("--region", default=os.getenv("F3_AWS_REGION") or os.getenv("AWS_REGION"), help="AWS region (optional)")
    args = parser.parse_args()

    if not args.hour.isdigit() or not 0 <= int(args.hour) <= 23:
        raise ValueError("--hour must be between 00 and 23")

    order_items_df, inventory_df = generate_data(args.date, args.hour)
    order_items_file, inventory_file = save_local_files(order_items_df, inventory_df, args.date, args.hour)

    if args.upload_s3:
        if not args.bucket:
            raise ValueError("Missing S3 bucket. Set --bucket or AWS_S3_BUCKET in environment.")
        if loaded_env:
            print(f"Loaded env from {loaded_env}")
        upload_generated_files(
            order_items_file,
            inventory_file,
            args.date,
            args.hour,
            args.bucket,
            args.s3_prefix,
            args.region,
        )
