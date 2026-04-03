import boto3
import json
import time
from pathlib import Path

import pandas as pd

s3 = boto3.client('s3')
BUCKET_NAME = 'my-ecommerce-project-landing-zone'
FILE_PATH = Path(__file__).resolve().parent.parent / 'looker-ecommerce-dataset' / 'events.csv'

def stream_csv_to_s3() -> None:
    # Read CSV and stream each record to S3 as an individual JSON object.
    df = pd.read_csv(FILE_PATH)

    for i, row in df.iterrows():
        event = row.to_dict()
        session_id_raw = event.get("session_id")
        if pd.isna(session_id_raw) or str(session_id_raw).strip() == "":
            session_id = f"unknown_{i}"
        else:
            session_id = str(session_id_raw)
        safe_session_id = "".join(ch if ch.isalnum() or ch in ("-", "_") else "_" for ch in session_id)
        filename = f"click-stream-raw/{safe_session_id}_{int(time.time())}.json"

        s3.put_object(
            Bucket=BUCKET_NAME,
            Key=filename,
            Body=json.dumps(event),
        )
        print(f"Uploaded {filename} to S3...")
        time.sleep(10)


if __name__ == "__main__":
    stream_csv_to_s3()
