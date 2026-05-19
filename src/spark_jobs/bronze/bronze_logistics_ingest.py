import requests
import json
from datetime import datetime
import boto3

LOCALSTACK_ENDPOINT = "http://localstack:4566"
BUCKET_NAME = "retail-bronze"
LOGISTICS_API = "http://mock-api:8000"
BULK_BATCH_SIZE = 500


def get_order_ids():
    s3 = boto3.client(
        "s3",
        endpoint_url=LOCALSTACK_ENDPOINT,
        aws_access_key_id="test",
        aws_secret_access_key="test",
        region_name="us-east-1",
    )

    order_ids = set()
    paginator = s3.get_paginator("list_objects_v2")

    for page in paginator.paginate(Bucket=BUCKET_NAME, Prefix="orders/"):
        for obj in page.get("Contents", []):
            file = s3.get_object(Bucket=BUCKET_NAME, Key=obj["Key"])
            data = json.loads(file["Body"].read())
            if isinstance(data, list):
                for record in data:
                    order_ids.add(record["order_id"])
            else:
                order_ids.add(data["order_id"])

    return list(order_ids)


def fetch_bulk(order_ids_batch):
    response = requests.post(
        f"{LOGISTICS_API}/shipments/bulk",
        json={"order_ids": order_ids_batch},
        timeout=30,
    )
    response.raise_for_status()
    return response.json()


def upload_batch(records, batch_num):
    s3 = boto3.client(
        "s3",
        endpoint_url=LOCALSTACK_ENDPOINT,
        aws_access_key_id="test",
        aws_secret_access_key="test",
        region_name="us-east-1",
    )
    timestamp = datetime.now().strftime("%Y-%m-%d-%H-%M-%S-%f")
    key = f"logistics/batch_{batch_num:04d}_{timestamp}.json"
    s3.put_object(Bucket=BUCKET_NAME, Key=key, Body=json.dumps(records))
    print(f"Uploaded logistics batch {batch_num} ({len(records)} records) → s3://{BUCKET_NAME}/{key}")


if __name__ == "__main__":
    order_ids = get_order_ids()
    print(f"Found {len(order_ids)} orders — fetching logistics in batches of {BULK_BATCH_SIZE}")

    total_batches = (len(order_ids) + BULK_BATCH_SIZE - 1) // BULK_BATCH_SIZE

    for batch_num, start in enumerate(range(0, len(order_ids), BULK_BATCH_SIZE)):
        batch = order_ids[start:start + BULK_BATCH_SIZE]
        try:
            records = fetch_bulk(batch)
            upload_batch(records, batch_num)
        except Exception as e:
            print(f"Batch {batch_num} failed: {e}")

    print(f"Done. Processed {total_batches} batches.")
