import requests
import json
from datetime import datetime
import boto3

LOCALSTACK_ENDPOINT = "http://localstack:4566"
BUCKET_NAME = "retail-bronze"


def fetch_logistics_event(order_id: str):
    url = f"http://mock-api:8000/shipments/{order_id}"
    response = requests.get(url)
    response.raise_for_status()
    return response.json()


def upload_to_s3(data: dict):
    s3 = boto3.client(
        "s3",
        endpoint_url=LOCALSTACK_ENDPOINT,
        aws_access_key_id="test",
        aws_secret_access_key="test",
        region_name="us-east-1",
    )

    timestamp = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
    key = f"logistics/{timestamp}.json"

    s3.put_object(
        Bucket=BUCKET_NAME,
        Key=key,
        Body=json.dumps(data),
    )

    print(f"Uploaded to s3://{BUCKET_NAME}/{key}")


if __name__ == "__main__":
    order_id = "ORD123"
    data = fetch_logistics_event(order_id)
    upload_to_s3(data)