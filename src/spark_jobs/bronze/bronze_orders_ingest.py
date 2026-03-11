import json
import random
from datetime import datetime, timedelta
import boto3

LOCALSTACK_ENDPOINT = "http://localstack:4566"
BUCKET_NAME = "retail-bronze"


def generate_orders(n=10):
    orders = []

    for i in range(n):
        order = {
            "order_id": f"ORD{i+1000}",
            "order_ts": datetime.now().isoformat(),
            "customer_id": f"CUST{random.randint(1,50)}",
            "channel": random.choice(["online", "store"]),
            "region": random.choice(["West", "East", "Central"]),
            "sku": f"SKU{random.randint(1,20)}",
            "qty": random.randint(1,5),
            "unit_price": random.randint(10,200),
            "discount": random.randint(0,20),
            "promised_delivery_ts": (datetime.now() + timedelta(days=3)).isoformat(),
            "order_status": "placed"
        }
        orders.append(order)

    return orders


def upload_to_s3(data):
    s3 = boto3.client(
        "s3",
        endpoint_url=LOCALSTACK_ENDPOINT,
        aws_access_key_id="test",
        aws_secret_access_key="test",
        region_name="us-east-1",
    )

    timestamp = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
    key = f"orders/{timestamp}.json"

    s3.put_object(
        Bucket=BUCKET_NAME,
        Key=key,
        Body=json.dumps(data),
    )

    print(f"Uploaded to s3://{BUCKET_NAME}/{key}")


if __name__ == "__main__":
    orders = generate_orders(20)
    upload_to_s3(orders)