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

    # microseconds prevent overwrite collisions
    timestamp = datetime.now().strftime("%Y-%m-%d-%H-%M-%S-%f")

    key = f"logistics/{timestamp}.json"

    s3.put_object(
        Bucket=BUCKET_NAME,
        Key=key,
        Body=json.dumps(data),
    )

    print(f"Uploaded to s3://{BUCKET_NAME}/{key}")


def get_order_ids():

    s3 = boto3.client(
        "s3",
        endpoint_url=LOCALSTACK_ENDPOINT,
        aws_access_key_id="test",
        aws_secret_access_key="test",
        region_name="us-east-1",
    )

    order_ids = set()

    response = s3.list_objects_v2(
        Bucket=BUCKET_NAME,
        Prefix="orders/"
    )

    for obj in response.get("Contents", []):

        file = s3.get_object(
            Bucket=BUCKET_NAME,
            Key=obj["Key"]
        )

        data = json.loads(file["Body"].read())

        # handle both JSON formats
        if isinstance(data, list):

            for record in data:
                order_ids.add(record["order_id"])

        else:

            order_ids.add(data["order_id"])

    return list(order_ids)


if __name__ == "__main__":

    order_ids = get_order_ids()

    print(f"Found {len(order_ids)} orders")

    for order_id in order_ids:

        try:

            data = fetch_logistics_event(order_id)

            upload_to_s3(data)

        except Exception as e:

            print(f"Failed for {order_id}: {e}")