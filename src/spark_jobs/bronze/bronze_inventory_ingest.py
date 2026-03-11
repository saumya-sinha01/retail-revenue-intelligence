import json
from datetime import datetime
import boto3
import psycopg2

LOCALSTACK_ENDPOINT = "http://localstack:4566"
BUCKET_NAME = "retail-bronze"

POSTGRES_CONN = {
    "host": "postgres",   # docker service name
    "database": "retail",
    "user": "retail_user",
    "password": "retail_pass",
    "port": 5432,
}

def fetch_inventory():
    conn = psycopg2.connect(**POSTGRES_CONN)
    cursor = conn.cursor()

    cursor.execute("SELECT * FROM inventory_snapshots;")
    rows = cursor.fetchall()

    columns = [desc[0] for desc in cursor.description]
    data = [dict(zip(columns, row)) for row in rows]

    cursor.close()
    conn.close()

    return data


def upload_to_s3(data):
    s3 = boto3.client(
        "s3",
        endpoint_url=LOCALSTACK_ENDPOINT,
        aws_access_key_id="test",
        aws_secret_access_key="test",
        region_name="us-east-1",
    )

    timestamp = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
    key = f"inventory/{timestamp}.json"

    s3.put_object(
        Bucket=BUCKET_NAME,
        Key=key,
        Body=json.dumps(data, default=str),
    )

    print(f"Uploaded to s3://{BUCKET_NAME}/{key}")


if __name__ == "__main__":
    inventory_data = fetch_inventory()
    upload_to_s3(inventory_data)