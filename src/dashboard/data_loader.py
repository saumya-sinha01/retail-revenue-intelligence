import io
from typing import List

import boto3
import pandas as pd
import pyarrow.parquet as pq

# LocalStack endpoint (host machine)
S3_ENDPOINT = "http://localhost:4566"
AWS_REGION = "us-east-1"
AWS_ACCESS_KEY_ID = "test"
AWS_SECRET_ACCESS_KEY = "test"

GOLD_BUCKET = "retail-gold"


def _get_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        region_name=AWS_REGION,
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    )


def _list_parquet_keys(bucket: str, prefix: str) -> List[str]:
    """
    Return all parquet object keys under a dataset prefix like:
    revenue_kpis/summary/
    """
    s3 = _get_s3_client()

    paginator = s3.get_paginator("list_objects_v2")
    keys: List[str] = []

    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.endswith(".parquet"):
                keys.append(key)

    if not keys:
        raise FileNotFoundError(
            f"No parquet files found in s3://{bucket}/{prefix}"
        )

    return sorted(keys)


def _read_parquet_dataset(bucket: str, prefix: str) -> pd.DataFrame:
    """
    Read a Spark-written parquet dataset from LocalStack S3 by downloading
    all part-*.parquet files under the prefix and concatenating them.
    """
    s3 = _get_s3_client()
    parquet_keys = _list_parquet_keys(bucket, prefix)

    frames = []
    for key in parquet_keys:
        response = s3.get_object(Bucket=bucket, Key=key)
        body = response["Body"].read()

        table = pq.read_table(io.BytesIO(body))
        frames.append(table.to_pandas())

    if not frames:
        raise FileNotFoundError(
            f"Dataset exists but no readable parquet parts found in s3://{bucket}/{prefix}"
        )

    return pd.concat(frames, ignore_index=True)


# -----------------------------
# Revenue Data
# -----------------------------
def load_revenue_data():
    summary = _read_parquet_dataset(GOLD_BUCKET, "revenue_kpis/summary/")
    by_region = _read_parquet_dataset(GOLD_BUCKET, "revenue_kpis/by_region/")
    return summary, by_region


# -----------------------------
# Revenue at Risk
# -----------------------------
def load_risk_data():
    summary = _read_parquet_dataset(GOLD_BUCKET, "revenue_at_risk/summary/")
    return summary


# -----------------------------
# Supply Chain Metrics
# -----------------------------
def load_supply_chain_data():
    summary = _read_parquet_dataset(GOLD_BUCKET, "supply_chain_metrics/summary/")
    return summary