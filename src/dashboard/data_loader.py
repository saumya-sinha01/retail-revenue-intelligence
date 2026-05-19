import io
from typing import List

import boto3
import pandas as pd
import pyarrow.parquet as pq

S3_ENDPOINT           = "http://localhost:4566"
AWS_REGION            = "us-east-1"
AWS_ACCESS_KEY_ID     = "test"
AWS_SECRET_ACCESS_KEY = "test"
GOLD_BUCKET           = "retail-gold"


def _get_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        region_name=AWS_REGION,
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    )


def _list_parquet_keys(bucket: str, prefix: str) -> List[str]:
    s3  = _get_s3_client()
    pag = s3.get_paginator("list_objects_v2")
    keys: List[str] = []
    for page in pag.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(".parquet"):
                keys.append(obj["Key"])
    return sorted(keys)


def _read_parquet_dataset(bucket: str, prefix: str) -> pd.DataFrame:
    s3   = _get_s3_client()
    keys = _list_parquet_keys(bucket, prefix)
    if not keys:
        return pd.DataFrame()
    frames = []
    for key in keys:
        try:
            body = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
            frames.append(pq.read_table(io.BytesIO(body)).to_pandas())
        except Exception:
            continue
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


# ── Revenue ───────────────────────────────────────────────────────────────
def load_revenue_data():
    summary           = _read_parquet_dataset(GOLD_BUCKET, "revenue_kpis/summary/")
    by_region         = _read_parquet_dataset(GOLD_BUCKET, "revenue_kpis/by_region/")
    by_channel        = _read_parquet_dataset(GOLD_BUCKET, "revenue_kpis/by_channel/")
    by_month          = _read_parquet_dataset(GOLD_BUCKET, "revenue_kpis/by_month/")
    by_month_region   = _read_parquet_dataset(GOLD_BUCKET, "revenue_kpis/by_month_region/")
    by_month_channel  = _read_parquet_dataset(GOLD_BUCKET, "revenue_kpis/by_month_channel/")
    by_sku            = _read_parquet_dataset(GOLD_BUCKET, "revenue_kpis/by_sku/")
    return summary, by_region, by_channel, by_month, by_month_region, by_month_channel, by_sku


# ── Risk ──────────────────────────────────────────────────────────────────
def load_risk_data():
    summary   = _read_parquet_dataset(GOLD_BUCKET, "revenue_at_risk/summary/")
    by_region = _read_parquet_dataset(GOLD_BUCKET, "revenue_at_risk/by_region/")
    by_sku    = _read_parquet_dataset(GOLD_BUCKET, "revenue_at_risk/by_sku/")
    return summary, by_region, by_sku


# ── Supply Chain ──────────────────────────────────────────────────────────
def load_supply_chain_data():
    summary       = _read_parquet_dataset(GOLD_BUCKET, "supply_chain_metrics/summary/")
    by_warehouse  = _read_parquet_dataset(GOLD_BUCKET, "supply_chain_metrics/by_warehouse/")
    top_stockouts = _read_parquet_dataset(GOLD_BUCKET, "supply_chain_metrics/top_stockout_skus/")
    return summary, by_warehouse, top_stockouts
