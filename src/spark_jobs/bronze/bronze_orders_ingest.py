import json
import random
from datetime import datetime, timedelta
import boto3

LOCALSTACK_ENDPOINT = "http://localstack:4566"
BUCKET_NAME         = "retail-bronze"

# ── Regions: West is dominant, North is smallest ──────────────────────────
REGIONS         = ["West", "East", "Central", "South", "North"]
REGION_WEIGHTS  = [0.35,   0.25,   0.20,      0.12,    0.08]

# ── Channels: online leads, marketplace is niche ──────────────────────────
CHANNELS        = ["online", "store", "mobile", "marketplace"]
CHANNEL_WEIGHTS = [0.45,     0.25,    0.20,     0.10]

# ── SKUs: Pareto — top 40 SKUs drive ~70% of volume ──────────────────────
ALL_SKUS         = [f"SKU{i:03d}" for i in range(1, 201)]
HIGH_VELOCITY    = ALL_SKUS[:40]   # top 40 — frequently ordered
LOW_VELOCITY     = ALL_SKUS[40:]   # long tail

# ── Customers ─────────────────────────────────────────────────────────────
CUSTOMERS = [f"CUST{i:05d}" for i in range(1, 5001)]

HISTORY_DAYS = 180  # 6 months

# ── Avg price per channel (realistic differentiation) ────────────────────
CHANNEL_PRICE_RANGE = {
    "online":      (30,  800),
    "store":       (20,  400),
    "mobile":      (15,  300),
    "marketplace": (10,  250),
}

# ── Avg qty per region (West customers buy more per order) ────────────────
REGION_QTY_RANGE = {
    "West":    (1, 8),
    "East":    (1, 6),
    "Central": (1, 5),
    "South":   (1, 4),
    "North":   (1, 3),
}


def _pick_sku() -> str:
    """70% chance of picking a high-velocity SKU (Pareto principle)."""
    return random.choice(HIGH_VELOCITY if random.random() < 0.70 else LOW_VELOCITY)


def generate_orders(n: int = 50000) -> list:
    orders = []
    now = datetime.now()

    for i in range(n):
        days_ago = random.randint(0, HISTORY_DAYS)
        hours_ago = random.randint(0, 23)
        order_ts = now - timedelta(days=days_ago, hours=hours_ago)

        channel = random.choices(CHANNELS, weights=CHANNEL_WEIGHTS, k=1)[0]
        region  = random.choices(REGIONS,  weights=REGION_WEIGHTS,  k=1)[0]
        sku     = _pick_sku()

        price_lo, price_hi = CHANNEL_PRICE_RANGE[channel]
        qty_lo,   qty_hi   = REGION_QTY_RANGE[region]

        orders.append({
            "order_id":             f"ORD{i+10000:06d}",
            "order_ts":             order_ts.isoformat(),
            "customer_id":          random.choice(CUSTOMERS),
            "channel":              channel,
            "region":               region,
            "sku":                  sku,
            "qty":                  random.randint(qty_lo, qty_hi),
            "unit_price":           round(random.uniform(price_lo, price_hi), 2),
            "discount":             round(random.uniform(0.0, 0.30), 2),
            "promised_delivery_ts": (order_ts + timedelta(days=random.randint(1, 7))).isoformat(),
            "order_status":         random.choices(
                ["placed", "confirmed", "shipped", "delivered", "cancelled"],
                weights=[0.05, 0.10, 0.20, 0.60, 0.05],
                k=1
            )[0],
        })

    return orders


def upload_batches(orders: list, batch_size: int = 1000):
    s3 = boto3.client(
        "s3",
        endpoint_url=LOCALSTACK_ENDPOINT,
        aws_access_key_id="test",
        aws_secret_access_key="test",
        region_name="us-east-1",
    )

    total   = len(orders)
    n_batches = (total + batch_size - 1) // batch_size

    for batch_num, start in enumerate(range(0, total, batch_size)):
        batch     = orders[start:start + batch_size]
        timestamp = datetime.now().strftime("%Y-%m-%d-%H-%M-%S-%f")
        key       = f"orders/batch_{batch_num:04d}_{timestamp}.json"
        s3.put_object(Bucket=BUCKET_NAME, Key=key, Body=json.dumps(batch))
        print(f"Uploaded batch {batch_num + 1}/{n_batches} → s3://{BUCKET_NAME}/{key}")


if __name__ == "__main__":
    print("Generating 50,000 weighted orders...")
    orders = generate_orders(50000)
    print("Uploading in batches of 1,000...")
    upload_batches(orders, batch_size=1000)
    print("Done.")
