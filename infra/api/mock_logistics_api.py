from fastapi import FastAPI
from pydantic import BaseModel
from datetime import datetime, timedelta
from typing import List
import random

app = FastAPI(title="Mock Logistics API")

CARRIERS = ["UPS", "FedEx", "USPS", "DHL", "OnTrac"]
STATUSES = ["DELIVERED", "IN_TRANSIT", "OUT_FOR_DELIVERY", "DELAYED", "RETURNED"]
STATUS_WEIGHTS = [0.55, 0.25, 0.10, 0.07, 0.03]


def _build_shipment(order_id: str) -> dict:
    ship_time = datetime.utcnow() - timedelta(hours=random.randint(1, 120))
    status = random.choices(STATUSES, weights=STATUS_WEIGHTS, k=1)[0]

    delivered_ts = None
    if status == "DELIVERED":
        delivered_ts = (ship_time + timedelta(hours=random.randint(12, 96))).isoformat()

    return {
        "order_id": order_id,
        "carrier": random.choice(CARRIERS),
        "ship_ts": ship_time.isoformat(),
        "delivered_ts": delivered_ts,
        "status": status,
    }


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/shipments/{order_id}")
def get_shipment(order_id: str):
    return _build_shipment(order_id)


class BulkRequest(BaseModel):
    order_ids: List[str]


@app.post("/shipments/bulk")
def get_shipments_bulk(body: BulkRequest):
    """Return shipment records for up to 500 order IDs in one call."""
    return [_build_shipment(oid) for oid in body.order_ids[:500]]
