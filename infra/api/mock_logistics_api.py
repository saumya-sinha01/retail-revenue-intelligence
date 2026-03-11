from fastapi import FastAPI
from datetime import datetime, timedelta
import random

app = FastAPI(title="Mock Logistics API")


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/shipments/{order_id}")
def get_shipment(order_id: str):
    """
    Mock logistics response for an order.
    Simulates delivery delays and status.
    """
    ship_time = datetime.utcnow() - timedelta(hours=random.randint(1, 48))
    delivered = random.choice([True, False])

    if delivered:
        delivered_ts = ship_time + timedelta(hours=random.randint(2, 72))
        status = "DELIVERED"
    else:
        delivered_ts = None
        status = "IN_TRANSIT"

    return {
        "order_id": order_id,
        "carrier": random.choice(["UPS", "FedEx", "USPS"]),
        "ship_ts": ship_time.isoformat(),
        "delivered_ts": delivered_ts.isoformat() if delivered_ts else None,
        "status": status,
    }