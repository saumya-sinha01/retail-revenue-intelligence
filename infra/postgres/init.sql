CREATE TABLE inventory_snapshots (
    snapshot_ts TIMESTAMP,
    warehouse_id TEXT,
    region TEXT,
    sku TEXT,
    on_hand_qty INT
);

INSERT INTO inventory_snapshots 
(snapshot_ts, warehouse_id, region, sku, on_hand_qty)
VALUES 
(NOW(), 'W1', 'West', 'SKU1', 20),
(NOW(), 'W1', 'West', 'SKU2', 0),
(NOW(), 'W2', 'East', 'SKU3', 15),
(NOW(), 'W2', 'East', 'SKU4', 5),
(NOW(), 'W3', 'Central', 'SKU5', 50);