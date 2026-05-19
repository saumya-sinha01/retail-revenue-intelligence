CREATE TABLE inventory_snapshots (
    snapshot_ts  TIMESTAMP,
    warehouse_id TEXT,
    region       TEXT,
    sku          TEXT,
    on_hand_qty  INT
);

-- 10 warehouses × 4 snapshot dates × 50 SKUs per warehouse = 2,000 rows
-- High-velocity SKUs (001-040) get intentionally low stock → realistic stockouts
DO $$
DECLARE
    warehouses  TEXT[]      := ARRAY['W01','W02','W03','W04','W05','W06','W07','W08','W09','W10'];
    regions     TEXT[]      := ARRAY['West','West','East','East','Central','Central','South','South','North','North'];
    snap_dates  TIMESTAMP[] := ARRAY[
        NOW() - INTERVAL '90 days',
        NOW() - INTERVAL '60 days',
        NOW() - INTERVAL '30 days',
        NOW()
    ];
    w_idx    INT;
    sku_num  INT;
    snap_ts  TIMESTAMP;
    qty      INT;
    sku_id   TEXT;
BEGIN
    FOREACH snap_ts IN ARRAY snap_dates LOOP
        FOR w_idx IN 1..10 LOOP
            FOR sku_num IN 1..50 LOOP
                sku_id := FORMAT('SKU%s', LPAD(((w_idx - 1) * 20 + sku_num)::TEXT, 3, '0'));

                -- High-velocity SKUs (001-040): low stock → higher stockout chance
                -- Low-velocity SKUs (041-200): ample stock
                IF ((w_idx - 1) * 20 + sku_num) <= 40 THEN
                    qty := FLOOR(RANDOM() * 15)::INT;   -- 0–14 units (scarce)
                ELSE
                    qty := FLOOR(RANDOM() * 150 + 50)::INT;  -- 50–199 units (plentiful)
                END IF;

                INSERT INTO inventory_snapshots (snapshot_ts, warehouse_id, region, sku, on_hand_qty)
                VALUES (snap_ts, warehouses[w_idx], regions[w_idx], sku_id, qty);
            END LOOP;
        END LOOP;
    END LOOP;
END $$;
