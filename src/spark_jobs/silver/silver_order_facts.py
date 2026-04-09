from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, row_number
from pyspark.sql.window import Window


BRONZE_BUCKET = "s3a://retail-bronze"
SILVER_BUCKET = "s3a://retail-silver/order_facts"

LOCALSTACK_ENDPOINT = "http://localstack:4566"


def create_spark_session():

    spark = (
        SparkSession.builder
        .appName("SilverOrders")

        # Load S3 connector
        .config(
            "spark.jars.packages",
            "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262"
        )

        # LocalStack S3 configuration
        .config("spark.hadoop.fs.s3a.endpoint", LOCALSTACK_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", "test")
        .config("spark.hadoop.fs.s3a.secret.key", "test")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")

        # Required for LocalStack
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
        )

        .getOrCreate()
    )

    return spark


def main():

    spark = create_spark_session()

    # ------------------------------------------------------------------
    # Load Bronze Layer Data
    # ------------------------------------------------------------------

    orders = (
        spark.read
        .option("multiLine", "true")
        .json(f"{BRONZE_BUCKET}/orders/*.json")
        .dropDuplicates(["order_id", "sku"])
    )

    inventory = (
        spark.read
        .option("multiLine", "true")
        .json(f"{BRONZE_BUCKET}/inventory/*.json")

        # ------------------------------------------------------------------
        # NEW AMENDMENT
        #
        # Inventory dataset also contains "region".
        # This creates ambiguity when joined with orders.
        #
        # Rename it immediately to avoid Spark ambiguous reference errors.
        # ------------------------------------------------------------------
        .withColumnRenamed("region", "inventory_region")
    )

    logistics = (
        spark.read
        .option("multiLine", "true")
        .json(f"{BRONZE_BUCKET}/logistics/*.json")

        # ------------------------------------------------------------------
        # NEW COMMENT
        #
        # Normalize logistics schema so downstream layers always receive
        # consistent column naming.
        #
        # Bronze uses "status" but Silver standardizes to "delivery_status"
        # to make the meaning clearer for analytics layers.
        # ------------------------------------------------------------------
        .withColumnRenamed("status", "delivery_status")
    )

    # ------------------------------------------------------------------
    # POINT-IN-TIME INVENTORY JOIN
    #
    # Problem this solves:
    # If we join orders with the latest inventory snapshot we introduce
    # look-ahead bias (future inventory appearing available for past orders).
    #
    # Correct rule:
    # inventory.snapshot_ts <= orders.order_ts
    #
    # Then select the latest valid snapshot.
    #
    # Grain enforced:
    # one row per (order_id + sku)
    # ------------------------------------------------------------------

    orders_inventory = (
        orders.alias("o")
        .join(
            inventory.alias("i"),
            col("o.sku") == col("i.sku"),
            "left"
        )

        # enforce point-in-time condition
        .filter(col("i.snapshot_ts") <= col("o.order_ts"))

        # ------------------------------------------------------------------
        # NEW AMENDMENT
        #
        # Remove duplicate SKU column from inventory
        # (orders.sku is canonical)
        # ------------------------------------------------------------------
        .drop(col("i.sku"))
    )

    # ------------------------------------------------------------------
    # Select the latest inventory snapshot BEFORE the order timestamp
    # using a window function
    # ------------------------------------------------------------------

    inventory_window = Window.partitionBy(
        "o.order_id",
        "o.sku"
    ).orderBy(col("i.snapshot_ts").desc())

    orders_inventory_latest = (
        orders_inventory
        .withColumn("rn", row_number().over(inventory_window))
        .filter(col("rn") == 1)

        # ------------------------------------------------------------------
        # NEW AMENDMENT
        #
        # Drop helper ranking column after selecting latest snapshot
        # ------------------------------------------------------------------
        .drop("rn")
    )

    # ------------------------------------------------------------------
    # Build the SILVER FACT TABLE
    #
    # Fact Table Grain:
    # one row per order line (order_id + sku)
    #
    # Derived Metrics Added:
    # order_value
    # inventory_at_order_time
    # stockout_flag
    # ingestion_ts
    # ------------------------------------------------------------------

    order_facts = (
        orders_inventory_latest.alias("oi")
        .join(
            logistics.alias("l"),
            col("oi.order_id") == col("l.order_id"),
            "left"
        )
        .select(

            # --------------------------------------------------------------
            # Fact grain identifiers
            # --------------------------------------------------------------
            col("oi.order_id"),
            col("oi.sku"),
            col("oi.customer_id"),

            # --------------------------------------------------------------
            # Order attributes
            # --------------------------------------------------------------
            col("oi.qty").alias("quantity"),
            col("oi.unit_price").alias("price"),

            # revenue calculation
            (col("oi.qty") * col("oi.unit_price")).alias("order_value"),

            # --------------------------------------------------------------
            # Inventory state at time of order
            # --------------------------------------------------------------
            col("oi.on_hand_qty").alias("inventory_at_order_time"),

            # stockout indicator
            (col("oi.on_hand_qty") < col("oi.qty")).alias("stockout_flag"),

            col("oi.warehouse_id"),

            # ------------------------------------------------------------------
            # NEW AMENDMENT
            #
            # Use orders.region as the canonical region for the fact table
            # inventory_region remains available if needed later
            # ------------------------------------------------------------------
            col("oi.region").alias("region"),

            # --------------------------------------------------------------
            # Logistics attributes
            # --------------------------------------------------------------

            # NEW COMMENT
            # Include shipment lifecycle timestamps and carrier information
            # so downstream Gold models can compute delivery performance KPIs
            # such as shipping latency, carrier performance, and delivery success.
            col("l.delivery_status"),
            col("l.carrier"),
            col("l.ship_ts"),
            col("l.delivered_ts"),

            # --------------------------------------------------------------
            # Pipeline metadata
            # --------------------------------------------------------------
            current_timestamp().alias("ingestion_ts")
        )
    )

    # ------------------------------------------------------------------
    # Write Silver Layer
    # ------------------------------------------------------------------

    (
        order_facts
        .write
        .mode("overwrite")
        .json(SILVER_BUCKET)
    )

    spark.stop()


if __name__ == "__main__":
    main()