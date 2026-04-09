from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    count,
    sum,
    avg,
    when,
    current_timestamp,
    coalesce,
    lit,
    lower
)

SILVER_BUCKET = "s3a://retail-silver/order_facts"
GOLD_BUCKET = "s3a://retail-gold/supply_chain_metrics"

LOCALSTACK_ENDPOINT = "http://localstack:4566"


def create_spark_session():

    spark = (
        SparkSession.builder
        .appName("GoldSupplyChainMetrics")
        .config(
            "spark.jars.packages",
            "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262"
        )
        .config("spark.hadoop.fs.s3a.endpoint", LOCALSTACK_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", "test")
        .config("spark.hadoop.fs.s3a.secret.key", "test")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
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

    print("Loading Silver data...")
    order_facts = spark.read.json(SILVER_BUCKET)

    # ✅ DEBUG (run once, then remove)
    print("Columns:", order_facts.columns)
    order_facts.select("delivery_status").distinct().show()

    # ------------------------------------------------------------
    # CLEAN + STANDARDIZE
    # ------------------------------------------------------------

    order_facts_clean = (
        order_facts
        .filter(col("order_id").isNotNull())
        .filter(col("sku").isNotNull())
        .filter(col("warehouse_id").isNotNull())
        # ✅ FIX: normalize delivery_status
        .withColumn("delivery_status_clean", lower(col("delivery_status")))
    )

    # ------------------------------------------------------------
    # SUPPLY CHAIN SUMMARY
    # ------------------------------------------------------------

    supply_chain_summary = (
        order_facts_clean
        .agg(
            count("order_id").alias("total_order_lines"),

            coalesce(
                sum(when(col("stockout_flag") == True, 1).otherwise(0)),
                lit(0)
            ).alias("stockout_order_lines"),

            avg(
                when(col("stockout_flag") == True, 1).otherwise(0).cast("double")
            ).alias("stockout_rate"),

            # ✅ FIXED DELIVERY LOGIC
            coalesce(
                sum(
                    when(col("delivery_status_clean") == "delivered", 1).otherwise(0)
                ),
                lit(0)
            ).alias("delivered_order_lines"),

            avg(
                when(col("delivery_status_clean") == "delivered", 1).otherwise(0).cast("double")
            ).alias("delivery_success_rate")
        )
        .withColumn("metric_generated_ts", current_timestamp())
    )

    # ------------------------------------------------------------
    # WRITE OUTPUT
    # ------------------------------------------------------------

    (
        supply_chain_summary
        .write
        .mode("overwrite")
        .parquet(f"{GOLD_BUCKET}/summary")
    )

    print("✅ Supply chain metrics updated")

    spark.stop()


if __name__ == "__main__":
    main()