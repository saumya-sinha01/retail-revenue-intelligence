from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, sum, avg, when, current_timestamp, coalesce, lit, lower
)

SILVER_BUCKET = "s3a://retail-silver/order_facts"
GOLD_BUCKET   = "s3a://retail-gold/supply_chain_metrics"

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

    order_facts = spark.read.parquet(SILVER_BUCKET)

    order_facts_clean = (
        order_facts
        .filter(col("order_id").isNotNull())
        .filter(col("sku").isNotNull())
        .filter(col("warehouse_id").isNotNull())
        .withColumn("delivery_status_clean", lower(col("delivery_status")))
    )

    def _agg_supply(df):
        return df.agg(
            count("order_id").alias("total_order_lines"),
            coalesce(sum(when(col("stockout_flag"), 1).otherwise(0)), lit(0)).alias("stockout_order_lines"),
            avg(when(col("stockout_flag"), 1).otherwise(0).cast("double")).alias("stockout_rate"),
            coalesce(sum(when(col("delivery_status_clean") == "delivered", 1).otherwise(0)), lit(0)).alias("delivered_order_lines"),
            avg(when(col("delivery_status_clean") == "delivered", 1).otherwise(0).cast("double")).alias("delivery_success_rate"),
        ).withColumn("metric_generated_ts", current_timestamp())

    # 1 — Overall summary
    supply_summary = _agg_supply(order_facts_clean)

    # 2 — By warehouse
    supply_by_warehouse = (
        order_facts_clean
        .groupBy("warehouse_id")
        .agg(
            count("order_id").alias("total_order_lines"),
            coalesce(sum(when(col("stockout_flag"), 1).otherwise(0)), lit(0)).alias("stockout_order_lines"),
            avg(when(col("stockout_flag"), 1).otherwise(0).cast("double")).alias("stockout_rate"),
            avg(when(col("delivery_status_clean") == "delivered", 1).otherwise(0).cast("double")).alias("delivery_success_rate"),
        )
        .orderBy("warehouse_id")
        .withColumn("metric_generated_ts", current_timestamp())
    )

    # 3 — Top stockout SKUs
    top_stockout_skus = (
        order_facts_clean
        .filter(col("stockout_flag"))
        .groupBy("sku")
        .agg(
            count("order_id").alias("stockout_orders"),
            sum("order_value").alias("revenue_at_risk"),
        )
        .orderBy(col("stockout_orders").desc())
        .limit(50)
        .withColumn("metric_generated_ts", current_timestamp())
    )

    print("Writing supply chain datasets...")
    supply_summary.write.mode("overwrite").parquet(f"{GOLD_BUCKET}/summary")
    supply_by_warehouse.write.mode("overwrite").parquet(f"{GOLD_BUCKET}/by_warehouse")
    top_stockout_skus.write.mode("overwrite").parquet(f"{GOLD_BUCKET}/top_stockout_skus")

    print("Supply chain metrics job completed.")
    spark.stop()


if __name__ == "__main__":
    main()
