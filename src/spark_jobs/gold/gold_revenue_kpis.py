from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, sum, avg, count, current_timestamp,
    date_format, to_timestamp
)

SILVER_BUCKET = "s3a://retail-silver/order_facts"
GOLD_BUCKET   = "s3a://retail-gold/revenue_kpis"
LOCALSTACK_ENDPOINT = "http://localstack:4566"


def create_spark_session():
    print("Creating Spark session...")
    spark = (
        SparkSession.builder
        .appName("GoldRevenueKPIs")
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
    print("Spark session created")
    return spark


def main():
    print("Starting Gold Revenue KPIs job...")
    spark = create_spark_session()

    order_facts = (
        spark.read.parquet(SILVER_BUCKET)
        .filter(col("order_id").isNotNull())
        .filter(col("sku").isNotNull())
        .filter(col("order_value").isNotNull())
        .withColumn("order_month", date_format(to_timestamp(col("order_ts")), "yyyy-MM"))
    )

    ts = current_timestamp()

    # 1 — Overall summary
    revenue_summary = (
        order_facts.agg(
            sum("order_value").alias("total_revenue"),
            avg("order_value").alias("avg_order_value"),
            count("order_id").alias("total_orders"),
        ).withColumn("metric_generated_ts", ts)
    )

    # 2 — By region
    revenue_by_region = (
        order_facts.groupBy("region")
        .agg(
            sum("order_value").alias("total_revenue"),
            count("order_id").alias("total_orders"),
            avg("order_value").alias("avg_order_value"),
        ).withColumn("metric_generated_ts", ts)
    )

    # 3 — By channel
    revenue_by_channel = (
        order_facts.groupBy("channel")
        .agg(
            sum("order_value").alias("total_revenue"),
            count("order_id").alias("total_orders"),
            avg("order_value").alias("avg_order_value"),
        ).withColumn("metric_generated_ts", ts)
    )

    # 4 — Monthly trend
    revenue_by_month = (
        order_facts.groupBy("order_month")
        .agg(
            sum("order_value").alias("total_revenue"),
            count("order_id").alias("total_orders"),
            avg("order_value").alias("avg_order_value"),
        ).orderBy("order_month")
        .withColumn("metric_generated_ts", ts)
    )

    # 5 — Monthly × Region (powers the filtered region chart in the dashboard)
    revenue_by_month_region = (
        order_facts.groupBy("order_month", "region")
        .agg(
            sum("order_value").alias("total_revenue"),
            count("order_id").alias("total_orders"),
            avg("order_value").alias("avg_order_value"),
        ).orderBy("order_month", "region")
        .withColumn("metric_generated_ts", ts)
    )

    # 6 — Monthly × Channel
    revenue_by_month_channel = (
        order_facts.groupBy("order_month", "channel")
        .agg(
            sum("order_value").alias("total_revenue"),
            count("order_id").alias("total_orders"),
            avg("order_value").alias("avg_order_value"),
        ).orderBy("order_month", "channel")
        .withColumn("metric_generated_ts", ts)
    )

    # 7 — Top SKUs
    revenue_by_sku = (
        order_facts.groupBy("sku")
        .agg(
            sum("order_value").alias("total_revenue"),
            count("order_id").alias("total_orders"),
            avg("order_value").alias("avg_order_value"),
        ).orderBy(col("total_revenue").desc())
        .withColumn("metric_generated_ts", ts)
    )

    print("Writing Gold revenue datasets...")
    revenue_summary.write.mode("overwrite").parquet(f"{GOLD_BUCKET}/summary")
    revenue_by_region.write.mode("overwrite").parquet(f"{GOLD_BUCKET}/by_region")
    revenue_by_channel.write.mode("overwrite").parquet(f"{GOLD_BUCKET}/by_channel")
    revenue_by_month.write.mode("overwrite").parquet(f"{GOLD_BUCKET}/by_month")
    revenue_by_month_region.write.mode("overwrite").parquet(f"{GOLD_BUCKET}/by_month_region")
    revenue_by_month_channel.write.mode("overwrite").parquet(f"{GOLD_BUCKET}/by_month_channel")
    revenue_by_sku.write.mode("overwrite").parquet(f"{GOLD_BUCKET}/by_sku")

    print("Gold Revenue KPI job completed.")
    spark.stop()


if __name__ == "__main__":
    main()
