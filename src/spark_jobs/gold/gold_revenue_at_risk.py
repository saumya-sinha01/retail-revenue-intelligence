from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, count, current_timestamp, coalesce, lit


SILVER_BUCKET = "s3a://retail-silver/order_facts"
GOLD_BUCKET = "s3a://retail-gold/revenue_at_risk"

LOCALSTACK_ENDPOINT = "http://localstack:4566"


def create_spark_session():
    """
    Create Spark session configured for S3A + LocalStack.

    This keeps consistency with the Bronze and Silver Spark jobs
    already implemented in the project.
    """

    print("Creating Spark session...")

    spark = (
        SparkSession.builder
        .appName("GoldRevenueAtRisk")
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

    print("Spark session created successfully")

    return spark


def main():

    print("Starting Revenue At Risk computation job...")

    spark = create_spark_session()

    # ---------------------------------------------------------------
    # Load Silver order facts
    #
    # Grain:
    # one row per (order_id + sku)
    # ---------------------------------------------------------------

    print("Loading Silver order_facts dataset...")

    # FIX: Silver is written as parquet, not json
    order_facts = spark.read.parquet(SILVER_BUCKET)

    print("Silver data loaded successfully")

    # ---------------------------------------------------------------
    # Identify stockout orders
    #
    # stockout_flag = TRUE
    # ---------------------------------------------------------------

    print("Filtering stockout orders...")

    stockout_orders = order_facts.filter(col("stockout_flag"))

    print("Stockout orders filtered")

    # ---------------------------------------------------------------
    # Compute Revenue At Risk
    #
    # Core business metric:
    # revenue lost due to stockouts
    # ---------------------------------------------------------------

    print("Computing revenue at risk metrics...")

    revenue_at_risk_summary = (
        stockout_orders
        .agg(
            coalesce(sum("order_value"), lit(0)).alias("total_revenue_at_risk"),
            count("order_id").alias("stockout_orders")
        )
        .withColumn("metric_generated_ts", current_timestamp())
    )

    # ---------------------------------------------------------------
    # Revenue At Risk by SKU
    #
    # Helps identify products causing revenue leakage
    # ---------------------------------------------------------------

    print("Computing revenue at risk by SKU...")

    revenue_at_risk_by_sku = (
        stockout_orders
        .groupBy("sku")
        .agg(
            coalesce(sum("order_value"), lit(0)).alias("total_revenue_at_risk"),
            count("order_id").alias("stockout_orders")
        )
        .withColumn("metric_generated_ts", current_timestamp())
    )

    # ---------------------------------------------------------------
    # Revenue At Risk by Region
    #
    # Helps identify geographic supply issues
    # ---------------------------------------------------------------

    print("Computing revenue at risk by region...")

    revenue_at_risk_by_region = (
        stockout_orders
        .groupBy("region")
        .agg(
            coalesce(sum("order_value"), lit(0)).alias("total_revenue_at_risk"),
            count("order_id").alias("stockout_orders")
        )
        .withColumn("metric_generated_ts", current_timestamp())
    )

    # ---------------------------------------------------------------
    # Write Gold datasets
    #
    # Stored in Parquet for analytics workloads
    # ---------------------------------------------------------------

    print("Writing revenue at risk summary dataset...")

    (
        revenue_at_risk_summary
        .write
        .mode("overwrite")
        .parquet(f"{GOLD_BUCKET}/summary")
    )

    print("Writing revenue at risk by SKU dataset...")

    (
        revenue_at_risk_by_sku
        .write
        .mode("overwrite")
        .parquet(f"{GOLD_BUCKET}/by_sku")
    )

    print("Writing revenue at risk by region dataset...")

    (
        revenue_at_risk_by_region
        .write
        .mode("overwrite")
        .parquet(f"{GOLD_BUCKET}/by_region")
    )

    print("Revenue At Risk job completed successfully")

    spark.stop()

    print("Spark session stopped")


if __name__ == "__main__":
    main()