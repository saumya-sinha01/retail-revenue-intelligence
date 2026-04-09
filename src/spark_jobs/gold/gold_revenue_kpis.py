from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, avg, count, current_timestamp


SILVER_BUCKET = "s3a://retail-silver/order_facts"
GOLD_BUCKET = "s3a://retail-gold/revenue_kpis"

LOCALSTACK_ENDPOINT = "http://localstack:4566"


def create_spark_session():
    """
    Create Spark session with S3A + LocalStack configuration.

    This mirrors the same runtime pattern used in Bronze/Silver jobs so
    all Spark jobs behave consistently in the project.
    """

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

    print("Spark session created successfully")

    return spark


def main():

    print("Starting Gold Revenue KPIs job...")

    spark = create_spark_session()

    # ------------------------------------------------------------------
    # Load Silver fact table
    # ------------------------------------------------------------------

    print("Loading Silver order_facts dataset...")

    order_facts = spark.read.json(SILVER_BUCKET)

    print("Silver data loaded successfully")

    # ------------------------------------------------------------------
    # Data quality guardrail
    # ------------------------------------------------------------------

    print("Applying data quality filters...")

    order_facts_clean = (
        order_facts
        .filter(col("order_id").isNotNull())
        .filter(col("sku").isNotNull())
        .filter(col("order_value").isNotNull())
    )

    print("Data quality filtering completed")

    # ------------------------------------------------------------------
    # GOLD DATASET 1: Revenue Summary
    # ------------------------------------------------------------------

    print("Computing revenue summary metrics...")

    revenue_summary = (
        order_facts_clean
        .agg(
            sum("order_value").alias("total_revenue"),
            avg("order_value").alias("avg_order_value"),
            count("order_id").alias("total_orders")   # ✅ FIXED
        )
        .withColumn("metric_generated_ts", current_timestamp())
    )

    # ------------------------------------------------------------------
    # GOLD DATASET 2: Revenue by Region
    # ------------------------------------------------------------------

    print("Computing revenue by region metrics...")

    revenue_by_region = (
        order_facts_clean
        .groupBy("region")
        .agg(
            sum("order_value").alias("total_revenue"),
            count("order_id").alias("total_orders"),   # ✅ FIXED
            avg("order_value").alias("avg_order_value")
        )
        .withColumn("metric_generated_ts", current_timestamp())
    )

    # ------------------------------------------------------------------
    # GOLD DATASET 3: Revenue by SKU
    # ------------------------------------------------------------------

    print("Computing revenue by SKU metrics...")

    revenue_by_sku = (
        order_facts_clean
        .groupBy("sku")
        .agg(
            sum("order_value").alias("total_revenue"),
            count("order_id").alias("total_orders"),   # ✅ FIXED
            avg("order_value").alias("avg_order_value")
        )
        .withColumn("metric_generated_ts", current_timestamp())
    )

    # ------------------------------------------------------------------
    # Write Gold outputs
    # ------------------------------------------------------------------

    print("Writing revenue summary dataset...")

    (
        revenue_summary
        .write
        .mode("overwrite")
        .parquet(f"{GOLD_BUCKET}/summary")
    )

    print("Writing revenue by region dataset...")

    (
        revenue_by_region
        .write
        .mode("overwrite")
        .parquet(f"{GOLD_BUCKET}/by_region")
    )

    print("Writing revenue by SKU dataset...")

    (
        revenue_by_sku
        .write
        .mode("overwrite")
        .parquet(f"{GOLD_BUCKET}/by_sku")
    )

    print("Gold Revenue KPI job completed successfully")

    spark.stop()

    print("Spark session stopped")


if __name__ == "__main__":
    main()