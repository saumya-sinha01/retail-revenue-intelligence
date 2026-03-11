    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col, current_timestamp


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
            .config("spark.hadoop.fs.s3a.endpoint", "http://localstack:4566")
            .config("spark.hadoop.fs.s3a.access.key", "test")
            .config("spark.hadoop.fs.s3a.secret.key", "test")
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")

            # Important for LocalStack
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                    "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")

            .getOrCreate()
        )

        return spark

    def main():
        spark = create_spark_session()

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
        )

        logistics = (
            spark.read
            .option("multiLine", "true")
            .json(f"{BRONZE_BUCKET}/logistics/*.json")
        )

        inventory_latest = (
            inventory
            .dropDuplicates(["sku"])
            .select(
                col("sku").alias("inventory_sku"),
                col("warehouse_id"),
                col("region").alias("inventory_region"),
                col("on_hand_qty"),
                col("snapshot_ts"),
            )
        )

        order_facts = (
            orders.alias("o")
            .join(
                inventory_latest.alias("i"),
                col("o.sku") == col("i.inventory_sku"),
                "left"
            )
            .join(
                logistics.alias("l"),
                col("o.order_id") == col("l.order_id"),
                "left"
            )
            .select(
                col("o.order_id"),
                col("o.sku"),
                col("o.customer_id"),
                col("o.qty").alias("quantity"),
                col("o.unit_price").alias("price"),
                col("i.warehouse_id"),
                col("i.inventory_region"),
                col("i.on_hand_qty"),
                col("l.status").alias("delivery_status"),
                col("l.carrier")
            )
        )

        (
            order_facts
            .write
            .mode("overwrite")
            .json(SILVER_BUCKET)
        )

        spark.stop()

    if __name__ == "__main__":
        main()