# File: /dags/extract_job.py

import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import col


def run_job(args):
    """
    Main function to run the Spark job.
    """
    if len(args) < 2:
        print("Usage: my_spark_job.py <postgres_conn_id> <minio_conn_id>")
        sys.exit(1)

    pg_conn_id = args[0]
    minio_conn_id = args[1]

    # In a real-world scenario, you would fetch these from
    # Airflow using BaseHook or directly from a secret manager.
    # For a simple example, we assume we get the connection info
    # from the command-line arguments.

    print("Running Spark job...")

    # Initialize a SparkSession
    spark = (
        SparkSession.builder.appName("PostgresToDeltaLake")
        .config(
            "spark.jars.packages",
            "org.postgresql:postgresql:42.7.3,io.delta:delta-core_2.12:2.4.0",
        )
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config(
            "spark.hadoop.fs.s3a.endpoint",
            "http://minio-tenant-hl.storage.svc.cluster.local:9000",
        )
        .config("spark.hadoop.fs.s3a.access.key", "minio")
        .config("spark.hadoop.fs.s3a.secret.key", "minio123")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .enableHiveSupport()
        .getOrCreate()
    )

    # Define connection details and paths
    pg_url = "jdbc:postgresql://postgresql.storage.svc.cluster.local:5432/crm_db"
    pg_table = "product_reviews"

    delta_table_path = "s3a://tsc-bucket/delta/product_reviews"
    hive_table_name = "product_reviews_delta_lake"

    try:
        print(f"Reading data from {pg_url}...")
        df_postgres = (
            spark.read.format("jdbc")
            .option("url", pg_url)
            .option("dbtable", pg_table)
            .option("user", "pgadmin")
            .option("password", "pw123")
            .load()
        )

        print(f"Read {df_postgres.count()} rows from PostgreSQL.")

        df_transformed = df_postgres.select(
            col("review_id"),
            col("review"),
            col("created_at"),
            col("product_id"),
            col("source"),
        )

        print(f"Writing data to Delta Lake at {delta_table_path}...")
        df_transformed.write.format("delta").mode("overwrite").option(
            "path", delta_table_path
        ).saveAsTable(hive_table_name)

        print(
            f"Data successfully written to Delta Lake and registered as table '{hive_table_name}'."
        )

    except Exception as e:
        print(f"An error occurred: {e}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    run_job(sys.argv[1:])
