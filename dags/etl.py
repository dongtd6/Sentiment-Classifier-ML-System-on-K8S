import os
from datetime import datetime

from airflow.sdk import DAG, task
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# A DAG represents a workflow, a collection of tasks
with DAG(
    dag_id="extract", start_date=datetime(2025, 8, 1), schedule="0 0 * * *"
) as dag:
    # Tasks are represented as operators
    # hello = BashOperator(task_id="hello", bash_command="echo hello")

    @task()
    def extract():
        """
        Reads data from PostgreSQL and writes it to a Delta table on MinIO.
        This task uses a Spark Session to handle the data processing.
        """
        print("Extracting data from PostgreSQL...")
        # read data from postgresql: jdbc:postgresql://postgresql.storage.svc.cluster.local:5432/crm_db, table product_reviews
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

        # MinIO (S3-compatible) path for the Delta table
        delta_table_path = "s3a://tsc-bucket/delta/product_reviews"

        # Hive table name
        hive_table_name = "product_reviews_delta_lake"

        try:
            # Read data from PostgreSQL
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

            # Perform any necessary transformations (optional)
            # For example, filtering or selecting specific columns
            df_transformed = df_postgres.select(
                col("review_id"),
                col("review"),
                col("created_at"),
                col("product_id"),
                col("source"),
            )

            # Write data to the Delta table
            print(f"Writing data to Delta Lake at {delta_table_path}...")
            df_transformed.write.format("delta").mode("overwrite").option(
                "path", delta_table_path
            ).saveAsTable(hive_table_name)

            print(
                f"Data successfully written to Delta Lake and registered as table '{hive_table_name}'."
            )

        except Exception as e:
            print(f"An error occurred: {e}")
        finally:
            spark.stop()

    # Define the tasks in the DAG
    # Each task is a function decorated with @task()
    # These tasks can be executed in parallel or sequentially based on dependencies
    @task()
    def transform():
        print("transforming data...")

    @task()
    def load():
        print("loading data...")

    # Set dependencies between tasks
    extract() >> transform() >> load()
