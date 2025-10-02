# postgresl/inputdata.py
import os
import uuid

import pandas as pd
from common.io import write_delta
from common.utils import get_spark
from pyspark.sql.functions import current_timestamp, lit, udf
from pyspark.sql.types import StringType

CSV_FILE = "../data/data.csv"

import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def main():

    logger.info("Starting Init data to Minio Job...")
    logger.info(f"Reading data from CSV file: {CSV_FILE}")
    spark = get_spark("Init data to Minio Job")
    try:
        if not os.path.exists(CSV_FILE):
            logger.info(f"Error: CSV file '{CSV_FILE}' not found.")
            return
        logger.info("CSV file found, proceeding to read data...")

        # Đọc file CSV bằng Spark, không phải pandas
        dataframe = (
            spark.read.format("csv")
            .option("header", "true")
            .option("inferSchema", "true")
            .load(CSV_FILE)
        )

        # Rename columns
        dataframe = dataframe.withColumnRenamed("comment", "review").withColumnRenamed(
            "label", "sentiment"
        )

        logger.info("Read csv successfully: ")
        dataframe.printSchema()
        dataframe.show(6)
        logger.info(f"Total records extracted: {dataframe.count()}")

        # Add additional columns
        uuid_udf = udf(lambda: str(uuid.uuid4()), StringType())
        dataframe = (
            dataframe.withColumn("review_id", uuid_udf())
            .withColumn("created_at", current_timestamp())
            .withColumn("updated_at", current_timestamp())
            .withColumn("product_id", lit("undefined"))
            .withColumn("user_id", lit("undefined"))
            .withColumn("is_deleted", lit(False))
            .withColumn("source", lit("csv_upload"))
        )

        logger.info("Rich data successfully: ")
        dataframe.printSchema()
        dataframe.show(6)
        logger.info(f"Total records enriched: {dataframe.count()}")

        # Load
        logger.info("Load: 🚀 Writing to Delta table in MinIO ...")
        bucket = "tsc-bucket"
        schema = "silver"
        table_name = "reviews_with_sentiment"
        silver_path = f"s3a://{bucket}/{schema}/{table_name}"
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")
        write_delta(dataframe, f"{schema}.{table_name}", silver_path, mode="overwrite")

        logger.info("Read minio after write:")
        silver_dataframe = spark.read.format("delta").load(silver_path)
        silver_dataframe.printSchema()
        silver_dataframe.show(6)
        logger.info(f"Total records enriched: {silver_dataframe.count()}")

        spark.stop()
        logger.info(f"{schema.capitalize()} job completed successfully.")

    except Exception as e:
        logger.info(f"Error inserting data: {e}")


if __name__ == "__main__":
    main()
