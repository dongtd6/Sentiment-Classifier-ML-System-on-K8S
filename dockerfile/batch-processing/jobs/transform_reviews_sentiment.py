# jobs/silver_job.py
from datetime import datetime, timedelta

import pyspark.sql.functions as F
from common.io import read_postgres, write_delta
from common.utils import get_spark
from pyspark.sql.functions import col, current_date, date_sub, lit, to_date
from pyspark.sql.types import StringType
from transforms.batch_prediction import batch_predict


def extract(spark, silver_path, logger):
    logger.info("Extract: Reading from Delta table in MinIO...")
    try:
        max_created_at = (
            spark.read.format("delta")
            .load(silver_path)
            .agg(F.max("created_at").alias("max_created_at"))
            .collect()[0]["max_created_at"]
        )
        logger.info(f"Max created_at in Silver: {max_created_at}")
    except Exception as e:
        max_created_at = None
        logger.info("No Silver data found yet.")
    bronze_path = f"s3a://{bucket}/bronze/raw_reviews"
    yesterday = date_sub(current_date(), 1)
    bronze_dataframe = (
        spark.read.format("delta")
        .load(bronze_path)
        .filter((col("created_at") >= yesterday) & (col("created_at") < current_date()))
    )
    bronze_dataframe.show(6)
    bronze_dataframe.logger.infoSchema()
    logger.info(f"Total records read: {bronze_dataframe.count()}")


def transform(spark, bronze_dataframe, logger):
    logger.info("Transform: Add sentiment column and clean data...")
    bronze_dataframe = bronze_dataframe.withColumn(
        "sentiment", F.lit(None).cast(StringType())
    )
    bronze_dataframe = bronze_dataframe.dropDuplicates(["review"])
    bronze_dataframe = bronze_dataframe.na.drop(subset=["review"])
    bronze_dataframe = bronze_dataframe.filter(col("review") != "")
    # bronze_dataframe = bronze_dataframe.filter(col("product_id").isNotNull())
    # bronze_dataframe = bronze_dataframe.filter(col("source").isNotNull())
    # bronze_dataframe = bronze_dataframe.filter(col("created_at").isNotNull())
    # bronze_dataframe = bronze_dataframe.filter(col("created_at") >= "2000-01-01")
    # bronze_dataframe = bronze_dataframe.filter(col("created_at") < current_date())
    bronze_dataframe = bronze_dataframe.drop("user_id")
    bronze_dataframe = bronze_dataframe.drop("review_id")
    bronze_dataframe = bronze_dataframe.drop("product_id")
    bronze_dataframe = bronze_dataframe.drop("created_at")
    bronze_dataframe = bronze_dataframe.drop("updated_at")
    bronze_dataframe = bronze_dataframe.drop("source")
    bronze_dataframe = bronze_dataframe.drop("is_deleted")
    bronze_dataframe.show(6)
    bronze_dataframe.logger.infoSchema()
    logger.info(f"Total records after cleaning: {bronze_dataframe.count()}")
    logger.info("Update data ...")
    silver_dataframe = batch_predict(spark, bronze_dataframe, logger)
    silver_dataframe.show(6)
    silver_dataframe.logger.infoSchema()
    logger.info(f"Total records after prediction: {silver_dataframe.count()}")


if __name__ == "__main__":
    bucket = "tsc-bucket"
    schema = "silver"
    table_name = "reviews_with_sentiment"
    silver_path = f"s3a://{bucket}/{schema}/{table_name}"
    spark = get_spark(f"{schema.capitalize()} Job")
    logger = spark._jvm.org.apache.log4j.LogManager.getLogger(__name__)
    logger.info(f"Starting Tranform Review Sentiment Job...")

    # Extract
    bronze_dataframe = extract(
        spark, silver_path, logger
    )  # Check max creat at in Silver and extract

    # Transform
    silver_dataframe = transform(spark, bronze_dataframe, logger)

    # Load
    logger.info("Load: Writing to Delta table in MinIO ...")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")
    write_delta(
        silver_dataframe, f"{schema}.{table_name}", silver_path, mode="overwrite"
    )
    spark.stop()
    logger.info("Tranform Review Sentiment Job completed successfully.")

    # dataframe = bronze_dataframe.select("review_id", "product_id", "review_text", "sentiment", "source", "created_at")
    # dataframe.show(6)
    # dataframe.logger.infoSchema()
    # logger.info(f"Total records after transformation: {dataframe.count()}")

    # # Run SQL query
    # logger.info("🚀 Running SQL query...")
    # dataframe_sql = spark.sql("""
    #     SELECT count(*) as cnt, source
    #     FROM raw_reviews_delta_lake
    #     GROUP BY source
    # """)
    # dataframe_sql.show()
    # dataframe_sql.logger.infoSchema()
    # logger.info(f"Total records after SQL transformation: {dataframe_sql.count()}")

    # Register DataFrame as a temp view
    # bronze_dataframe.createOrReplaceTempView("raw_reviews_delta_lake")

    # Add column name "sentiment" to bronze_dataframe
    # bronze_dataframe = bronze_dataframe.withColumn("sentiment", lit("unknown"))
