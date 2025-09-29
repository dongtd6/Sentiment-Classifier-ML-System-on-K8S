# jobs/gold_job.py
import pyspark.sql.functions as F
from common.io import write_delta
from common.utils import get_spark


def transform(bronze_df, silver_df):
    logger.info("Transform: aggregating sentiment by product & source...")
    gold_df = (
        silver_df.join(
            bronze_df.select("review", "product_id", "source", "created_at"),
            on="review",
            how="left",
        )
        .groupBy("source")  # .groupBy("product_id", "source")
        .agg(
            F.count("*").alias("review_count"),
            F.sum(F.when(F.col("sentiment") == "POS", 1).otherwise(0)).alias(
                "positive_count"
            ),
            F.sum(F.when(F.col("sentiment") == "NEG", 1).otherwise(0)).alias(
                "negative_count"
            ),
            F.sum(F.when(F.col("sentiment") == "NEU", 1).otherwise(0)).alias(
                "neutral_count"
            ),
            F.max("created_at").alias("last_review_at"),
        )
        .withColumn("positive_ratio", F.col("positive_count") / F.col("review_count"))
    )
    return gold_df


if __name__ == "__main__":
    spark = get_spark("Gold Job")
    bronze_path = f"s3a://tsc-bucket/bronze/raw_reviews"
    silver_path = f"s3a://tsc-bucket/silver/reviews_with_sentiment"
    gold_path = f"s3a://tsc-bucket/gold/reviews_summary"

    logger = spark._jvm.org.apache.log4j.LogManager.getLogger(__name__)
    logger.info("Starting Aggregate Reviews Daily Job...")

    # Read Bronze and Silver data
    bronze_df = spark.read.format("delta").load(bronze_path)
    silver_df = spark.read.format("delta").load(silver_path)
    bronze_df.show(6)
    bronze_df.printSchema()
    logger.info(f"Total records in Bronze: {bronze_df.count()}")
    silver_df.show(6)
    silver_df.printSchema()
    logger.info(f"Total records in Silver: {silver_df.count()}")

    # Transform
    gold_df = transform(bronze_df, silver_df)

    # Load
    logger.info("Load: Writing Gold data to Delta Lake...")
    write_delta(gold_df, "reviews_summary", gold_path, mode="overwrite")
    gold_df.show(6)
    gold_df.printSchema()
    logger.info(f"Total records in Gold: {gold_df.count()}")
    spark.stop()
    logger.info("Aggregate Reviews Daily Job completed successfully.")

    # # Join Bronze and Silver data on review text
    # joined_df = bronze_df.alias("b").join(
    #     silver_df.alias("s"),
    #     F.trim(F.lower(F.col("b.review"))) == F.trim(F.lower(F.col("s.review"))),
    #     "inner"
    # ).select(
    #     F.col("b.source"),
    #     F.col("b.sentiment"),
    #     F.col("b.created_at")
    # )
    # joined_df.show(6)
    # joined_df.printSchema()
    # logger.info(f"Total records after join: {joined_df.count()}")
    # # Aggregate to get counts by source and sentiment
    # summary_df = joined_df.groupBy("source", "sentiment").count()
    # summary_df = summary_df.withColumnRenamed("count", "review_count")
    # summary_df.show(6)
    # summary_df.printSchema()
    # logger.info(f"Total summary records: {summary_df.count()}")

    # logger.info("Bronze Data...")
    # bronze_dataframe = (
    # spark.read.format("delta")
    # .load(bronze_path)
    # )
    # bronze_dataframe.show(6)
    # bronze_dataframe.printSchema()
    # logger.info(f"Total records read: {bronze_dataframe.count()}")

    # logger.info("Silver Data...")
    # silver_dataframe = (
    # spark.read.format("delta")
    # .load(silver_path)
    # )
    # silver_dataframe.show(6)
    # silver_dataframe.printSchema()
    # logger.info(f"Total records read: {silver_dataframe.count()}")
