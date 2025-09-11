# jobs/gold_job.py
import pyspark.sql.functions as F
from common.io import write_delta
from common.utils import get_spark

if __name__ == "__main__":
    spark = get_spark("Gold Job")
    print("Starting Gold Job...")
    bronze_path = f"s3a://tsc-bucket/bronze/raw_reviews"
    silver_path = f"s3a://tsc-bucket/silver/reviews_with_sentiment"
    gold_path = f"s3a://tsc-bucket/gold/reviews_summary"

    # Read Bronze and Silver data
    bronze_df = spark.read.format("delta").load(bronze_path)
    silver_df = spark.read.format("delta").load(silver_path)

    bronze_df.show(6)
    bronze_df.printSchema()
    print(f"Total records in Bronze: {bronze_df.count()}")
    silver_df.show(6)
    silver_df.printSchema()
    print(f"Total records in Silver: {silver_df.count()}")

    # Transform
    print("Transform: aggregating sentiment by product & source...")
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

    # Load
    print("Writing Gold data to Delta Lake...")
    write_delta(gold_df, "reviews_summary", gold_path, mode="overwrite")
    gold_df.show(6)
    gold_df.printSchema()
    print(f"Total records in Gold: {gold_df.count()}")
    spark.stop()
    print("Gold Job completed ✅")

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
    # print(f"Total records after join: {joined_df.count()}")
    # # Aggregate to get counts by source and sentiment
    # summary_df = joined_df.groupBy("source", "sentiment").count()
    # summary_df = summary_df.withColumnRenamed("count", "review_count")
    # summary_df.show(6)
    # summary_df.printSchema()
    # print(f"Total summary records: {summary_df.count()}")

    # print("Bronze Data...")
    # bronze_dataframe = (
    # spark.read.format("delta")
    # .load(bronze_path)
    # )
    # bronze_dataframe.show(6)
    # bronze_dataframe.printSchema()
    # print(f"Total records read: {bronze_dataframe.count()}")

    # print("Silver Data...")
    # silver_dataframe = (
    # spark.read.format("delta")
    # .load(silver_path)
    # )
    # silver_dataframe.show(6)
    # silver_dataframe.printSchema()
    # print(f"Total records read: {silver_dataframe.count()}")
