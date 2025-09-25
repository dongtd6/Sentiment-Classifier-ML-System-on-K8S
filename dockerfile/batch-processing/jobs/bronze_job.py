# jobs/bronze_job.py
from common.io import read_postgres, write_delta
from common.utils import get_spark
from pyspark.sql.functions import col, current_date, date_sub, to_date

if __name__ == "__main__":
    bucket = "tsc-bucket"
    schema = "bronze"
    table_name = "raw_reviews"
    path = f"s3a://{bucket}/{schema}/{table_name}"

    spark = get_spark(f"{schema.capitalize()} Job")
    print(f"Starting {schema.capitalize()} Job...")

    # Extract
    print("Extracting data from PostgreSQL...")
    try:
        max_created_at = (
            spark.read.format("delta")
            .load(path)
            .agg({"created_at": "max"})
            .collect()[0][0]
        )
        print(f"Max created_at in Bronze: {max_created_at}")
    except Exception as e:
        max_created_at = None
        print("No Bronze data found yet.")
    if max_created_at:  # Query Postgres incremental
        query = f"SELECT * FROM product_reviews WHERE created_at > '{max_created_at}'"
    else:
        query = "SELECT * FROM product_reviews"
    dataframe = read_postgres(spark, query=query)
    dataframe.show(6)
    dataframe.printSchema()
    print(f"Total records extracted: {dataframe.count()}")

    # Load
    print("Writing to Bronze Delta Lake...")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")
    write_delta(dataframe, f"{schema}.{table_name}", path, mode="overwrite")
    spark.stop()
    print(f"{schema.capitalize()} job completed successfully.")

    # Transform
    # print("Transforming data...")
    # yesterday = date_sub(current_date(), 1)
    # transformed_dataframe = dataframe.where(to_date(col("created_at")) == yesterday)
    # transformed_dataframe.show(6)
    # transformed_dataframe.printSchema()
    # print(f"Total records after filtering: {transformed_dataframe.count()}")

    # dataframe.sparkSession.sql("CREATE SCHEMA IF NOT EXISTS bronze")
    # print(dataframe.collect())
    # print(dataframe.head(5))
    # print(dataframe.take(5))
    # print(dataframe.show(5, truncate=False))
    # dataframe.describe().show()
    # dataframe.summary().show()
    # dataframe.select("review_text").show(5, truncate=False)
    # dataframe.groupBy("sentiment").count().show()
    # dataframe.groupBy("product_category").count().show()
    # dataframe.groupBy("review_date").count().orderBy("review_date").show(10)
    # dataframe.createOrReplaceTempView("reviews")
    # spark.sql("SELECT COUNT(*) FROM reviews WHERE sentiment = 'positive'").show()
    # spark.sql("SELECT product_category, AVG(rating) as avg_rating FROM reviews GROUP BY product_category").show()
    # spark.sql("SELECT * FROM reviews ORDER BY review_date DESC LIMIT 5").show(truncate=False)
    # dataframe.filter(dataframe['sentiment'] == 'positive').show(5)
    # dataframe.filter(dataframe['sentiment'] == 'negative').show(5)
    # dataframe.filter(dataframe['sentiment'] == 'neutral').show(5)
    # dataframe.filter(dataframe['rating'] >= 4).show(5)
    # dataframe.filter(dataframe['rating'] <= 2).show(5)
    # dataframe.filter((dataframe['sentiment'] == 'positive') & (dataframe['rating'] >= 4)).show(5)
    # dataframe.filter((dataframe['sentiment'] == 'negative') & (dataframe['rating'] <= 2)).show(5)
    # dataframe.filter((dataframe['sentiment'] == 'neutral') & (dataframe['rating'] == 3)).show(5)
    # dataframe.filter(dataframe['review_text'].isNotNull()).show(5)
    # dataframe.filter(dataframe['review_text'].isNull()).show(5)
    # dataframe.filter(dataframe['review_text'] != '').show(5)
    # dataframe.filter(dataframe['review_text'] == '').show(5)
    # dataframe.dropna(subset=['review_text']).show(5)
    # dataframe = dataframe.dropna(subset=['review_text'])
    # dataframe = dataframe.filter(dataframe['review_text'] != '')
    # dataframe = dataframe.filter(dataframe['review_text'].isNotNull())
    # dataframe = dataframe.dropDuplicates(['review_id'])
    # dataframe = dataframe.dropDuplicates(['review_id', 'user_id'])
    # dataframe = dataframe.withColumnRenamed('review_text', 'text')
    # dataframe = dataframe.withColumnRenamed('review_date', 'date')
    # dataframe = dataframe.withColumn('date', to_date(col('date'), 'yyyy-MM

    # transformed_dataframe = filter_yesterday(dataframe)
    # transformed_dataframe = transformed_dataframe.withColumnRenamed('review_text', 'text')
    # transformed_dataframe = transformed_dataframe.withColumnRenamed('review_date', 'date')
    # transformed_dataframe = transformed_dataframe.withColumn('date', to_date(col('date'), 'yyyy-MM-dd'))
    # transformed_dataframe = transformed_dataframe.dropna(subset=['text'])
    # transformed_dataframe = transformed_dataframe.filter(transformed_dataframe['text'] != '')
    # transformed_dataframe = transformed_dataframe.dropDuplicates(['review_id'])
    # transformed_dataframe = transformed_dataframe.dropDuplicates(['review_id', 'user_id'])
    # transformed_dataframe = transformed_dataframe.withColumn('rating', col('rating').cast('integer'))
    # transformed_dataframe = transformed_dataframe.withColumn('sentiment', col('sentiment').cast('string'))
    # transformed_dataframe = transformed_dataframe.withColumn('product_category', col('product_category').cast('string'))
    # transformed_dataframe = transformed_dataframe.withColumn('text', col('text').cast('string'))
    # transformed_dataframe = transformed_dataframe.withColumn('date', col('date').cast('date'))
    # transformed_dataframe = transformed_dataframe.withColumn('user_id', col('user_id').cast('string'))
    # transformed_dataframe = transformed_dataframe.withColumn('review_id', col('review_id').cast('string'))
    # transformed_dataframe = transformed_dataframe.withColumn('created_at', col('created_at').cast('timestamp'))
    # transformed_dataframe = transformed_dataframe.withColumn('updated_at', col('updated_at').cast('timestamp'))
    # transformed_dataframe = transformed_dataframe.withColumn('verified_purchase', col('verified_purchase').cast('boolean'))
    # transformed_dataframe = transformed_dataframe.withColumn('helpful_votes', col('helpful_votes').cast('integer'))
    # transformed_dataframe = transformed_dataframe.withColumn('total_votes', col('total_votes').cast('integer'))
    # transformed_dataframe = transformed_dataframe.withColumn('review_title', col('review_title').cast('string'))
    # transformed_dataframe = transformed_dataframe.withColumn('review_title', col('review_title').cast('string'))
    # transformed_dataframe = transformed_dataframe.withColumn('review_text', col('review_text').cast('string'))
    # transformed_dataframe = transformed_dataframe.withColumn('product_id', col('product_id').cast('string'))
    # transformed_dataframe = transformed_dataframe.withColumn('product_name', col('product_name').cast('string'))
    # transformed_dataframe = transformed_dataframe.withColumn('product_category', col('product_category').cast('string'))
    # transformed_dataframe = transformed_dataframe.withColumn('sentiment', col('sentiment').cast('string'))
    # transformed_dataframe = transformed_dataframe.withColumn('rating', col('rating').cast('integer'))
    # transformed_dataframe = transformed_dataframe.withColumn('review_date', to_date(col('review_date'), 'yyyy-MM-dd'))
    # transformed_dataframe = transformed_dataframe.withColumn('created_at', col('created_at').cast('timestamp'))
    # transformed_dataframe = transformed_dataframe.withColumn('updated_at', col('updated_at').cast('timestamp'))
    # transformed_dataframe = transformed_dataframe.withColumn('helpful_votes', col('helpful_votes').cast('integer'))
    # transformed_dataframe = transformed_dataframe.withColumn('total_votes', col('total_votes').cast('integer'))
    # transformed_dataframe = transformed_dataframe.withColumn('verified_purchase', col('verified_purchase').cast('boolean'))
    # transformed_dataframe = transformed_dataframe.withColumn('user_id', col('user_id').cast('string'))
    # transformed_dataframe = transformed_dataframe.withColumn('review_id', col('review_id').cast('string'))
    # transformed_dataframe = transformed_dataframe.withColumn('review_title', col('review_title').cast('string'))
    # transformed_dataframe = transformed_dataframe.withColumn('review_text', col('review_text').cast('string'))
    # transformed_dataframe = transformed_dataframe.withColumn('product_id', col('product_id').cast('string'))
    # transformed_dataframe = transformed_dataframe.withColumn('product_name', col('product_name').cast('string'))
    # transformed_dataframe = transformed_dataframe.withColumn('product_category', col('product_category').cast('string'))
