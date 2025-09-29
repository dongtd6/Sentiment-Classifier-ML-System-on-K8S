# jobs/bronze_job.py
from common.io import read_postgres, write_delta
from common.utils import get_spark
from pyspark.sql.functions import col, current_date, date_sub, to_date

if __name__ == "__main__":
    spark = get_spark("Test Job")
    logger = spark._jvm.org.apache.log4j.LogManager.getLogger(__name__)
    logger.info("Starting Test Job...")
    spark.sql("SHOW DATABASES").show()
    spark.sql("CREATE DATABASE IF NOT EXISTS mlops_test")
    spark.sql("SHOW DATABASES").show()

    df = spark.read.format("delta").load("s3a://tsc-bucket/bronze/raw_reviews")
    df.show()
    print(spark._jvm.io.delta.__version__)
