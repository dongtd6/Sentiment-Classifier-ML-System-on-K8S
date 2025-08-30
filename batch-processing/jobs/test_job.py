# jobs/bronze_job.py
from common.io import read_postgres, write_delta
from common.utils import get_spark
from pyspark.sql.functions import col, current_date, date_sub, to_date

if __name__ == "__main__":
    spark = get_spark("Test Job")
    print("Starting Test Job...")
    spark.sql("SHOW DATABASES").show()
    spark.sql("CREATE DATABASE IF NOT EXISTS mlops_test")
    spark.sql("SHOW DATABASES").show()
