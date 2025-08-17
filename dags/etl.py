# File: /dags/etl.py

from datetime import datetime

from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.sdk import DAG, task

# A DAG represents a workflow, a collection of tasks
with DAG(
    dag_id="extract_with_spark_submit",
    start_date=datetime(2025, 8, 1),
    schedule="0 0 * * *",
) as dag:
    # Use SparkSubmitOperator to run the PySpark job
    extract = SparkSubmitOperator(
        task_id="extract_data",
        application="/opt/airflow/dags/my_spark_job.py",  # Path to your Python file
        conn_id="spark_default",  # Airflow connection ID for your Spark cluster
        # You can pass arguments to the script here
        application_args=["<postgres_conn_id>", "<minio_conn_id>"],
    )

    @task()
    def transform():
        print("transforming data...")

    @task()
    def load():
        print("loading data...")

    # Set dependencies between tasks
    extract >> transform() >> load()
