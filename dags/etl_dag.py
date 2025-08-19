# etl_dag.py
from datetime import datetime

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

with DAG(
    dag_id="full_etl_pipeline",
    start_date=datetime(2025, 8, 1),
    schedule="0 0 * * *",
    catchup=False,
) as dag:

    extract_task = SparkSubmitOperator(
        task_id="extract_to_bronze",
        application="/opt/jobs/extract_job.py",
        conn_id="spark_default",
        jars="/opt/jars/postgresql-42.6.0.jar",
        conf={
            "spark.kubernetes.container.image": "dongtd6/airflow-job-scripts:latest",
            "spark.driver.extraJavaOptions": "--add-opens=java.base/java.nio=ALL-UNNAMED",
        },
    )
    extract_task = SparkSubmitOperator(
        task_id="transform_to_silver",
        application="/opt/jobs/transform_job.py",
        conn_id="spark_default",
        jars="/opt/jars/postgresql-42.6.0.jar",
        conf={
            "spark.kubernetes.container.image": "dongtd6/airflow-job-scripts:latest",
            "spark.driver.extraJavaOptions": "--add-opens=java.base/java.nio=ALL-UNNAMED",
        },
    )

    extract_task = SparkSubmitOperator(
        task_id="create_gold_summary",
        application="/opt/jobs/summary_job.py",
        conn_id="spark_default",
        jars="/opt/jars/postgresql-42.6.0.jar",
        conf={
            "spark.kubernetes.container.image": "dongtd6/airflow-job-scripts:latest",
            "spark.driver.extraJavaOptions": "--add-opens=java.base/java.nio=ALL-UNNAMED",
        },
    )

    extract_task = SparkSubmitOperator(
        task_id="predict_and_save",
        application="/opt/jobs/predict_job.py",
        conn_id="spark_default",
        jars="/opt/jars/postgresql-42.6.0.jar",
        conf={
            "spark.kubernetes.container.image": "dongtd6/airflow-job-scripts:latest",
            "spark.driver.extraJavaOptions": "--add-opens=java.base/java.nio=ALL-UNNAMED",
        },
    )

    extract_task >> transform_task >> summary_task >> predict_task
