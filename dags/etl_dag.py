from datetime import datetime

from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.sdk import DAG

# Define the Docker image for Spark jobs
# This image should contain the necessary Spark and PostgreSQL JDBC driver setup
SPARK_JOB_IMAGE = "dongtd6/airflow-job-scripts:latest"

with DAG(
    dag_id="full_etl_pipeline", start_date=datetime(2025, 8, 1), schedule="0 0 * * *"
) as dag:
    # Task 1: Kéo dữ liệu (Extract)
    extract_task = SparkSubmitOperator(
        task_id="extract_to_bronze",
        # Pass master as a top-level argument
        master="k8s://https://kubernetes.default.svc",
        application="/opt/jobs/extract_job.py",
        driver_class_path="/opt/jars/postgresql-42.6.0.jar",
        # Keep Kubernetes-specific conf parameters here
        conf={
            "spark.kubernetes.container.image": SPARK_JOB_IMAGE,
            "spark.driver.extraJavaOptions": "--add-opens=java.base/java.nio=ALL-UNNAMED",
            "spark.kubernetes.namespace": "orchestration",
            "spark.executor.instances": "1",
            "spark.driver.serviceAccountName": "airflow",
        },
        jars="/opt/jars/postgresql-42.6.0.jar",
    )

    # Task 2: Xử lý và làm sạch (Transform)
    transform_task = SparkSubmitOperator(
        task_id="transform_to_silver",
        application="/opt/jobs/transform_job.py",
        conf={"spark.kubernetes.container.image": SPARK_JOB_IMAGE},
        # ... các tham số khác
    )

    # Task 3: Tổng hợp (Summary)
    summary_task = SparkSubmitOperator(
        task_id="create_gold_summary",
        application="/opt/jobs/summary_job.py",
        conf={"spark.kubernetes.container.image": SPARK_JOB_IMAGE},
        # ... các tham số khác
    )

    # Task 4: Dự đoán (Predict)
    predict_task = SparkSubmitOperator(
        task_id="predict_and_save",
        application="/opt/jobs/predict_job.py",
        conf={"spark.kubernetes.container.image": SPARK_JOB_IMAGE},
        # ... các tham số khác
    )

    # Thiết lập luồng công việc
    extract_task >> transform_task >> summary_task >> predict_task
