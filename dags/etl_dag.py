from datetime import datetime

from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.sdk import DAG, task

# Docker Image for Spark jobs
# Ensure this image has Spark and the necessary dependencies installed
# You can build this image with the required JAR files included
SPARK_JOB_IMAGE = "dongtd6/airflow-job-scripts:latest"

with DAG(
    dag_id="full_etl_pipeline", start_date=datetime(2025, 8, 1), schedule="0 0 * * *"
) as dag:

    # Task 1: Kéo dữ liệu (Extract)
    extract_task = SparkSubmitOperator(
        task_id="extract_to_bronze",
        # Use the Kubernetes master URL
        master="k8s://https://kubernetes.default.svc",
        application="/opt/jobs/extract_job.py",
        # Pass the JDBC JAR file to Spark's classpath
        driver_class_path="/opt/jars/postgresql-42.6.0.jar",
        # Pass the Java 17+ configuration options
        conf={
            "spark.kubernetes.container.image": SPARK_JOB_IMAGE,
            "spark.driver.extraJavaOptions": "--add-opens=java.base/java.nio=ALL-UNNAMED",
            "spark.kubernetes.namespace": "orchestration",  # Change to your namespace
            "spark.executor.instances": "1",  # Or more, depending on your needs
            "spark.driver.serviceAccountName": "airflow",  # The service account with permissions to launch pods
        },
        # Ensure the JAR file is available in the cluster (e.g., via a shared volume or image build)
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
