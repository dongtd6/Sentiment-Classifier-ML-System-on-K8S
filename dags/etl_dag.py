# dags/etl_dag.py
from datetime import datetime

from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator

default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 1, 1),
}

with DAG(
    dag_id="full_etl_pipeline",
    start_date=datetime(2025, 8, 1),
    schedule="0 0 * * *",
    default_args=default_args,
    catchup=False,
) as dag:

    run_spark_job = KubernetesPodOperator(
        task_id="extract_job",
        name="extract-job",
        namespace="orchestration",  # namespace của airflow worker trên k8s
        image="dongtd6/airflow-job-scripts:latest",  # image bạn build
        cmds=["python", "extract_job.py"],  # command chạy trong container
        is_delete_operator_pod=True,
        get_logs=True,
    )

    # transform_task = SparkSubmitOperator(
    #     task_id="transform_to_silver",
    #     application="/opt/jobs/transform_job.py",
    #     conn_id="spark_default",
    #     jars="/opt/jars/postgresql-42.6.0.jar",
    #     conf={
    #         "spark.kubernetes.container.image": "dongtd6/airflow-job-scripts:latest",
    #         "spark.driver.extraJavaOptions": "--add-opens=java.base/java.nio=ALL-UNNAMED",
    #     },
    # )

    # summary_task = SparkSubmitOperator(
    #     task_id="create_gold_summary",
    #     application="/opt/jobs/summary_job.py",
    #     conn_id="spark_default",
    #     jars="/opt/jars/postgresql-42.6.0.jar",
    #     conf={
    #         "spark.kubernetes.container.image": "dongtd6/airflow-job-scripts:latest",
    #         "spark.driver.extraJavaOptions": "--add-opens=java.base/java.nio=ALL-UNNAMED",
    #     },
    # )

    # predict_task = SparkSubmitOperator(
    #     task_id="predict_and_save",
    #     application="/opt/jobs/predict_job.py",
    #     conn_id="spark_default",
    #     jars="/opt/jars/postgresql-42.6.0.jar",
    #     conf={
    #         "spark.kubernetes.container.image": "dongtd6/airflow-job-scripts:latest",
    #         "spark.driver.extraJavaOptions": "--add-opens=java.base/java.nio=ALL-UNNAMED",
    #     },
    # )

    # extract_task >> transform_task >> summary_task >> predict_task
