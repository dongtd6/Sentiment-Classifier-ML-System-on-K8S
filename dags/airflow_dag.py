# dags/airflow_dag.py
from datetime import datetime

from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator

default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 1, 1),
}

with DAG(
    dag_id="masterdata_full_etl",
    start_date=datetime(2025, 1, 1),
    schedule="0 0 * * *",
    default_args=default_args,
    catchup=False,
) as dag:

    bronze_task = KubernetesPodOperator(
        task_id="bronze_job",
        name="bronze-job",
        namespace="orchestration",  # namespace of airflow worker on k8s
        image="dongtd6/airflow-job-scripts:latest",  # image you built
        cmds=["python", "jobs/bronze_job.py"],  # command to run in the container
        is_delete_operator_pod=True,
        get_logs=True,
    )

    silver_task = KubernetesPodOperator(
        task_id="silver_job",
        name="silver-job",
        namespace="orchestration",
        image="dongtd6/airflow-job-scripts:latest",
        cmds=["python", "jobs/silver_job.py"],
        is_delete_operator_pod=True,
        get_logs=True,
    )

    gold_task = KubernetesPodOperator(
        task_id="gold_job",
        name="gold_job",
        namespace="orchestration",
        image="dongtd6/airflow-job-scripts:latest",
        cmds=["python", "jobs/gold_job.py"],
        is_delete_operator_pod=True,
        get_logs=True,
    )

    bronze_task >> silver_task >> gold_task
