from datetime import datetime, timedelta
from airflow import DAG
from conveyor.operators import ConveyorSparkSubmitOperatorV2

role = "capstone_conveyor_llm"

default_args = {
    "owner": "airflow",
    "description": "Capstone - Airflow - DockerOperator",
    "depend_on_past": False,
    "start_date": datetime(2021, 5, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "capstonellm-dag-cristian",
    default_args=default_args,
    schedule_interval="5 * * * *",
    catchup=False,
) as dag:
    t1 = ConveyorSparkSubmitOperatorV2(
        task_id="operator-task-id",
        num_executors=1,
        driver_instance_type="mx.small",
        executor_instance_type="mx.small",
        aws_role=role,
        application="/opt/spark/work-dir/src/capstonellm/tasks/clean.py",
        application_args=[
            "-e", "test",
            "-t", "dbt"
        ]
    )

    t1
