from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
import os

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
    "capstonellm-dag",
    default_args=default_args,
    schedule_interval="5 * * * *",
    catchup=False,
) as dag:
    t1 = DockerOperator(
        task_id="docker_command_sleep",
        image="my-python-app",
        container_name="container",
        api_version="auto",
        auto_remove=True,
        command="python3 -m capstonellm.tasks.clean -e local -t dbt",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        environment={
            'AWS_ACCESS_KEY_ID': os.getenv("AWS_ACCESS_KEY_ID"),
            'AWS_SECRET_ACCESS_KEY': os.getenv("AWS_SECRET_ACCESS_KEY"),
            'AWS_SESSION_TOKEN': os.getenv("AWS_SESSION_TOKEN")
        }
    )
