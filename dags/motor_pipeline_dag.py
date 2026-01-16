from datetime import datetime, timedelta
from pathlib import Path
from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount
import os

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Get the project directory from environment variable (set by docker-compose)
# This is the HOST path, not the container path, needed for DockerOperator volume mounts
PROJECT_DIR = os.getenv("AIRFLOW_PROJ_DIR", ".")

with DAG(
    "motor_ingestion_pipeline",
    default_args=default_args,
    description="Motor insurance policy ingestion pipeline",
    schedule=timedelta(days=1),
    start_date=datetime(2026, 1, 16),
    catchup=False,
    tags=["motor", "ingestion", "pyspark"],
) as dag:

    run_pipeline = DockerOperator(
        task_id="run_motor_pipeline",
        image="motor-ingestion-pipeline:latest",
        api_version="auto",
        auto_remove="success",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        mounts=[
            Mount(source=f"{PROJECT_DIR}/Data", target="/app/Data", type="bind"),
            Mount(
                source=f"{PROJECT_DIR}/metadata_motor.json",
                target="/app/metadata_motor.json",
                type="bind",
            ),
        ],
        command="python main.py",
    )

    run_pipeline
