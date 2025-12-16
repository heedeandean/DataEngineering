import datetime as dt
import pandas as pd
from pathlib import Path
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

dag = DAG(
    dag_id="01_unscheduled",
    start_date=dt.datetime(2024, 1, 1),
    schedule_interval=None, 
)

fetch_events = BashOperator(
    task_id="fetch_events",
    bash_command=(
        "mkdir -p /data && "
        "curl -0 /data/events.json "
        "http://localhost:5000/events"
    ),
    dag=dag,
)