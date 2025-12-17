import pandas as pd
import datetime as dt
from pathlib import Path
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

dag = DAG(
    dag_id="scheduled",
    schedule_interval="@daily", # 매일 자정에 실행
    # schedule_interval=dt.timedelta(days=3), # 3일마다 실행
    # schedule_interval=dt.timedelta(minutes=10), # 10분마다 실행
    start_date=dt.datetime(2025, 12, 17),
    end_date=dt.datetime(2025, 12, 25),
    catchup=False,
)

fetch_events = BashOperator(
    task_id="fetch_events",
    bash_command=(
        "mkdir -p /data/events && "
        "curl -o /data/events/{{ds}}.json "
        "http://events_api:5000/events?"
        "start_date={{ds}}&"
        "end_date={{next_ds}}"
    ),
    dag=dag,
)

# def _calulate_stats(input_path, output_path):
def _calulate_stats(**context):
    input_path = context['templates_dict']['input_path']
    output_path = context['templates_dict']['output_path']

    Path(output_path).parent.mkdir(exist_ok=True)

    events = pd.read_json(input_path)
    stats = events.groupby(['date', 'user']).size().reset_index()
    stats.to_csv(output_path, index=False)

calculate_stats = PythonOperator(
    task_id="calculate_stats",
    python_callable=_calulate_stats,
    templates_dict={
        "input_path": "/data/events/{{ds}}.json",
        "output_path": "/data/stats/{{ds}}.csv",
    },
    dag=dag,
)

def email_stats(stats, email):
    print(f"Sending stats to {email}...")

def _send_stats(email, **context):
    stats = pd.read_csv(context['templates_dict']['stats_path'])
    email_stats(stats, email)

send_stats = PythonOperator(
    python_callable=_send_stats,
    task_id="send_stats",
    op_kwargs={"email": "7942ahj@hanmail.net"},
    templates_dict={"stats_path": "/data/stats/{{ds}}.csv"},
    dag=dag,
)

fetch_events >> calculate_stats >> send_stats