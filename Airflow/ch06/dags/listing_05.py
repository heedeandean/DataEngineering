import airflow.utils.dates
from airflow import DAG
from airflow.operators.python import PythonOperator

dag = DAG(
    dag_id="print_dag_run_conf",
    start_date=airflow.utils.dates.days_ago(3),
    schedule_interval=None,
)

def print_conf(**context):
    print(f"DAG Run Configuration:{context['dag_run'].conf}")

process = PythonOperator(
    task_id='process',
    python_callable=print_conf,
    dag=dag,
)

# Airflow 외부에서 DAG를 트리거.
# 방법 1. Airflow CLI 명령어를 사용하여 DAG 실행(트리거):
# airflow dags trigger -c '{"supermarket_id": 1}' print_dag_run_conf 

# 방법 2. Airflow REST API를 사용하여 DAG 실행(트리거):
# curl -u admin:admin -X POST "http://localhost:8080/api/v1/dags/print_dag_run_conf/dagRuns" -H "Content-Type: application/json" -d '{"conf": {"supermarket_id": 1}}'