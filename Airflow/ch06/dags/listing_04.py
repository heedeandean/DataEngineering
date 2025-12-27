import datetime

import airflow.utils.dates
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.sensors.external_task import ExternalTaskSensor

dag1 = DAG(
    dag_id="listing_04_dag1", 
    start_date=airflow.utils.dates.days_ago(3), 
    schedule_interval="0 16 * * *",
)
dag2 = DAG(
    dag_id="listing_04_dag2",
    start_date=airflow.utils.dates.days_ago(3), 
    schedule_interval="0 20 * * *",
)

DummyOperator(task_id='etl', dag=dag1,) 

ExternalTaskSensor(
    task_id='wait_for_etl',
    external_dag_id='listing_04_dag1',
    external_task_id='etl',
    execution_delta=datetime.timedelta(hours=4), # schedule_interval 간격 맞추기
    dag=dag2,
)