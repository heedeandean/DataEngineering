from pathlib import Path

import airflow.utils.dates
from airflow import DAG
from airflow.sensors.python import PythonSensor

dag = DAG(
    dag_id="listing_02",
    start_date=airflow.utils.dates.days_ago(3),
    schedule_interval="0 16 * * *",
    description="A batch workflow for ingesting supermarket promotions data.",
    default_args={"depends_on_past": True},
)

def _wait_for_supermarket(supermarket_id):
    supermarket_path = Path(f'/data/{supermarket_id}')
    data_files = supermarket_path.glob('data-*.csv')
    sucess_file = supermarket_path / '_SUCCESS'
    return sucess_file.exists() and data_files

wait_for_supermarket_1 = PythonSensor(
    task_id='wait_for_supermarket_1',
    python_callable=_wait_for_supermarket,
    op_kwargs={'supermarket_id': 'supermarket1'},
    dag=dag,
)