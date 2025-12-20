from urllib import request
import airflow.utils.dates
from airflow import DAG
from airflow.operators.python import PythonOperator

dag = DAG(
    dag_id = "listing_02",
    start_date=airflow.utils.dates.days_ago(1),
    schedule_interval='@hourly',
)

def _get_data(output_path, **context):
    year, month, day, hour, *_ = context['execution_date'].timetuple()

    day = 1 # 임의 지정

    url = (
        "https://dumps.wikimedia.org/other/pageviews/"
        f"{year}/{year}-{month:0>2}/"
        f"pageviews-{year}{month:0>2}{day:0>2}-{hour:0>2}0000.gz"
    )
    request.urlretrieve(url, output_path)

get_data = PythonOperator(
    task_id='get_data',
    python_callable=_get_data,
    op_kwargs={"output_path": '/tmp/wikipageviews.gz'}, 
    dag=dag,
)