#!/bin/bash

set -x

SCRIPT_DIR=$(cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd)

docker run -d \
-it \
-p 8080:8080 \
-v ${SCRIPT_DIR}/../dags/download_rocket_launches.py:/opt/airflow/dags/download_rocket_launches.py \
--entrypoint /bin/bash \
--name airflow \
apache/airflow:2.0.0-python3.8 \
-c '(\
    airflow db init && \
    airflow users create \
        --username admin \
        --password admin \
        --firstname Anonymous \
        --lastname Admin \
        --role Admin \
        --email admin@example.org \
    ); \
    airflow webserver & \
    airflow scheduler \
'