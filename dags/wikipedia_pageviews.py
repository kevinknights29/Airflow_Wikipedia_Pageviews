from __future__ import annotations

from urllib import request

import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

TZ = "America/Panama"
LOCAL_TZ = pendulum.timezone(TZ)
GZIP_OUTPUT_PATH = "/tmp/wikipageview.gz"

dag = DAG(
    dag_id="wikipedia_pageviews",
    start_date=pendulum.datetime(2024, 1, 1, tz=LOCAL_TZ),
    schedule_interval=None,
)


def _get_data(output_path, **context):
    year, month, day, hour, *_ = LOCAL_TZ.convert(context["data_interval_start"]).timetuple()
    url = (
        "https://dumps.wikimedia.org/other/pageviews/"
        f"{year}/{year}-{month:0>2}/"
        f"pageviews-{year}{month:0>2}{day:0>2}-{hour:0>2}0000.gz"
    )
    request.urlretrieve(url, output_path)


get_data = PythonOperator(
    task_id="get_data",
    python_callable=_get_data,
    op_kwargs={"output_path": GZIP_OUTPUT_PATH},
    dag=dag,
)

extract_gz = BashOperator(
    task_id="extract_gz",
    bash_command="gunzip --force $path",
    env={"path": GZIP_OUTPUT_PATH},
    dag=dag,
)

# Execution Order
get_data >> extract_gz
