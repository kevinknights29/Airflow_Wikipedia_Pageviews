from __future__ import annotations

from urllib import request

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator

tz = "America/Panama"
local_tz = pendulum.timezone(tz)
dag = DAG(
    dag_id="wikipedia_pageviews",
    start_date=pendulum.datetime(2024, 1, 1, tz=local_tz),
    schedule_interval=None,
)


def _get_data(execution_date):
    year, month, day, hour, *_ = local_tz.convert(execution_date).timetuple()
    url = (
        "https://dumps.wikimedia.org/other/pageviews/"
        f"{year}/{year}-{month:0>2}/"
        f"pageviews-{year}{month:0>2}{day:0>2}-{hour:0>2}0000.gz"
    )
    output_path = "/tmp/wikipageview.gz"
    request.urlretrieve(url, output_path)


get_data = PythonOperator(
    task_id="get_data",
    python_callable=_get_data,
    dag=dag,
)
