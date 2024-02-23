from __future__ import annotations

import json
from pathlib import Path
from urllib import request

import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

TZ = "America/Panama"
LOCAL_TZ = pendulum.timezone(TZ)
GZIP_OUTPUT_PATH = "/tmp/wikipageview.gz"
JSON_OUTPUT_PATH = "/tmp/pageviews.json"
SQL_OUTPUT_PATH = "/tmp/pageviews.sql"
INTEREST_PAGENAMES = [
    "Meta",
    "Microsoft",
    "Apple",
    "Amazon",
    "Netflix",
    "Nvidia",
    "Google",
]

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


def _fetch_pageviews(pagenames, pageviews_file_path, output_path):
    result = dict.fromkeys(pagenames, 0)
    file_path = Path(pageviews_file_path).parent / Path(pageviews_file_path).stem
    with open(file=file_path, encoding="utf-8") as f:
        for line in f:
            domain_code, page_title, view_counts, _ = line.split(" ")
            if domain_code == "en" and page_title in pagenames:
                result[page_title] = view_counts
    with open(output_path, mode="w", encoding="utf-8") as f:
        content = json.dumps(result, indent=4)
        f.write(content)
        print("Results:", content, sep="\n")


fetch_pageviews = PythonOperator(
    task_id="fetch_pageviews",
    python_callable=_fetch_pageviews,
    op_kwargs={
        "pagenames": INTEREST_PAGENAMES,
        "pageviews_file_path": GZIP_OUTPUT_PATH,
        "output_path": JSON_OUTPUT_PATH,
    },
    dag=dag,
)


def _create_sql_query(pageviews_file_path, output_path):
    results = {}
    with open(pageviews_file_path, encoding="utf-8") as f:
        results = json.loads(f)
    with open(output_path, mode="w", encoding="utf-8") as f:
        f.write(
            (
                "CREATE TABLE IF NOT EXISTS pageviews_count ("
                "pagename       VARCHAR(255) NOT NULL,"
                "value          INT NOT NULL,"
                "insertion_date TIMESTAMP WITH TIMEZONE DEFAULT CURRENT_TIMESTAMP);\n"
            ),
        )
        for key, value in results:
            f.write(f"INSERT INTO pageviews_count VALUES ('{key}', {value}, {pendulum.now(tz=LOCAL_TZ)});\n")


create_sql_query = PythonOperator(
    task_id="create_sql_query",
    python_callable=_create_sql_query,
    op_kwargs={
        "pageviews_file_path": JSON_OUTPUT_PATH,
        "output_path": SQL_OUTPUT_PATH,
    },
)

# Execution Order
get_data >> extract_gz >> fetch_pageviews >> create_sql_query
