from __future__ import annotations

import json
from pathlib import Path
from urllib import request

import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator

TZ = "America/Panama"
LOCAL_TZ = pendulum.timezone(TZ)
GZIP_OUTPUT_PATH = "/tmp/wikipageview.gz"
JSON_OUTPUT_PATH = "/tmp/pageviews.json"
SQL_OUTPUT_PATH = "/tmp/pageviews.sql"
SQL_ANALYTICS_PATH = "sql/most_popular_hour_per_page.sql"
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
        results = json.loads(f.read())
    with open(output_path, mode="w", encoding="utf-8") as f:
        f.write("ALTER DATABASE postgres SET timezone TO 'America/Panama';\n")
        f.write(
            (
                "CREATE TABLE IF NOT EXISTS pageviews_count ("
                "pagename       VARCHAR(255) NOT NULL,"
                "value          INT NOT NULL,"
                "insertion_date TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP);\n"
            ),
        )
        for key, value in results.items():
            f.write(
                (
                    f"INSERT INTO pageviews_count VALUES ('{key}', {value},"
                    f" '{pendulum.now(tz=LOCAL_TZ).strftime('%Y-%m-%d %H:%M:%S%z')}');\n"
                ),
            )


create_sql_query = PythonOperator(
    task_id="create_sql_query",
    python_callable=_create_sql_query,
    op_kwargs={
        "pageviews_file_path": JSON_OUTPUT_PATH,
        "output_path": SQL_OUTPUT_PATH,
    },
)

write_to_postgres = PostgresOperator(
    task_id="write_to_postgres",
    sql=open(SQL_OUTPUT_PATH).read(),
    postgres_conn_id="postgres_default",
    dag=dag,
)


def _get_page_analytics(sql_query, postgres_conn_id="postgres_default"):
    pg_hook = PostgresHook().get_hook(conn_id=postgres_conn_id)
    results = pg_hook.get_records(sql_query)
    print(results)


get_page_analytics = PythonOperator(
    task_id="get_page_analytics",
    python_callable=_get_page_analytics,
    op_kwargs={
        "sql_query": open(SQL_ANALYTICS_PATH).read(),
    },
    dag=dag,
)

# Execution Order
get_data >> extract_gz >> fetch_pageviews >> create_sql_query >> write_to_postgres >> get_page_analytics
