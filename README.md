# Airflow_Wikipedia_Pageviews

This project implements the Airflow DAG presented in chapter 4 of the book [_Data Pipelines with Apache Airflow_ by B. Harenslak and J. de Ruiter](https://amzn.to/49qSLIV)

## Results

This pipeline fetches page views from `https://dumps.wikimedia.org/`.

Pages of interest are:

- Meta
- Microsoft
- Apple
- Amazon
- Netflix
- Nvidia
- Google

Overall pipeline runs in less than **20 seconds**. This includes fetching results as zip, unziping, processing, inserting to postgress, and analytics.

![image](https://github.com/kevinknights29/Airflow_Wikipedia_Pageviews/assets/74464814/b84abad0-a8f6-4a54-bc7f-b1ddb3b6041e)

## Prerequisites

- [ ] Have Docker installed

    To install check: [Docker Dekstop Install](https://www.docker.com/products/docker-desktop/)

- [ ] Have Astro CLI installed

    If you use brew, you can run: `brew install astro`

    For other systems, please refer to: [Install Astro CLI](https://docs.astronomer.io/astro/cli/install-cli)

## Getting Started

1. Run `astro dev init` to create the necessary files for your environment.

2. Run `astro dev start` to start the airflow service with docker.

3. Configure Postrges connection by following this steps:

    1. Run `astro dev bash` to access airflow terminal.

    2. Run the following command to add the connection:

        ```bash
        airflow connections add \
        --conn-type postgres \
        --conn-host host.docker.internal \
        --conn-login postgres \
        --conn-password postgres \
        postgres_default
        ```

    Here using localhost will create an error. For an in depth explanation check: [Connect to local Postgres from docker airflow](https://stackoverflow.com/questions/72452675/connect-to-local-postgres-from-docker-airflow)

## Execution

To execute DAG, please visit: [Airflow UI](http://localhost:8080/)

In the DAGs section, you should see a DAG called `wikipedia_pageviews`.

![image](https://github.com/kevinknights29/Airflow_Wikipedia_Pageviews/assets/74464814/411871d2-a9c5-4249-a16e-6e3018d9c925)

> NOTE: Your run section will be empty instead of the colored options you see in the image.

Click the dag to open it, and to run it click the trigger `play` button in the top right side.

![image](https://github.com/kevinknights29/Airflow_Wikipedia_Pageviews/assets/74464814/7e19d15a-f857-4f1a-9724-591523568d39)

To take at the process flow of the pipeline. Select the `Graph` view.

![image](https://github.com/kevinknights29/Airflow_Wikipedia_Pageviews/assets/74464814/d36a038f-51bf-4ad8-b6da-02589c30652e)

## Project Structure

```text
.
├── Dockerfile
├── LICENSE
├── README.md
├── dags
│   ├── sql
│   │   └── most_popular_hour_per_page.sql
│   └── wikipedia_pageviews.py
├── packages.txt
├── pyproject.toml
├── requirements.txt
└── tests
    └── dags
        └── test_dag_example.py
```

Generated with: `tree --gitignore --prune`

### Have fun! 😄

## Reference

- [_Data Pipelines with Apache Airflow_ by B. Harenslak and J. de Ruiter](https://amzn.to/49qSLIV)
- [Develop your Astro project](https://docs.astronomer.io/astro/cli/develop-project)
- [Airflow Docs](https://airflow.apache.org/docs/apache-airflow/stable/index.html)
- [TemplateNotFound error when running simple Airflow BashOperator](https://stackoverflow.com/questions/42147514/templatenotfound-error-when-running-simple-airflow-bashoperator)
- [How to Change the Timezone of a Postgres Database](https://www.commandprompt.com/education/how-to-change-the-timezone-of-a-postgres-database/)
- [Airflow PostgresHook Example](https://gist.github.com/antweiss/a6716339983bcc93aa505fd0c620b013)
- [Start a process when the container starts
](https://code.visualstudio.com/remote/advancedcontainers/start-processes)
- [Read JSON file using Python](https://www.geeksforgeeks.org/read-json-file-using-python/)
- [Reading and Writing JSON to a File in Python](https://www.geeksforgeeks.org/reading-and-writing-json-to-a-file-in-python/)
- [Passing a command line argument to airflow BashOperator](https://stackoverflow.com/questions/42016491/passing-a-command-line-argument-to-airflow-bashoperator)
- [Templates reference](https://airflow.apache.org/docs/apache-airflow/stable/templates-ref.html)
- [Time Zones](https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/timezone.html)
