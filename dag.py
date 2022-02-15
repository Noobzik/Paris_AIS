import airflow
from datetime import datetime, timedelta
from subprocess import Popen, PIPE
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
import requests
from airflow import DAG, macros
from airflow.decorators import task
import os

YEAR = '{{ macros.ds_format(ds, "%Y-%m-%d", "%Y") }}'
MONTH = '{{ macros.ds_format(ds, "%Y-%m-%d", "%m") }}'
YESTERDAY = '{{ macros.ds_format(macros.ds_add(ds, 1), "%Y-%m-%d", "%d") }}'

dag = DAG(
    # TODO : compléter le nom du DAG avec votre nom de gorupe dedans
    dag_id='',
    # TODO : compléter le schedule interval
    schedule_interval='',
    max_active_runs=1,
    start_date=datetime(2022, 2, 14),
)

@task(task_id='download_raw_data', dag=dag)
def download_raw_data(year, month, day):
    # TODO : compléter la valeur
    raw_data_path = ""

    url = f'https://opendata.paris.fr/api/v2/catalog/datasets/comptages-routiers-permanents/exports/csv?refine=t_1h%3A{year}%2F{month}%2F{day}&timezone=UTC'
    r = requests.get(url, allow_redirects=True)
    open(f'/tmp/data-{year}-{month}-{day}.csv', 'wb').write(r.content)
    put = Popen(["hadoop", "fs", "-put", f"/tmp/data-{year}-{month}-{day}.csv", raw_data_path], stdin=PIPE, bufsize=-1)
    put.communicate()
    os.remove(f'/tmp/data-{year}-{month}-{day}.csv')
    print("ok")


download_raw_data = download_raw_data(YEAR, MONTH, YESTERDAY)

clean_data = BashOperator(
    task_id="spark_job_clean",
    # TODO : compléter le spark submit
    bash_command="",
    dag=dag
)

transform_data = BashOperator(
    task_id="spark_job_transform",
    # TODO : compléter le spark submit
    bash_command=f"",
    dag=dag
)

download_raw_data >> clean_data >> transform_data
