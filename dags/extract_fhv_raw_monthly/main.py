from datetime import datetime, timedelta
from textwrap import dedent
import requests

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.bash import BashOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.sensors.filesystem import FileSensor
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import ShortCircuitOperator, PythonOperator
import logging

with DAG(
    'extract_fhv_raw_monthly',
    # These args will get passed on to each operator
    # You can override them on a per-task basis during operator initialization
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 3,
        'retry_delay': timedelta(minutes=5),
    },
    description='Extract step for fhv raw data',
    schedule_interval='00 17 1 * *',
    start_date=datetime(2019, 12, 1),
    catchup=True,
    tags=['tlc_analytics'],
) as dag:
    ## Check if data exists using API
    def check_raw(**kwargs):
        # Based on documentation, the data is uploaded 2 months later. So we need to get 2 months ago
        month_partition = kwargs.get('templates_dict').get('month_partition')

        # Url of data
        url = f'https://d37ci6vzurychx.cloudfront.net/trip-data/fhvhv_tripdata_{month_partition}.parquet'
        date_partition = kwargs.get('templates_dict').get('date_partition')
        try:
            print("Url: " + url)
            # Get Url
            get = requests.get(url)
            # if the request succeeds
            if get.status_code == 200:
                return True
            else:
                raise Exception()

        # Exception
        except requests.exceptions.RequestException as e:
            # print URL with Errs
            raise Exception()

    check_fhv_raw = PythonOperator(
        task_id='check_fhv_raw',
        provide_context=True,
        python_callable=check_raw,
        templates_dict={
            'month_partition': '{{ macros.ds_format(macros.ds_add(ds, -1), "%Y-%m-%d", "%Y-%m") }}',
            'date_partition': '{{ ds }}'
        },
        dag=dag
    )

    # Download data into server
    download_fhv_data = BashOperator(
        task_id='download_fhv_data',
        bash_command="""
             wget -P /opt/airflow/dags/extract_fhv_raw_monthly/data/staging https://d37ci6vzurychx.cloudfront.net/trip-data/fhv_tripdata_{{ macros.ds_format(macros.ds_add(ds, -1), "%Y-%m-%d", "%Y-%m") }}.parquet
        """,
        dag = dag
    )

    # Move into HDFS
    move_fhv_hdfs = BashOperator(
        task_id='move_fhv_hdfs',
        bash_command="""
            /opt/hadoop/bin/hdfs dfs -mkdir -p /data/tlc_analytics/staging/{{ macros.ds_format(macros.ds_add(ds, -1), "%Y-%m-%d", "%Y-%m") }}/fhv &&
            /opt/hadoop/bin/hdfs dfs -put /opt/airflow/dags/extract_fhv_raw_monthly/data/staging/fhv_tripdata_{{ macros.ds_format(macros.ds_add(ds, -1), "%Y-%m-%d", "%Y-%m") }}.parquet /data/tlc_analytics/staging/{{ macros.ds_format(macros.ds_add(ds, -1), "%Y-%m-%d", "%Y-%m") }}/fhv
        """,
        dag=dag
    )

    check_fhv_raw >> download_fhv_data >> move_fhv_hdfs
