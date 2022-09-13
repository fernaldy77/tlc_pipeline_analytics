from datetime import datetime, timedelta
from textwrap import dedent

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.bash import BashOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.models import Variable

with DAG(
    'agg_fhv_monthly',
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
    description='Aggregation step for fhv data',
    schedule_interval='00 17 1 * *',
    start_date=datetime(2019, 12, 1),
    catchup=True,
    tags=['tlc_analytics'],
) as dag:

    ## Wait for extract process success
    fhv_data_sensor = ExternalTaskSensor(
        task_id='fhv_data_sensor',
        poke_interval=60,
        timeout=180,
        soft_fail=False,
        retries=2,
        external_task_id='move_fhv_hdfs',
        external_dag_id='extract_fhv_raw_monthly',
        dag=dag
    )

    ## Simple transformation process using Spark
    user = Variable.get('user')
    month_partition = '{{ macros.ds_format(macros.ds_add(ds, -1), "%Y-%m-%d", "%Y-%m") }}'
    agg_fhv_trip = BashOperator(
        task_id='agg_fhv_trip',
        depends_on_past=False,
        bash_command=f"""
            sudo -u {user} /opt/spark3/bin/spark-submit --jars=/opt/airflow/dags/lib/postgresql-42.3.5.jar /opt/airflow/dags/agg_fhv_monthly/python/agg_fhv_trip.py {month_partition}
        """,
    )

    fhv_data_sensor >> agg_fhv_trip