import logging

import psycopg2

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from config.db_config import db_connection
from datetime import datetime, timedelta
from airflow.models import TaskInstance

default_args = {
    'owner': 'airflow',
    'retries': 5,
    'retry_delay': timedelta(minutes=10)
}


def establishConnection():
    try:
        conn = psycopg2.connect(
            host=db_connection["host"],
            port=db_connection["port"],
            user=db_connection["username"],
            password=db_connection["password"],
            database=db_connection["database"]
        )

        logging.info("Connection established....")
        print("Connected...")
        conn.close()

        # kwargs['ti'].xcom_push(key='status', value='success')
    except Exception as e:
        logging.error(f'Connection not established....reason: {e}')
        # kwargs['ti'].xcom_push(key='status', value='failed')
        print(f"Error///{e}")
        return

#
# check_previous_run_status = BranchPythonOperator(
#     task_id="check_previous_run_status",
#     python_callable=slack_notif_failure,
#     op_kwargs={"dag_id": dag.dag_id, "task_id": 'check_previous_run_status',
#                "func": check_last_run_status, "kwargs": {'data_stream_name': 'crm_campaign_run_stream',
#                                                          'region': region,
#                                                          'success_condition':
#                                                              'start_run'}},
#     dag=dag)


with DAG(
    dag_id="example_status_dag_run",
    default_args=default_args,
    schedule_interval=timedelta(minutes=15),
    start_date=datetime(2024, 6, 15, 10),
    description="Checking dag status"
    ) as dag:
        task1=DummyOperator(
            task_id='start',
            
        )
    
# with DAG(
#         dag_id='example_status_dag_1',
#         description="checking status of the dag",
#         schedule_interval=timedelta(minutes=10),
#         start_date=datetime(2024, 6, 14, 14, 33),
#         default_args=default_args
# ) as dag:
#     task1 = PythonOperator(
#         task_id="establish_connection",
#         python_callable=establishConnection
#     )
#     task2 = PythonOperator(
#         task_id="check_status",
#         python_callable=check_state
#     )
#
#     task1 >> task2
