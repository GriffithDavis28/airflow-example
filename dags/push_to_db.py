import logging

import psycopg2
from airflow.models import TaskInstance

from config.db_config import db_connection
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from datetime import datetime, timedelta
from airflow.operators.empty import EmptyOperator
from airflow.models.dagrun import DagRun
from airflow.utils.state import State
from airflow import settings

default_args = {
    'owner': 'airflow',
    'retries': 5,
    'retry_delay': timedelta()
}

def store_data():
    conn = psycopg2.connect(
        host=db_connection['host'],
        port=db_connection['port'],
        database=db_connection['database'],
        user=db_connection['username'],
        password=db_connection['password']
    )
    logging.info("Connection established///......")

    session = settings.Session()
    skipped_tasks=session.query(TaskInstance).filter(TaskInstance.state=='skipped').all()

    cursor = conn.cursor()

    insert_query = """
        INSERT INTO public.skipped_data
        (id, dag_id, task_id, state, last_updated, skips, allowe_skips)
        VALUES(nextval('skipped_data_id_seq'::regclass), '%s', '%s', '%s', CURRENT_TIMESTAMP, %d, %d);
        """

dag = DAG(
    dag_id='check_status_v1',
    schedule_interval=timedelta(minutes=5),
    start_date=datetime.astimezone(datetime.now()).now(),
    default_args=default_args
)

start = EmptyOperator(
    task_id='start'
)

end = EmptyOperator(
    task_id='end'
)


def check_prev_dag(dag_id, **kwargs):
    DAG_RUN = DagRun.find(dag_id, state=State.FAILED)

    if DAG_RUN:
        return 'skipped_run'
    else:
        return 'check_status'


def printStatus(**kwargs):
    logging.info('Status: checking status...')


def skippedState(**kwargs):
    logging.info('Status: skipped...')


check_prev_status = BranchPythonOperator(
    task_id='check_prev_status',
    python_callable=check_prev_dag,
    op_kwargs={
        'dag_id': dag.dag_id
    },
    dag=dag
)

check_status = PythonOperator(
    task_id='check_status',
    python_callable=printStatus,
    dag=dag
)


skip_status = PythonOperator(
    task_id='skipped_run',
    python_callable=skippedState,
    op_kwargs={
        'dag_id': dag.dag_id,
        'task_id': 'skipped_run_status'
    },
    dag=dag
)

start >> check_prev_status >> [check_status, skip_status] >> end
