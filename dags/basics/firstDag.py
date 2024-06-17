from airflow import DAG
from airflow.operators.bash import BashOperator

from datetime import datetime, timedelta

default_args = {
    'owner': 'griffith',
    'retires': 5,
    'retry_delay': timedelta(minutes=2)
}


with DAG(
    dag_id="first_dag_v5",
    default_args=default_args,
    description="first dag created in VSCode",
    start_date=datetime(2024, 6, 10, 2),
    schedule_interval='@daily'
) as dag:
    task1 = BashOperator(
        task_id='first_task',
        bash_command="echo This is the first task written in VSCode"
    )

    task2 = BashOperator(
        task_id='second_task',
        bash_command="echo This is the second task will run after task 1 is complete"
    )

    task3 = BashOperator(
        task_id='third_task',
        bash_command="echo This is task 3 which will run after task 1"
    )

    # task1.set_downstream(task2)
    # task1.set_downstream(task3)
    
    task1 >> [task2, task3]