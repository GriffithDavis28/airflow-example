from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator


default_args =(
    {
        'owner': 'airflow',
        'retires':5,
        'retry_delay':timedelta(minutes=5)
    }
)


def greet():
    print("Hello, World!")


def dyncamicGreet(name):
    print(f'Hello {name}')


with DAG(
    dag_id="new_dag_2",
    default_args=default_args,
    description="new dag created with PythonOperator",
    start_date=datetime(2024, 6,10, 2),
    schedule_interval='@daily'
) as dag:
    task1 = PythonOperator(
        task_id="greet",
        python_callable=greet
        )
    
    task2 = PythonOperator(
        task_id="greeting_person",
        python_callable=dyncamicGreet,
        op_kwargs={'name': 'John'}
    )
    
    task1 >> task2
