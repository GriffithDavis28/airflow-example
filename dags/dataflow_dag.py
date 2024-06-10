import json
from airflow import DAG
from airflow.operators.python import PythonOperator

from datetime import datetime, timedelta


default_args =(
    {
        'owner': 'airflow',
        'retires':5,
        'retry_delay':timedelta(minutes=5)
    }
)

def readFromFile():
    with open("/home/davisgriffith/Analytics/Python/airflow-docker/data.json", 'r')as file:
        data = json.load(file)

    return data

def filteringData(data):
    
    keys_to_check=['name', 'age', 'email', 'occupation', 'skills']

    filteredData={key: data[key] for key in keys_to_check if key in data}
    
    return filteredData

def transformation(filteredData):
    
    transformedData = {key: (value.lower() if isinstance(value, str) else value)for key, value in filteredData.items()}

    print(transformedData)

    print(transformedData.items())

with DAG(
    dag_id="data_flow",
    default_args=default_args,
    description="reading data from a local .json file",
    start_date=datetime(2024, 6, 11, 2),
    schedule_interval='@daily',
) as dag:
    task1 = PythonOperator(
        task_id="reading_data",
        python_callable=readFromFile
    )

    task2 = PythonOperator(
        task_id="filtering_data",
        python_callable=filteringData
    )

    task3 = PythonOperator(
        task_id="transforming_data",
        python_callable=transformation
    )

    task1>>task2>>task3