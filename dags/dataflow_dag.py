import json
import os
from airflow import DAG
from airflow.operators.python import PythonOperator

from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}


def reading(**kwargs):
    
    file_path = "dags/testing.json"
    fileExists = os.path.isfile(file_path)
    
    if(fileExists!=False):
        print ("File exists")
        with open(file_path)as file:
            data=json.load(file)
            print(data)
        kwargs['ti'].xcom_push(key='data', value=data)
        print(kwargs['ti'].xcom_push(key='data', value=data))
    else:
        print ("File does not exist")
    
    

def filterData(**kwargs):
    
    ti = kwargs['ti']
    data=ti.xcom_pull(key='data', task_ids='read_data')
    
    keys_to_check=["name", "email", "skills"]
    
    filteredData = {key: data[key] for key in keys_to_check if key in data}
    
    print("filtered data", filteredData)
    
    ti.xcom_push(key='filteredData', value=filteredData)
    


def transformData(**kwargs):
    ti=kwargs['ti']
    filteredData=ti.xcom_pull(key='filteredData', task_ids='filter_data')
    
    transformData ={key: (value.upper() if isinstance (value, str) else value)for key, value in filteredData.items()}
    
    print(transformData)
    
    ti.xcom_push(key='transformedData', value=transformData)


def storeData(**kwargs):
    
    ti=kwargs['ti']
    storage = ti.xcom_pull(key='transformedData', task_ids='transform_data')
    
    with open("dags/data.json", 'w') as file:
        json.dump(storage, file)
        print("Data stored..")
    
    
    
with DAG(
    dag_id="example_data_flow_dag",
    description="Testing dag before pushing working code",
    default_args=default_args,
    start_date=datetime(2024, 6, 11, 8),
    schedule_interval='@daily',
)as dag:
    
    task1 = PythonOperator(
        task_id="read_data",
        python_callable=reading,
        # provide_context=True
    )
    task2 = PythonOperator(
        task_id="filter_data",
        python_callable=filterData,
        # provide_context=True
    )
    task3 = PythonOperator(
        task_id="transform_data",
        python_callable=transformData,
        # provide_context=True
    )
    task4 = PythonOperator(
        task_id="store_data",
        python_callable=storeData,
        # provide_context=True
    )
    
    task1>>task2>>task3>>task4