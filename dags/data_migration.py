import json
import logging
import os
import psycopg2

from config.db_config import db_connection
from airflow import DAG
from airflow.operators.python import PythonOperator

from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}


def reading(**kwargs):
    
    file_path = "dags/newData.json"
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
    
    try:
        conn = psycopg2.connect(
            dbname=db_connection["database"],
            user=db_connection["username"],
            password=db_connection["password"],
            host=db_connection["host"],
            port=db_connection["port"]
        )
        logging.info("Connection established...")
    except Exception as e:
        logging.error(f"Error connecting to db..{e}")
        return
    
    ti=kwargs['ti']
    record = ti.xcom_pull(key='transformedData', task_ids='transform_data')
    
    
    
    cursor=conn.cursor()
    skills = json.dumps(record["skills"])
    try:
        cursor.execute("""
                        INSERT INTO users (name, email, skills) VALUES (%s, %s, %s)
                        """, (record["name"], record["email"], skills))
        
    except Exception as e:
        logging.error(f'Something happened while executing query...{e}')
    
    
    conn.commit()
    cursor.close()
    conn.close()
    logging.info("Data storage is done correctly...")
    
    
with DAG(
    dag_id="migration_v3",
    description="Testing dag before pushing working code",
    default_args=default_args,
    start_date=datetime(2024, 6, 13, 11),
    schedule_interval=timedelta(hours=2),
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