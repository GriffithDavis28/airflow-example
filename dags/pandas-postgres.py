import json
import logging
import pandas as pd
import psycopg2
import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from config.db_config import db_connection


from datetime import datetime, timedelta


default_args = {
    'owner': 'airflow',
    'retires':5,
    'retry_delay':timedelta(minutes=5)
}

def readData(**kwargs):
    
    filePath ="dags/randomData.csv"
    
    fileExists = os.path.isfile(filePath)
    
    if(fileExists):
        data = pd.read_csv(filePath)
        logging.info("Data Read Successfully")
        print(data)
        
        data_json = pd.json_normalize(data.to_dict(orient='records'))
        
        print(data_json)
        
        kwargs['ti'].xcom_push(key='data', value=data_json.to_json(orient='records'))
    else:
        logging.error("File does not exist")
        


def storeData(**kwargs):
    try:
        conn = psycopg2.connect(
            host=db_connection["host"],
            port=db_connection["port"],
            database=db_connection["database"],
            user=db_connection["username"],
            password=db_connection["password"]
        )
        logging.info("///...Local DB connected...///")
    except Exception as e:
        logging.error(f'Error connecting to the database: {e}')
        return
    
    cursor = conn.cursor()
    ti = kwargs['ti']
    records = ti.xcom_pull(key='data', task_ids='read_data')

    try:
        entry_records = json.loads(records)
    except json.JSONDecodeError as e:
        logging.error(f'Error decoding JSON: {e}')
        cursor.close()
        conn.close()
        return
    
    if isinstance(entry_records, list):
        for record in entry_records:
            try:
                cursor.execute("""
                    INSERT INTO public.random_data
                        (id, name, email, age, join_date)
                    VALUES (%d, %s, %s, %d, %s)
                """, (int(record['ID']), record['Name'], record['Email'], int(record['Age']), record['Join date']))
                logging.info("Data stored...")
            except Exception as e:
                logging.error(f'Error inserting data: {e}')
            conn.commit()
    else:
        logging.error("Something went wrong stupid ")
    
    cursor.close()
    conn.close()

    


with DAG(
    dag_id="panda-postgres-dag_v3",
    description="Testing with Pandas framework",
    default_args=default_args,
    start_date=datetime(2024, 6, 13, 1),
    schedule_interval="@daily",
    ) as dag:
    task1 = PythonOperator(
        task_id="read_data",
        python_callable=readData
    )
    task2 = PythonOperator(
        task_id="store_data",
        python_callable=storeData
    )
    
    task1>>task2