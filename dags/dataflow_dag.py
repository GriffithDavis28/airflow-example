import json
import os
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


def readData(**kwargs):
    # data={
    # "name": "John Doe",
    # "age": 30,
    # "city": "New York",
    # "gender": "Male",
    # "email": "john.doe@example.com",
    # "phone": "+1-555-555-5555",
    # "address": "123 Main St, New York, NY 10001",
    # "occupation": "Software Engineer",
    # "company": "Tech Solutions",
    # "salary": 90000,
    # "marital_status": "Single",
    # "nationality": "American",
    # "birthdate": "1994-05-15",
    # "hobbies": ["reading", "hiking", "photography"],
    # "languages": ["English", "Spanish"],
    # "education": "Bachelor's Degree in Computer Science",
    # "experience": 8,
    # "skills": ["JavaScript", "Python", "React"],
    # "linkedin": "https://www.linkedin.com/in/johndoe",
    # "twitter": "@johndoe"
    #     }
    
    filePath = "/home/davisgriffith/Analytics/Python/airflow-docker/dags/data.json"
    fileExists = os.path.isfile(filePath)
    if(fileExists):
        print(fileExists, "File exists..continue the process")
    else:
        print(fileExists, "File does not exist --- Failure")
    
    with open(filePath, 'r') as file:
        data = json.load(file)
        print("Data coming in: ", data)
    kwargs['ti'].xcom_push(key='data', value=data)
# def readFromFile():
#     filePath="./data.json"
#     if not os.path.exists(filePath):
#         print("no file")
#     else:
#         with open(filePath, 'r')as file:
#             data = json.load(file)
#             print("Data has been read successfully...")
            
#         kwargs['ti'].xcom_push(key='data', value=data)

def filteringData(**kwargs):
    
    ti = kwargs['ti']
    data = ti.xcom_pull(key='data', task_ids='reading_data')
    
    keys_to_check=['name', 'age', 'email', 'occupation', 'skills']
    print("Data received in filteringData:", data)
    filteredData={key: data[key] for key in keys_to_check if key in data}
    
    print("Filtered data is: ", filteredData)
    if data is None:
        raise ValueError("No data found. Make sure the readFromFile task completed successfully.")
    
    ti.xcom_push(key='filteredData', value=filteredData)

def transformation(**kwargs):
    
    ti = kwargs['ti']
    filteredData = ti.xcom_pull(key='filteredData', task_ids='filtering_data')
    if filteredData is None:
        raise ValueError("No filtered data found. Make sure the filteringData task completed successfully.")
    
    transformedData = {key: (value.lower() if isinstance(value, str) else value)for key, value in filteredData.items()}
    print("Transformed Data: ", transformedData)
    ti.xcom_push(key='transformedData', value=transformedData)

def storageOfData(**kwargs):
    
    ti = kwargs['ti']
    print(ti)
    transformedData = ti.xcom_pull(key='transformedData', task_ids='transforming_data')
    print(transformedData)
    if transformedData is None:
        raise ValueError("No transformed data found. Make sure the transformation task completed successfully.")
    
    storedData = {}
    
    storedData.update(transformedData)
    
    print(storedData)
    # with open("/home/davisgriffith/Analytics/Python/airflow-docker/storeData.json", 'w') as file:
    #     json.dump(transformedData, file, indent=4)
    #     print("Data has been stored successfully...")

    



with DAG(
    dag_id="data_flow_v2",
    default_args=default_args,
    description="reading data from a local .json file",
    start_date=datetime(2024, 6, 10),
    schedule_interval='@daily',
) as dag:
    task1 = PythonOperator(
        task_id="reading_data",
        python_callable=readData,
        provide_context=True
    )

    task2 = PythonOperator(
        task_id="filtering_data",
        python_callable=filteringData,
        provide_context=True
    )

    task3 = PythonOperator(
        task_id="transforming_data",
        python_callable=transformation,
        provide_context=True
    )

    task4 = PythonOperator(
        task_id="storing_data",
        python_callable=storageOfData,
        provide_context=True
    )

    task1>>task2>>task3>>task4