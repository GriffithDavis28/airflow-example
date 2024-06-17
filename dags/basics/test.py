import json
import os
import pandas as pd
from config import db_config

def reading():

    df = pd.read_sql()


def filter_data(data):


    filteredData = []
    for item in data:
        filteredData = data.filter(items=["name", "email", "skills"])


    print("Filtered Data:\n",filteredData)

    return filteredData


def transform_data(filteredData):
    transformed_data = filteredData.copy()
    transformed_data = transformed_data.map(lambda x: x.upper() if isinstance(x, str) else x)
    transformed_data["skills"] = transformed_data["skills"].apply(
        lambda skills: [skill.upper() for skill in skills] if isinstance(skills, list) else skills
    )

    print("Transformed data:\n", transformed_data)


    return transformed_data


def store_data(transformed_data, file_path):
    transformed_data.to_json(file_path, orient='records', lines=True)
    print(f'Data in file {file_path}')


if __name__ == "__main__":
    file_path = "/home/davisgriffith/Analytics/Python/airflow-docker/dags/testing.json"
    saved_data="/home/davisgriffith/Analytics/Python/airflow-docker/dags/data.json"

    # Step 1: Read the data
    data = reading()

    if data is not None:
        # Step 2: Filter the data
        filtered_data = filter_data(data)

        # Step 3: Transform the data
        transformed_data = transform_data(filtered_data)

        # Step 4: Store the data
        store_data(transformed_data, saved_data)
