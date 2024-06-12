import json
import os
from config import db_connection

def reading(file_path):

    file_exists = os.path.isfile(file_path)

    if file_exists:
        print("File exists")
        with open(file_path) as file:
            data = json.load(file)
            print(data)
        return data
    else:
        print("File does not exist")
        return None


def filter_data(data):
    keys_to_check = ["ID", "Weight_kg"]

    filteredData = []
    for item in data:
        filtered_data = {key: item[key] for key in keys_to_check if item in data}
        filteredData.append(filtered_data)

    print("Filtered data:", filteredData)

    return filteredData


def transform_data(filteredData):
    transformed_data = [{key.upper(): value for key, value in item.items()} for item in filteredData]

    print("Transformed data:", transformed_data)

    return transformed_data


def store_data(data, file_path):
    with open(file_path, 'w') as file:
        json.dump(data, file)
        print("Data stored..")


if __name__ == "__main__":
    file_path = "/home/davisgriffith/Analytics/Python/airflow-docker/dags/randomData.json"
    saved_data="/home/davisgriffith/Analytics/Python/airflow-docker/dags/data.json"

    # Step 1: Read the data
    data = reading(file_path)

    if data is not None:
        # Step 2: Filter the data
        filtered_data = filter_data(data)

        # Step 3: Transform the data
        transformed_data = transform_data(filtered_data)

        # Step 4: Store the data
        store_data(transformed_data, saved_data)
