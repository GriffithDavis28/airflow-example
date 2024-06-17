from airflow.decorators import dag, task
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'retries': 5,
    'retry': timedelta(minutes=10)
}


@dag(dag_id='dag_with_api_v2',
     default_args=default_args,
     start_date=datetime.now(),
     schedule_interval=timedelta(minutes=5)
     )
def test_dag():
    @task(multiple_outputs=True)
    def get_name():
        return {
            'firstName': 'Griffith',
            'lastName': 'Davis'
        }

    @task()
    def get_age():
        return 24

    @task()
    def get_email():
        return 'griffith@gmail.com'

    @task()
    def get_details(fName, lName , Age, Email):
        print(f'Hello, my name is {fName} {lName}, I am {Age} years old, email is : {Email}')

    name_dict = get_name()
    age = get_age()
    email = get_email()
    get_details(fName = name_dict['firstName'], lName= name_dict['lastName'], Age=age, Email=email)


api_dag = test_dag()
