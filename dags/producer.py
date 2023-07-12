from airflow import DAG, Dataset
from airflow.decorators import task 


from datetime import datetime

my_file = Dataset('/tmp/my_file.txt')
my_file_2 = Dataset('/tmp/my_file_2.txt')

with DAG(
    dag_id='producer',
    schedule='@daily',
    start_date=datetime(2023, 1, 1),
    catchup=False
):
    @task(outlets=[my_file])
    def update_dataset():
        with open(my_file.uri, 'a+') as f:
            f.write('Producer Update')
    update_dataset()
    
    @task(outlets=[my_file_2])
    def update_dataset_2():
        with open(my_file_2.uri, 'a+') as f:
            f.write('Producer Update')
    update_dataset_2()