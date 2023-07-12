from airflow import DAG
from airflow.operators.bash import BashOperator
from groups.group_downloads import downloads_task
from groups.group_transforms import transforms_task
 
from datetime import datetime
 
with DAG('group_dag', start_date=datetime(2022, 1, 1), 
    schedule_interval='@daily', catchup=False) as dag: 
  
    downloads = downloads_task()
 
    check_files = BashOperator(
        task_id='check_files',
        bash_command='sleep 10'
    )
 
    transforms = transforms_task()
 
    downloads >> check_files >> transforms