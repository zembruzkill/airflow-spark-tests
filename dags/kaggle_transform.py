# Importing the necessary libraries
import json
import math
import os
from datetime import datetime
from glob import glob

import kaggle
import pandas as pd
from airflow import DAG, AirflowException
from airflow.operators.python import PythonOperator
from airflow.providers.microsoft.azure.transfers.local_to_wasb import LocalFilesystemToWasbOperator
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobClient, BlobServiceClient, ContainerClient

from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator 


default_args = {
    'owner': 'luciano.zembruzki'
}


# Define a function that fetches Azure credentials from a JSON file
def get_azure_credentials(filename):
    # Opening and reading the JSON file
    with open(filename, 'r') as f:
        data = json.load(f)

    return data['connection_string']

# Call the function with the specified JSON file path to get the Azure connection string
AZURE_CONNECTION_STRING = get_azure_credentials('/opt/airflow/secure_credentials/azure_credentials.json')

# Define a function to list out blob filenames available in Azure 
def _list_files_on_azure(ti):
    # Connect to the Azure blob storage
    blob_service_client = BlobServiceClient.from_connection_string(AZURE_CONNECTION_STRING)

    # Get list of blobs in 'bronze' container and append blob names to a list
    container_client = blob_service_client.get_container_client('bronze')
    blob_list = [blob.name for blob in container_client.list_blobs()]

    # Push the blob list to Xcom
    ti.xcom_push(key='blob_list', value=blob_list)

# Define a function to load datasets to Azure Blob Storage
def _load_datasets_to_azure(ti):
    # Retrieve list of blob names from Xcom
    blob_list = ti.xcom_pull(key='blob_list', task_ids='list_files_on_azure')

    # Connect to the Azure blob storage
    blob_service_client = BlobServiceClient.from_connection_string(AZURE_CONNECTION_STRING)

    # Get list of local CSV files downloaded from Kaggle
    datasets = glob('/tmp/kaggle_datasets/*.csv')

    # Upload each dataset to Azure Blob Storage if it's not already there
    for file in datasets:
        if os.path.basename(file) not in blob_list:
            filename = os.path.basename(file)
            container_name = 'silver'
            blob_client = blob_service_client.get_blob_client(container=container_name, blob=filename)
            with open(file, 'rb') as data:
                blob_client.upload_blob(data)
        else:
            print(f'File {file} already exists in Azure Storage.')


# Define DAG
# With DAG structure defined, each function defined above becomes a task in the DAG
with DAG(
    dag_id='kaggle_transform',
    schedule=None,
    start_date=datetime(2023, 1, 1),
    default_args=default_args,
    catchup=False
) as dag:
    # Define tasks in the DAG
    list_files_on_azure = PythonOperator(
        task_id="list_files_on_azure",
        python_callable=_list_files_on_azure
    )
    
    partition_data = SparkSubmitOperator(
        application="/usr/local/spark/app/spark_transform_script.py",
        name="transform_data",
        verbose=1,
        conf={"spark.master": "spark://spark:7077"},
		conn_id= 'spark_local',
		task_id='spark_transform_task',
    )
    
    # load_datasets_to_azure = PythonOperator(
    #     task_id="load_datasets_to_azure",
    #     python_callable=_load_datasets_to_azure
    # )
    
    # Define task dependencies
    list_files_on_azure >> partition_data
