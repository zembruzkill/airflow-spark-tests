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


# Define a function that fetches Azure credentials from a JSON file
def get_azure_credentials(filename):
    # Opening and reading the JSON file
    with open(filename, 'r') as f:
        data = json.load(f)
        
    connection_string = data['connection_string']

    return connection_string

# Call the function with the specified JSON file path to get the Azure connection string
AZURE_CONNECTION_STRING = get_azure_credentials('/opt/airflow/secure_credentials/azure_credentials.json')

# Define a function to get parameters for DAG upon its execution
def _get_dag_parameters(ti, **kwargs):
    parameters = kwargs['dag_run'].conf

    # Check if mandatory parameters are provided else raise exception
    if 'search_term' not in parameters:
        raise AirflowException('No search_term provided.')
    elif 'sort_by' not in parameters:
        raise AirflowException('No sort_by provided.')
    elif 'num_datasets' not in parameters:
        raise AirflowException('No num_datasets provided.')
    else:
        # If all parameters are present, push them to next task using Xcom
        ti.xcom_push(key='parameters', value=parameters)

# Define a function to list out the datasets on Kaggle using provided parameters
def _list_datasets_on_kaggle(ti):
    # Pull parameters from Xcom
    parameters = ti.xcom_pull(key='parameters', task_ids='get_dag_parameters')

    # Break parameters into individual variables
    search_term = parameters['search_term']
    sort_by = parameters['sort_by']
    num_datasets = parameters['num_datasets']

    # Calculate Number of pages to be scraped from Kaggle
    num_pages = math.ceil(num_datasets / 20)

    movies_dataset_list = []
    # For each page pull list of datasets and append to master list
    for page in range(1, num_pages + 1):
        dataset_list = kaggle.api.dataset_list(
            search=search_term,
            sort_by=sort_by,
            page=page
        )
        movies_dataset_list += dataset_list

    # Trim the master list to required number of datasets and push to Xcom
    data = [str(item) for item in movies_dataset_list[0:num_datasets]]
    ti.xcom_push(key='datasets_list', value=data)

# Define a function to download datasets from Kaggle
def _download_datasets(ti):
    # Retrieve the list of datasets from Xcom
    datasets_list = ti.xcom_pull(key='datasets_list', task_ids='list_datasets_on_kaggle')

    # For each dataset, try to download it and handle exceptions
    for dataset in datasets_list:
        try:
            kaggle.api.dataset_download_files(dataset, path='/tmp/kaggle_datasets', unzip=True)
        except:
            raise AirflowException(f'Error downloading dataset {dataset}')

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
            container_name = 'bronze'
            blob_client = blob_service_client.get_blob_client(container=container_name, blob=filename)
            with open(file, 'rb') as data:
                blob_client.upload_blob(data)
        else:
            print(f'File {file} already exists in Azure Storage.')

# Define a function to clear local datasets
def _clear_data():
    # Get list of local CSV files
    datasets = glob('/tmp/kaggle_datasets/*.csv')

    # Delete each local file
    for file in datasets:
        os.remove(file)

# Define DAG
# With DAG structure defined, each function defined above becomes a task in the DAG
with DAG(
    dag_id='kaggle_related_datasets',
    schedule=None,
    start_date=datetime(2023, 1, 1),
    catchup=False
) as dag:
    # Define tasks in the DAG
    get_dag_parameters = PythonOperator(
        task_id="get_dag_parameters",
        python_callable=_get_dag_parameters
    )
    
    list_datasets_on_kaggle = PythonOperator(
        task_id='list_datasets_on_kaggle',
        python_callable=_list_datasets_on_kaggle
    )
    
    download_datasets = PythonOperator(
        task_id='download_datasets',
        python_callable=_download_datasets
    )
    
    list_files_on_azure = PythonOperator(
        task_id="list_files_on_azure",
        python_callable=_list_files_on_azure
    )
    
    load_datasets_to_azure = PythonOperator(
        task_id="load_datasets_to_azure",
        python_callable=_load_datasets_to_azure
    )
    
    clear_data = PythonOperator(
        task_id="clear_data",
        python_callable=_clear_data
    )
    
    # Define task dependencies
    get_dag_parameters >> list_datasets_on_kaggle >> download_datasets >> list_files_on_azure >> load_datasets_to_azure >> clear_data
