from airflow import DAG
from airflow.operators.python import PythonOperator

from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import DateRange, Dimension, Metric, RunReportRequest

import os
import json
import pandas as pd
from datetime import datetime

from sqlalchemy import create_engine

creds_file = os.path.abspath('/opt/airflow/secure_credentials/client_secrets.json')
property_id = '338418657'

def get_postgres_credentials(filename):
    with open(filename, 'r') as f:
        data = json.load(f)
        
    host = data['host']
    user = data['username']
    password = data['password']
    dbname = data['dbname']

    return host, user, password, dbname

HOST, USERNAME, PASSWORD, DBNAME = get_postgres_credentials('/opt/airflow/secure_credentials/neon_credentials.json')

def _get_report(ti):
    
    client = BetaAnalyticsDataClient.from_service_account_file(creds_file)
    
    request = RunReportRequest(
        property=f'properties/{property_id}',
        dimensions=[Dimension(name='eventName')],
        metrics=[Metric(name='eventCount')],
        date_ranges=[DateRange(start_date='2020-01-01', end_date='today')],
    )

    # Send the request and get the response
    response = client.run_report(request)
    
    df = pd.DataFrame(columns=['event_name', 'total_events', 'sync_date'])

    for row in response.rows:
        sync_date = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        new_row = [row.dimension_values[0].value,
                row.metric_values[0].value,
                sync_date]
        df.loc[len(df)] = new_row
        
    df.to_csv('/tmp/analytics.csv', index=None, header=False)
    
def _send_report():
    print(HOST, USERNAME, PASSWORD, DBNAME)
    
    CONNSTR = f"postgresql://{USERNAME}:{PASSWORD}@{HOST}/{DBNAME}"
    engine = create_engine(CONNSTR)
    
    df = pd.read_csv('/tmp/analytics.csv', names=['event_name', 'total_events', 'sync_date'])
    
    print(df)
    df.to_sql('google_analytics_events', con=engine, if_exists='append', index=False)
    

with DAG(
    dag_id='analytics_dag',
    schedule='@daily',
    start_date=datetime(2023, 1, 1),
    catchup=False
):
    
    get_report = PythonOperator(
        task_id='get_report',
        python_callable=_get_report
    )
    
    send_report = PythonOperator(
        task_id='send_report',
        python_callable=_send_report
    )
    
    get_report >> send_report