import logging
from airflow import DAG
from datetime import datetime, timedelta
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from pymongo import MongoClient
import pandas as pd


def fetch_data_from_postgres(**kwargs):
    hook = PostgresHook(postgres_conn_id='psql_id')
    conn = hook.get_conn()
    
    customers_data = pd.read_sql(con=conn, sql='select * from customers_data')
    receivables_data = pd.read_sql(con=conn, sql='select * from receivables_data')

    print(customers_data.head())
    print(receivables_data.head())

    kwargs['ti'].xcom_push(key='receivables_data', value=receivables_data.to_json())
    kwargs['ti'].xcom_push(key='customers_data', value=customers_data.to_json())


def fetch_data_from_mongodb(**kwargs):
    client = MongoClient()
    db = client['mydb']

    payables_data = pd.DataFrame(list(db['payables_data'].find()))
    suppliers_data = pd.DataFrame(list(db['suppliers_data'].find()))

    print(payables_data.head())
    print(suppliers_data.head())

    kwargs['ti'].xcom_push(key='payables_data', value=payables_data.to_json())
    kwargs['ti'].xcom_push(key='suppliers_data', value=suppliers_data.to_json())


def preprocess_data_from_postgres(**kwargs):
    receivables_data = pd.read_json(kwargs['ti'].xcom_pull(key='receivables_data', task_ids='fetch_data_from_postgres'))
    customers_data = pd.read_json(kwargs['ti'].xcom_pull(key='customers_data', task_ids='fetch_data_from_postgres'))

    df1 = pd.merge(receivables_data, customers_data, on='Customer Name')
    df1['Address'] = df1['Address'].apply(lambda x: x.replace('\n', ' '))
    df1['Payment_Date'] = pd.to_datetime(df1['Payment_Date'], errors='coerce')
    df1['Baseline_Date'] = pd.to_datetime(df1['Baseline_Date'], errors='coerce')

    df1 = df1.dropna()
    df1 = df1.drop_duplicates()

    kwargs['ti'].xcom_push(key='df1', value=df1.to_json())


def preprocess_data_from_mongodb(**kwargs):
    payables_data = pd.read_json(kwargs['ti'].xcom_pull(key='payables_data', task_ids='fetch_data_from_mongodb'))
    suppliers_data = pd.read_json(kwargs['ti'].xcom_pull(key='suppliers_data', task_ids='fetch_data_from_mongodb'))

    print(payables_data.head())
    print(suppliers_data.head())

    df2 = pd.merge(payables_data, suppliers_data, on='Supplier ID')
    df2['Posting Date'] = pd.to_datetime(df2['Posting Date'], errors='coerce')
    df2['Invoice Date'] = pd.to_datetime(df2['Invoice Date'], errors='coerce')
    df2['Payment Date'] = pd.to_datetime(df2['Payment Date'], errors='coerce')
    df2['Net Due Date (System Calculated Date)'] = pd.to_datetime(df2['Net Due Date (System Calculated Date)'], errors='coerce')

    df2 = df2.drop(['Posting Date', 'Invoice Date'], axis=1)
    df2 = df2.dropna()
    df2 = df2.drop_duplicates()

    kwargs['ti'].xcom_push(key='df2', value=df2.to_json())


def export_to_mongodb(**kwargs):
    client = MongoClient()
    db = client['final']
    
    # Export postgres data
    df1 = pd.read_json(kwargs['ti'].xcom_pull(key='df1', task_ids='preprocess_data_from_postgres'))
    collection1 = db['df1']
    collection1.insert_many(df1.to_dict(orient='records'))
    
    # Export mongodb data
    df2 = pd.read_json(kwargs['ti'].xcom_pull(key='df2', task_ids='preprocess_data_from_mongodb'))
    collection2 = db['df2']
    collection2.insert_many(df2.to_dict(orient='records'))


my_dag = DAG(
    dag_id="my_dag_name",
    start_date=datetime(2023, 1, 1),
    schedule="@daily",
    default_args={
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    }
)

task_fetch_data_from_postgres = PythonOperator( 
    task_id='fetch_data_from_postgres',
    python_callable=fetch_data_from_postgres,
    dag=my_dag
)

task_fetch_data_from_mongodb = PythonOperator( 
    task_id='fetch_data_from_mongodb',
    python_callable=fetch_data_from_mongodb,
    dag=my_dag
)

task_preprocess_data_from_postgres = PythonOperator( 
    task_id='preprocess_data_from_postgres',
    python_callable=preprocess_data_from_postgres,
    dag=my_dag
)

task_preprocess_data_from_mongodb = PythonOperator( 
    task_id='preprocess_data_from_mongodb',
    python_callable=preprocess_data_from_mongodb,
    dag=my_dag
)

task_export_to_mongodb = PythonOperator( 
    task_id='export_to_mongodb',
    python_callable=export_to_mongodb,
    dag=my_dag
)

# Set task dependencies
[task_fetch_data_from_postgres, task_fetch_data_from_mongodb] >> task_preprocess_data_from_postgres
[task_fetch_data_from_postgres, task_fetch_data_from_mongodb] >> task_preprocess_data_from_mongodb
[task_preprocess_data_from_postgres, task_preprocess_data_from_mongodb] >> task_export_to_mongodb