from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from datetime import datetime


# функция для выполнения запроса
def execute_sql_query():
    try:
# создаем хук для подключения к postgreSQL
        pg_hook = PostgresHook(postgres_conn_id="psql_id")
    except Exception as e:
        print(e)
# выполняем запрос
    sql_query = "SELECT * FROM план;"
    result = pg_hook.get_records(sql=sql_query)
    
# выводим результаты
    print("Результаты запроса:")
    for row in result:
        print(row)

# определение dag
with DAG(
    dag_id="execute_sql_with_hook",
    start_date=datetime(2023, 10, 1),
    schedule_interval="@daily",  # запускать ежедневно
    catchup=False,
) as dag:

# таск для выполнения запроса
    execute_sql_task = PythonOperator(
        task_id="execute_sql",
        python_callable=execute_sql_query,
    )