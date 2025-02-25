from airflow import DAG
from airflow.operators.python import PythonOperator
from pymongo import MongoClient
from datetime import datetime
import json

# функция для извлечения данных из mongoDB
def fetch_data_from_mongo():
# настройки подключения к MongoDB
    #mongo_uri = "mongodb://airflow_user:password@localhost:27017/admin"
    #database_name = "admin"
    #collection_name = "airflow_collection"

    try:
# создаем клиент для подключения к mongoDB
        client = MongoClient()
        db = client['rh']
        collection = db['факт']

# выполняем запрос (пустой запрос выбирает все документы)
        data = list(collection.find({}))

# выводим данные (можно сохранить их в файл или передать дальше)
        print(f"Fetched {len(data)} documents from MongoDB.")
        for doc in data:
            print(doc)

# сохраняем данные в JSON файл (опционально)
        with open("/tmp/mongo_data.json", "w") as f:
            json.dump(data, f)

    except Exception as e:
        print(f"Error fetching data from MongoDB: {e}")
    finally:
# закрываем соединение
        if client:
            client.close()

# определение dag
with DAG(
    dag_id="mongo_to_airflow",
    start_date=datetime(2023, 1, 1),
    schedule_interval="@daily",  # Запуск каждый день
    catchup=False,
) as dag:

# оператор для выполнения функции
    fetch_mongo_data_task = PythonOperator(
        task_id="fetch_mongo_data",
        python_callable=fetch_data_from_mongo,
    )

# зависимости задач (в данном случае только одна задача)
fetch_mongo_data_task