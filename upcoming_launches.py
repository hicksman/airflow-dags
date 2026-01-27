from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime
import requests
import json
import os

def fetch_upcoming_launches():
    URL = "https://ll.thespacedevs.com/2.2.0/launch/upcoming/?limit=5" # Получаем ближайшие 5 запусков

    headers = {'User_agent': 'Airflow-Rocket-Bot'} # Рекомендуется добавить User-Agent, так как API иногда блокирует пустые запросы

    response = requests.get(URL, headers =headers)

    if response.status_code == 200:
        launches = response.json().get('results', []) # cам список пусков лежит под ключом results
        with open("/home/hicks/airflow/upcoming_launches.txt", "w") as f: # режим "w" полностью перезаписывает файл
            f.write(f"Обновлено: {datetime.now()}\n")
            f.write("-" * 30 + "\n") # это просто способ нарисовать красивую разделительную линию из 30 дефисов.

            for launch in launches:
                name = launch.get("name")
                windows_start = launch.get("windows_start")
                provider = launch.get("launch_service_provider").get('name')

                f.write(f"Mission: {name}\n")
                f.write(f"Provider: {provider}\n")
                f.write(f"Time: {windows_start}\n")
                f.write("-" * 10 + "\n") 
        print("List of launches is updated.")
    else:
        print(f"API Error: {response.status_code}")

with DAG(
     dag_id = 'upcoming_launches',
     start_date = datetime(2026, 1, 20),
     schedule = '@daily', # Указывает запускать задачу раз в сутки (в полночь по времени UTC).
     catchup = False
) as dag:
     
     download_task = PythonOperator(
          task_id = 'fetch_upcoming_launches', 
          python_callable = fetch_upcoming_launches
     )
