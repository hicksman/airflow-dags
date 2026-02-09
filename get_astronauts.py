from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime
import requests 

FILE_PATH = "/home/hicks/airflow/space_travelers.txt"

def fetch_astronauts():
    URL = "http://api.open-notify.org/astros.json"

    try:
        response = requests.get(URL, timeout=10)
        response.raise_for_status() # Выдаст ошибку, если статус не 200
        data = response.json()

        number = data.get('number')
        people = data.get('people', [])
        
        with open(FILE_PATH, "a", encoding = "utf-8") as f:
            f.write(f"--- Данные на {datetime.now().strftime('%d.%m.%Y %H:%M')} ---\n")
            f.write(f"Сейчас в космосе: {number} человек\n")

            for person in people:
                f.write(f" - {person['name']} ({person['craft']})\n")

            f.write("-" * 30 + "\n\n")
        print(f"Успешно записано {number} человек.")
    except Exception as e:
        print(f"Произошла ошибка при получении данных: {e}")

with DAG (
    dag_id='get_astronauts',
    start_date=datetime(2026, 1, 30),
    schedule='@daily',
    catchup=False,
    tags=['space', 'human_presence']
) as dag :
    task = PythonOperator(
        task_id = 'get_current_astronauts',
        python_callable=fetch_astronauts)
    
                    