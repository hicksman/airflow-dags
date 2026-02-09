from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime
import json
import requests
import random # Добавили для выбора случайного объекта

FILE_PATH = "/home/hicks/airflow/solar_system_atlas.txt"

def get_random_celestial_body():
    URL = "https://api.le-systeme-solaire.net/rest/bodies/"
    headers = {'Authorization': 'Bearer 976bcb61-b3b7-4ba8-9be5-7095d52f9bcd'}
    response = requests.get(URL, headers=headers) 

    if response.status_code == 200:
        # 1. Запрашиваем список всех объектов (их около 280)
        data = json.loads(response.text, strict=False)
        all_bodies = data.get('bodies', [])

        # 2. Выбираем один случайный объект из списка
        random_body = random.choice(all_bodies)

        # 3. Извлекаем интересные данные
        name = random_body.get('englishName', 'Unknown')
        is_planet = "Да" if random_body.get('isPlanet') else "Нет"
        gravity = random_body.get('gravity', 'Нет данных')
        mean_radius = random_body.get('meanRadius', 'Нет данных')
        discovered_by = random_body.get('discoveredBy', 'Древние астрономы')

        # 4. Записываем в файл (режим "a" — дозапись)
        with open(FILE_PATH, "a", encoding="utf-8") as f:
            f.write(f"--- Запись от {datetime.now().strftime('%Y-%m-%d')} ---\n")
            f.write(f"Объект: {name}\n")
            f.write(f"Это планета? {is_planet}\n")
            f.write(f"Гравитация: {gravity} м/с²\n")
            f.write(f"Средний радиус: {mean_radius} km\n")
            f.write(f"Кем открыто: {discovered_by}\n")
            f.write(f"Объект: {name}\n")
            f.write("-" * 30 + "\n\n")

        print(f"Информация об объекте {name} добавлена в атлас.")
    else:
        print(f"Ошибка API: {response.status_code}")


with DAG (
    dag_id = 'get_celestial_body_info',
    start_date = datetime(2026, 1, 25),
    schedule = '@daily',
    catchup = False
) as dag:
    task = PythonOperator(
        task_id = 'collect_celestial_data',
        python_callable = get_random_celestial_body
    )



