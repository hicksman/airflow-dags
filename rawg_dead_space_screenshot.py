from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime
import random
import os
import requests

API_KEY = 'a3d941ac1ccf48fc984f380f674b15c5'
GAME_SLUG = 'dead-space' # Уникальное имя игры в базе
SAVE_DIR = os.path.expanduser("~/airflow/deas_space_gallery")

# Создаем папку, если её нет
os.makedirs(SAVE_DIR, exist_ok= True)

def download_random_screenshot():
    # 1. Запрашиваем список скриншотов
    URL = f'https://api.rawg.io/api/games/{GAME_SLUG}/screenshots?key={API_KEY}'

    # Блок try ... except (Твоя страховка)
    try:
        response = requests.get(URL)
        response.raise_for_status() # Проверка на ошибки (404, 500 и т.д.)
        data = response.json()

        if not data.get('results'): # В ответе от RAWG все картинки лежат в списке под ключом results
            print(" There is no screenshots")
            return
        
        # 2. Выбираем случайный скриншот из списка
        shot = random.choice(data['results'])
        image_url = shot['image'] # Мы забираем именно прямую ссылку на картинку.

        # Генерируем имя файла (берем ID скриншота из ссылки)
        file_name = f"ds_shot_{random.randint(1000, 9000)}.jpg"
        file_path = os.path.join(SAVE_DIR, file_name) # Это «умный» склейщик путей.

        # 3. Скачиваем саму картинку
        print(f"Downloading {image_url}...")
        img_data = requests.get(image_url).content # .content — это бинарные данные (нули и единицы, из которых состоит изображение).

        with open(file_path, 'wb') as f: # Создай на жестком диске пустой файл по адресу file_path
            f.write(img_data) # Возьми данные из оперативной памяти (из переменной img_data) и переложи их в этот файл».

        print(f"Done! Saved in: {file_path}")

    except Exception as e: # перехватывает любую ошибку и записывает её текст в переменную e
        print(f"Mistake: {e}")

if __name__ == "__main__":
    download_random_screenshot()

with DAG (
    dag_id = 'rawg_dead_space_screenshot',
    start_date = datetime(2026, 2, 9),
    schedule = '@daily',
    catchup = False
) as dag :
    get_screenshot = PythonOperator(
        task_id = 'get_dead_space_screenshot',
        python_callable = download_random_screenshot
    )





