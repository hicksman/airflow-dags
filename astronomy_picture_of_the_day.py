from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime
import requests
import json
import os

IMAGE_DIR = "/home/hicks/airflow/nasa_images" # Путь, где будут лежать наши космические фото

def fetch_and_save_nasa_photo():
    # 1. Проверяем, существует ли папка, если нет — создаем
    if not os.path.exists(IMAGE_DIR):
        os.makedirs(IMAGE_DIR)
        print(f"Создана папка: {IMAGE_DIR}")

    # 2. Получаем данные от API
    URL = "https://api.nasa.gov/planetary/apod?api_key=DEMO_KEY" # URL для получения фото дня

    response = requests.get(URL) # Мы стучимся на сервер NASA
    if response.status_code == 200: # Проверяем код ответа (200 — всё Ок)
         data = response.json()
         # Извлекаем из JSON 
         image_url = data.get('url')
         media_type = data.get('media_type') # NASA иногда постит видео!

         # Проверяем, что это именно картинка
         if media_type == 'image':
             title = data.get('title').replace(" ", "_").replace('/', "-") # Убираем лишние символы из имени
             date_str = datetime.now().strftime("%Y-%m-%d")
             file_name = f"{date_str}_{title}.jpg"
             file_path = os.path.join(IMAGE_DIR, file_name)

             # 3. Скачиваем саму картинку
             img_response = requests.get(image_url)
             if img_response.status_code == 200: 
                  with open(file_path, 'wb') as f: # 'wb' — запись байтов (картинки это не текст!)
                       f.write(img_response.content)
                  print(f"Успешно скачано: {file_path}")
             else:
                  print("Не удалось скачать само изображение")
         else:
            print(f"Сегодня не фото, а {media_type}. Пропускаем скачивание.")
            
                
with DAG(
     dag_id = 'astronomy_picture_of_the_day',
     start_date = datetime(2026, 1, 20),
     schedule = '@daily', # Указывает запускать задачу раз в сутки (в полночь по времени UTC).
     catchup = False
) as dag:
     
     download_task = PythonOperator(
          task_id = 'fetch_and_save_nasa_photo', 
          python_callable = fetch_and_save_nasa_photo
     )

