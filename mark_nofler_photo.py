from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime
import requests
import random
import os

SAVE_DIR = os.path.expanduser("~/airflow/knopfler_gallery") # Знак тильды ~ в Linux означает «домашняя папка пользователя».
os.makedirs(SAVE_DIR, exist_ok = True)

def fetch_knofler_photo():
    # 1. Сначала находим ID Марка Нопфлера
    search_url = "https://api.deezer.com/search/artist?q=mark knofler"

    try:
        response = requests.get(search_url)
        response.raise_for_status()
        data = response.json() # Это словарь (Python dictionary)

        if not data.get('data'):
            print("Artisd doesn't found")
            return
        
        artist_id = data['data'][0]['id'] # [0] — это обращение к самому первому элементу в списке.
    

        # 2. Получаем список всех альбомов этого артиста
        albums_url = f"https://api.deezer.com/artist/{artist_id}/albums"
        albums_data = requests.get(albums_url).json()

        albums = albums_data.get('data', []) # []) — это безопасный способ. Если ключа data не будет, скрипт не сломается, а просто вернет пустой список.
        if not albums:
            print("Albums were not found")
            return
        
        # 3. ВЫБИРАЕМ СЛУЧАЙНЫЙ АЛЬБОМ
        random_album = random.choice(albums)
        album_title =random_album['title']
        # Берем самую большую обложку альбома
        photo_url = random_album['cover_xl']

        # 4. Скачиваем с уникальным именем (добавляем ID альбома)
        img_data = requests.get(photo_url).content
        # Заменяем пробелы и спецсимволы, чтобы Linux не ругался
        clean_title = album_title.replace(' ', '_').replace('/', '-')
        file_name = f"Knofler_Album_{clean_title}_{random_album['id']}.jpg"
        file_path = os.path.join(SAVE_DIR, file_name)
        

        with open(file_path, 'wb') as f:
            f.write(img_data)

        print(f"🎸 Нашел случайный альбом: {album_title}")
        print(f"✅ Сохранено как: {file_name}")

    except Exception as e:
        print(f"Mistake: {e}")

if __name__ == "__main__":
    fetch_knofler_photo()


with DAG (
    dag_id = 'mark_knofler_photo',
    start_date = datetime(2026, 2, 10),
    schedule = '@daily',
    catchup = False
) as dag:
    get_photo = PythonOperator(
        task_id = 'get_photo',
        python_callable = fetch_knofler_photo
    )