# API Key a3d941ac1ccf48fc984f380f674b15c5
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime
import requests

API_KEY = 'a3d941ac1ccf48fc984f380f674b15c5'
FILE_PATH = "/home/hicks/airflow/top_games.txt"

def fetch_top_games():
    URL = "https://api.rawg.io/api/games"
    
    params = {
        'key': API_KEY,
        'page_size': 10,
        'ordering': '-metacritic', # Сначала игры с самым высоким баллом критиков
        'metacritic': '80,100'     # Только хиты (80+)
    }
    
    response = requests.get(URL, params=params)
    
    if response.status_code == 200:
        data = response.json()
        games = data.get('results', [])
        
        with open(FILE_PATH, "w", encoding="utf-8") as f:
            f.write(f"🚀 ГЕЙМИНГ-ОТЧЕТ: ЛУЧШЕЕ НА {datetime.now().strftime('%d.%m.%Y %H:%M')} 🚀\n")
            f.write("=" * 50 + "\n\n")
            
            for game in games:
                name = game.get('name').upper()
                meta = game.get('metacritic', 'N/A')
                released = game.get('released', 'N/A')
                # Собираем жанры в одну строку
                genres = [g['name'] for g in game.get('genres', [])]
                genre_str = ", ".join(genres) if genres else "N/A"
                
                f.write(f"🎮 ИГРА: {name}\n")
                f.write(f"🏆 Metacritic: {meta} / 100\n")
                f.write(f"🎭 Жанры: {genre_str}\n")
                f.write(f"📅 Релиз: {released}\n")
                f.write("-" * 50 + "\n")
        
        print(f"Готово! В файл {FILE_PATH} улетели 10 хитов.")
    else:
        print(f"Ошибка API: {response.status_code}")
with DAG(
    dag_id = 'top10_games',
    start_date = datetime(2026, 1, 1),
    schedule = '@daily',
    catchup = False
) as dag:
    get_games = PythonOperator(
        task_id = 'fetch_top_rated_games',
        python_callable = fetch_top_games)
    
