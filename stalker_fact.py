from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime
import requests

FILE_PATH = "/home/hicks/airflow/stalker_lore.txt"

def fetch_stalker_fact():
    API_URL = "https://stalker.fandom.com/api.php"

    # Шаг 1: Получаем случайную статью
    random_params = {
        "action": "query", # Мы говорим серверу: «Я хочу получить данные из твоей базы».
        "format": "json",
        "list": "random", # заставляет сервер закрыть глаза и выбрать любую статью из тысяч доступных.
        "rnnamespace": 0, # Самый важный фильтр. 0 — это «Основное пространство», где лежат только статьи про игру.
        "rnlimit": 1 # Мы просим прислать только одну случайную страницу за раз.
    }

    random_res = requests.get(API_URL, params=random_params).json() # библиотека requests берет длинную текстовую строку от сервера и превращает её в обычный словарь (dict).
    page = random_res['query']['random'][0] # Внутри query мы ищем ключ random. Сервер возвращает его в виде списка []
    # мы берем первый элемент этого списка
    # В переменную page попадает маленький словарик: {"id": 1422, "ns": 0, "title": "Weasel"}.
    page_id = page['id']
    title = page['title']

    # Шаг 2: Получаем текст (Пробуем вытащить 5 предложений)
    text_params = {
        "action": "query",
        "format": "json",
        "prop": "extracts",
        "explaintext": True,
        "exsentences": 5,
        "pageids": page_id
    }

    text_res = requests.get(API_URL, params=text_params).json()
    extract = text_res['query']['pages'][str(page_id)].get('extract', "").strip()

    # План Б: Если 5 предложений не нашлось, берем просто первые 500 символов
    if not extract:
        fallback_params = {
            "action": "query",
            "format": "json",
            "prop": "extracts",
            "explaintext": True,
            "exchars": 500,
            "pageids": page_id
        }
        fallback_res = requests.get(API_URL, params=fallback_params).json()
        extract = fallback_res['query']['pages'][str(page_id)].get('extract', "").strip()

    # Финальная обрезка для красоты
    summary = (extract[:500] + '...') if len(extract) > 500 else extract

    # Шаг 3: Сохраняем результат
    with open(FILE_PATH, 'w', encoding='utf-8') as f:
        f.write(f"☢️ PDA ENCRYPTED ENTRY: {datetime.now().strftime('%Y-%m-%d')} ☢️\n")
        f.write(f"Subject: {title}\n")
        f.write("=" * 40 + "\n\n")
        f.write(summary if summary else "Data corrupted. Possible 'Monolith' interference.")
        f.write(f"\n\n🔗 Source: https://stalker.fandom.com/wiki/{title.replace(' ', '_')}")
    
    print(f"Entry about '{title}' uploaded to your PDA")

with DAG (
    dag_id='stalker_fact',
    start_date=datetime(2026, 2, 6),
    schedule='@hourly',
    catchup=False
) as dag:
    
    get_fact = PythonOperator(
        task_id='fetch_random_stalker_fact',
        python_callable=fetch_stalker_fact
    )