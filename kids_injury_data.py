from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
import os
import requests
import pandas as pd
from datetime import datetime, timedelta

# Папка для хранения медицинских отчетов
SAVE_DIR = os.path.expanduser("~/aiflow.health_reports")


def fetch_who_injury_data():
    os.makedirs(SAVE_DIR, exist_ok=True)
    api_url = "https://ghoapi.azureedge.net/api/WHOSIS_000003"

    try:
        print(f"Requesting data from: {api_url}")
        response = requests.get(api_url, timeout=40)
        response.raise_for_status()
        
        raw_data = response.json().get('value', [])
        df = pd.DataFrame(raw_data)

        if df.empty:
            print("No data found!")
            return

        # ДИАГНОСТИКА: Печатаем колонки, чтобы точно знать, что пришло
        print(f"Available columns: {df.columns.tolist()}")

        # В разных индикаторах ВОЗ разные названия. 
        # Давай выберем те, что точно должны быть (Код страны, Год, Значение)
        # Чаще всего это: 'SpatialDimensionValueCode', 'TimeDimensionValueCode', 'NumericValue'
        
        # Автоматический поиск нужных колонок (на случай опечаток в API)
        col_country = 'SpatialDimensionValueCode' if 'SpatialDimensionValueCode' in df.columns else 'Id'
        col_year = 'TimeDimensionValueCode' if 'TimeDimensionValueCode' in df.columns else 'TimeDim'
        col_val = 'NumericValue'

        # Создаем результирующий DF
        result = df[[col_country, col_year, col_val]].copy()
        
        result.columns = ['country', 'year', 'mortality_rate']

        # Фильтруем только Кыргызстан для теста, если хочешь
        # kgz_only = result[result['country'] == 'KGZ']

        file_path = os.path.join(SAVE_DIR, "who_child_mortality_basic.csv")
        result.to_csv(file_path, index=False)
        
        print(f"Success! Saved to {file_path}")
        print(result.head())

    except Exception as e:
        print(f"Error during processing: {e}")
        # Печатаем колонки при ошибке для отладки
        if 'df' in locals():
            print(f"Columns in DF were: {df.columns.tolist()}")
        raise e
with DAG(
    dag_id='kids_injury_data',
    start_date=datetime(2026, 2, 17),
    schedule="@weekly",
    catchup=False,
) as dag:
    task_get_data = PythonOperator(
        task_id= 'fetch_data',
        python_callable = fetch_who_injury_data
    )



