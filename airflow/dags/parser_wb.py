import os
import requests
import datetime

import psycopg2
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.log.logging_mixin import LoggingMixin
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd
from dotenv import load_dotenv


log = LoggingMixin().log

load_dotenv()

GOOGLE_SHEET_KEY = os.getenv("GOOGLE_SHEET_KEY")
SERVICE_ACCOUNT_FILE = os.getenv("SERVICE_ACCOUNT_FILE")

nmIds = [293378862, 295739458, 263038976, 287293767, 267672429, 
         265116995, 268759087, 284439349, 274048350, 293376054]

DB_SETTINGS = dict(
    host=os.getenv("POSTGRES_HOST"),
    dbname=os.getenv("POSTGRES_DB"),
    user=os.getenv("POSTGRES_USER"),
    password=os.getenv("POSTGRES_PASSWORD")
)


def fetch_and_store_stocks():
    today = datetime.datetime.now()
    
    with psycopg2.connect(**DB_SETTINGS) as conn:
        with conn.cursor() as cursor:
            for nmId in nmIds:
                try:
                    url = f"https://card.wb.ru/cards/v2/detail?appType=1&curr=rub&dest=-1257786&spp=99&nm={nmId}"
                    response = requests.get(url, timeout=10)
                    response.raise_for_status()

                    data = response.json()
                    sizes = data['data']['products'][0]['sizes']
                    total_stock = sum(size['stocks'][0]['qty'] for size in sizes if size.get('stocks'))

                    cursor.execute(
                        "INSERT INTO stocks (date, nmId, stocks) VALUES (%s, %s, %s)",
                        (today, nmId, total_stock)
                    )
                    log.info(f"Артикул {nmId}: {total_stock} шт.")

                except Exception as e:
                    log.error(f"Ошибка при обработке {nmId}: {e}")

    log.info("Данные по остаткам успешно сохранены.")


def refresh_view():
    with psycopg2.connect(**DB_SETTINGS) as conn:
        with conn.cursor() as cursor:
            cursor.execute("REFRESH MATERIALIZED VIEW sales_mv;")
            log.info("Материализованное представление обновлено.")


def export_to_google_sheets():
    with psycopg2.connect(**DB_SETTINGS) as conn:
        query = "SELECT * FROM sales_mv WHERE date >= current_date - interval '30 days';"
        df = pd.read_sql(query, conn, parse_dates=['date'])

    df['date'] = df['date'].astype(str)

    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name(SERVICE_ACCOUNT_FILE, scope)
    client = gspread.authorize(creds)

    sheet = client.open_by_key(GOOGLE_SHEET_KEY).sheet1
    sheet.clear()

    sheet.update([df.columns.values.tolist()] + df.values.tolist())
    log.info(f"В Google Sheets выгружено {len(df)} строк.")


with DAG(
    dag_id="wb_stocks_pipeline",
    start_date=datetime.datetime(2025, 10, 2),
    schedule_interval="0 23 * * *",
    catchup=False,
    tags=["wildberries", "stocks", "google_sheets"]
) as dag:

    extract_data = PythonOperator(
        task_id="fetch_stocks",
        python_callable=fetch_and_store_stocks
        )
    
    refresh_materialized_view = PythonOperator(
        task_id="refresh_materialized_view",
        python_callable=refresh_view
        )
    
    export_to_sheets = PythonOperator(
        task_id="export_to_google_sheets",
        python_callable=export_to_google_sheets
        )

    extract_data >> refresh_materialized_view >> export_to_sheets


