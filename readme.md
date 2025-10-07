# Wildberries Stocks Pipeline

Этот проект реализует DAG Airflow для автоматического сбора данных по остаткам товаров с Wildberries, сохранения их в PostgreSQL и выгрузки в Google Sheets.

---

## Структура проекта
├── airflow/
│ ├── dags/
│ ├── logs/
│ ├── plugins/
│ └── credentials/ # Файл Service Account для Google Sheets
├── init.sql # Скрипт инициализации БД
├── docker-compose.yml
├── Dockerfile
├── requirements.txt
├── .env 
└── README.md

---

## Переменные окружения (.env)

POSTGRES_USER=
POSTGRES_PASSWORD=
POSTGRES_DB=
POSTGRES_HOST=
AIRFLOW_UID=

GOOGLE_SHEET_KEY=<ваш_ключ_гугл_таблицы>
SERVICE_ACCOUNT_FILE=/opt/airflow/credentials/<ваш_json_файл>

---

## Запуск проекта

docker-compose up -d --build

### веб-интерфейс Airflow
http://localhost:8080
Логин: airflow
Пароль: airflow

---

## DAG: wb_stocks_pipeline

Описание: Сбор остатков Wildberries, сохранение в PostgreSQL и выгрузка в Google Sheets.

Расписание: Каждый день в 23:00.

Задачи:

fetch_stocks – собирает данные по артикулу и сохраняет в таблицу stocks.

refresh_materialized_view – обновляет материализованное представление sales_mv.

export_to_google_sheets – выгружает данные в Google Sheets.