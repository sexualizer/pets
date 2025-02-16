from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from hashlib import md5
from sqlalchemy import create_engine
import pandas as pd
import logging


default_args = {
  'owner': 'sexualizer',
  'depends_on_past': False,
  'start_date': datetime.now(),
  'schedule_interval': "@hourly",
  'catchup': False
}

# Считывание csv файла в df и сохранение в xcom его путь
def read_csv_file(**kwargs):
    ti = kwargs['ti']
    path_filename = '/opt/airflow/dags/houses.csv'
    df = pd.read_csv(path_filename, encoding = "utf-16", on_bad_lines='skip')
    print(df.info())
    print("Rows: ", len(df))
    df = df.dropna()
    print("Rows: ", len(df))
    ti.xcom_push(key='path_file', value=path_filename)

# Преобразование текстовых и числовых полей в соответсувующие типы данных
def change_types(**kwargs):
    ti = kwargs['ti']
    path_filename = ti.xcom_pull(key='path_file')
    df = pd.read_csv(path_filename, encoding="utf-16", on_bad_lines='skip')
    df['square'] = df['square'].str.replace(' ', '')
    df['square'] = pd.to_numeric(df['square'], errors='coerce')
    df['population'] = pd.to_numeric(df['population'], errors='coerce')
    df['region'] = df['region'].astype(str)
    df['locality_name'] = df['locality_name'].astype(str)
    df['maintenance_year'] = pd.to_numeric(df['maintenance_year'], errors='coerce')
    df['address'] = df['address'].astype(str)
    df['full_address'] = df['full_address'].astype(str)
    df['communal_service_id'] = pd.to_numeric(df['communal_service_id'], errors='coerce')
    df['description'] = df['description'].astype(str)
    df = df.dropna()
    print(df.info())
    new_path = path_filename.replace("houses", "modified_houses")
    df.to_csv(new_path, index=False, encoding='utf-16')
    ti.xcom_push(key='new_path', value=new_path)

# Вычисление среднего и медианного года постройки зданий
def get_mean_and_median(**kwargs):
    ti = kwargs['ti']
    path_filename = ti.xcom_pull(key='path_file')
    df = pd.read_csv(path_filename, encoding = "utf-16", on_bad_lines='skip')
    df['maintenance_year'] = pd.to_numeric(df['maintenance_year'], errors='coerce')
    df = df.dropna()
    df = df[(df['maintenance_year'] >= 1900) & (df['maintenance_year'] <= 2025)]
    average_year = df['maintenance_year'].mean()
    median_year = df['maintenance_year'].median()
    print("Mean value: ", average_year, "Median value: ", median_year)
    ti.xcom_push(key='mean_value', value='average_year')
    ti.xcom_push(key='median_value', value='median_year')



# Топ-10 регионов и городов с наибольшим количеством зданий
def get_tops(**kwargs):
    ti = kwargs['ti']
    path_filename = ti.xcom_pull(key='new_path')
    df = pd.read_csv(path_filename, encoding='utf-16', on_bad_lines='skip')
    top_regions = df['region'].value_counts().head(10)
    top_cities = df['locality_name'].value_counts().head(10)
    print("Top 10 regions: ", top_regions)
    print("Top 10 cities: ", top_cities)

# Количество зданий по десятилетиям
def get_decade(**kwargs):
    ti = kwargs['ti']
    path_filename = ti.xcom_pull(key='new_path')
    df = pd.read_csv(path_filename, encoding='utf-16', on_bad_lines='skip')
    df['decade'] = (pd.to_numeric(df['maintenance_year'], errors='coerce') // 10) * 10
    decade_counts = df['decade'].value_counts().sort_index()
    print(decade_counts)

# Загрузка данных в PostgreSQL
def load_file(**kwargs):
    ti = kwargs['ti']
    path_filename = ti.xcom_pull(key='new_path')
    df = pd.read_csv(path_filename, encoding='utf-16', on_bad_lines='skip')
    print(df.info())
    engine = create_engine('postgresql://user:user@host.docker.internal:5431/user')
    df.to_sql('houses', engine, if_exists='append', schema='public', index=False)
    ti.xcom_push(key='count_string', value=len(df))


dag = DAG(
    dag_id="houses",
    default_args=default_args
)

extraction = PythonOperator(
    task_id="extraction",
    python_callable=read_csv_file,
    dag=dag,
    provide_context=True
)
getting_values = PythonOperator(
    task_id="getting_values",
    python_callable=get_mean_and_median,
    dag=dag,
    provide_context=True
)
changing_types = PythonOperator(
    task_id="changing_types",
    python_callable=change_types,
    dag=dag,
    provide_context=True
)
rating = PythonOperator(
    task_id="rating",
    python_callable=get_tops,
    dag=dag,
    provide_context=True
)
decade_count = PythonOperator(
    task_id="decade_count",
    python_callable=get_decade,
    dag=dag,
    provide_context=True
)
create_table_psql = PostgresOperator(
	task_id='create_table',
    postgres_conn_id='psql_connection',
    sql=""" DROP TABLE IF EXISTS houses;
            CREATE TABLE houses ( 
            house_id serial primary key,
            latitude float,
            longitude float,
            maintenance_year float,
            square float,
            population float,
            region varchar(100),
            locality_name varchar(100),
            address text,
            full_address text,
            communal_service_id float,
            description text
            );
        """
)
loading = PythonOperator(
    task_id="loading",
    python_callable=load_file,
    dag=dag,
    provide_context=True
)
checking = PostgresOperator(
    task_id='checking',
    dag=dag,
    postgres_conn_id='psql_connection',
    sql=""" select house_id, locality_name from houses
            where maintenance_year > 1990
            limit 15;
        """,
    do_xcom_push=True
)

(
    extraction >>
    changing_types >>
    [getting_values, rating, decade_count] >>
    create_table_psql >>
    loading >>
    checking
)