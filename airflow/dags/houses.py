from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from hashlib import md5
from sqlalchemy import create_engine
import pandas as pd


default_args = {
  'owner': 'sexualizer',
  'depends_on_past': False,
  'start_date': datetime.now(),
  'schedule_interval': "@hourly",
  'catchup': False
}

def read_csv_file(**kwargs):
    ti = kwargs['ti']
    path_filename = '/opt/airflow/dags/houses.csv'
    df = pd.read_csv(path_filename, encoding = "utf-16", on_bad_lines='skip')
    print(df.info())
    print("Rows: ", len(df))
    df = df.dropna()
    print("Rows: ", len(df))
    ti.xcom_push(key='path_file', value=path_filename)

def get_mean_and_median(**kwargs):
    ti = kwargs['ti']
    path_filename = ti.xcom_pull(key='path_file')
    df = pd.read_csv(path_filename)
    df['maintenance_year'] = pd.to_numeric(df['maintenance_year'], errors='coerce')
    df = df.dropna()
    df = df[(df['maintenance_year'] >= 1900) & (df['maintenance_year'] <= 2025)]
    average_year = df['maintenance_year'].mean()
    median_year = df['maintenance_year'].median()
    print("Mean value: ", average_year, "Median value: ", median_year)
    ti.xcom_push(key='mean_value', value='average_year')
    ti.xcom_push(key='median_value', value='median_year')

    # df['square'] = pd.to_numeric(df['square'], errors='coerce')
    # df['population'] = pd.to_numeric(df['population'], errors='coerce')
    # df['region'] = df['region'].astype(str)
    # df['locality_name'] = df['locality_name'].astype(str)
    # df['address'] = df['address'].astype(str)
    # df['full_address'] = df['full_address'].astype(str)
    # df['communal_service_id'] = pd.to_numeric(df['communal_service_id'], errors='coerce')
    # df['description'] = df['description'].astype(str)
    # df = df.dropna()
    # print(df.info())
    #
    # top_regions = df['region'].value_counts().head(10)
    # top_cities = df['locality_name'].value_counts().head(10)
    # print("Top 10 regions: ", top_regions)
    # print("Top 10 cities: ", top_cities)
    #
    # df['decade'] = (df['maintenance_year'] // 10) * 10
    # decade_counts = df['decade'].value_counts().sort_index()
    # print(decade_counts)


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

(
    extraction >>
    getting_values
)