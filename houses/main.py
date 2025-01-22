from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import pandas as pd



def read_csv_file(**context):
    df = pd.read_csv('/opt/airflow/dags/houses.csv', encoding = "utf-16", on_bad_lines='skip')
    print(df.info())
    print("Rows: ", len(df))
    df = df.dropna()
    print("Rows: ", len(df))

    df['maintenance_year'] = pd.to_numeric(df['maintenance_year'], errors='coerce')
    df = df.dropna()
    df = df[(df['maintenance_year'] >= 1900) & (df['maintenance_year'] <= 2025)]
    average_year = df['maintenance_year'].mean()
    median_year = df['maintenance_year'].median()
    print("Mean value: ", average_year, "Median value: ", median_year)

    df['square'] = pd.to_numeric(df['square'], errors='coerce')
    df['population'] = pd.to_numeric(df['population'], errors='coerce')
    df['region'] = df['region'].astype(str)
    df['locality_name'] = df['locality_name'].astype(str)
    df['address'] = df['address'].astype(str)
    df['full_address'] = df['full_address'].astype(str)
    df['communal_service_id'] = pd.to_numeric(df['communal_service_id'], errors='coerce')
    df['description'] = df['description'].astype(str)
    df = df.dropna()
    print(df.info())

    top_regions = df['region'].value_counts().head(10)
    top_cities = df['locality_name'].value_counts().head(10)
    print("Top 10 regions: ", top_regions)
    print("Top 10 cities: ", top_cities)

    df['decade'] = (df['maintenance_year'] // 10) * 10
    decade_counts = df['decade'].value_counts().sort_index()
    print(decade_counts)

    """postgres_sql_upload = PostgresHook(postgres_conn_id='postgres_localhost')
    postgres_sql_upload.insert_rows('buildings', df)
    """

    hook = PostgresHook(postgres_conn_id='postgres_localhost')

    request = "insert into buildings (house_id,latitude,longitude,maintenance_year,square,population,region,locality_name,address,full_address,communal_service_id,description) values (1,44.707617,43.006476,1974,2 661.10,89,Ставропольский край,село Александровское,с. Александровское, ул. Войтика, д. 31","Ставропольский край, р-н. Александровский, с. Александровское, ул. Войтика, д. 31",1.0,"Жилой дом в , по адресу с. Александровское, ул. Войтика, д. 31, 1974 года постройки, под управлением УК «Жилсервис».)"
    hook.run(request)

with DAG(
        dag_id="main",
        start_date=datetime(2023, 10, 1),
        schedule_interval="@daily",
        catchup=False,
) as dag:
    extraction = PythonOperator(
        task_id="extraction",
        python_callable=read_csv_file,
        provide_context=True
    )

    extraction