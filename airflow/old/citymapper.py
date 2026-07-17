import pandas as pd
import boto3
from airflow.models import Variable
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from collections.abc import Sequence
from airflow.models import BaseOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup

class S3ToPostgresOperator(BaseOperator):
    template_fields: Sequence[str] = ("bucket", "key", "table", "postgres_conn_id", "aws_conn_id")

S3_BUCKET = "citymapper-jedha"
AWS_DEFAULT_REGION = Variable.get("AWS_DEFAULT_REGION")
AWS_ACCESS_KEY_ID = Variable.get("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = Variable.get("AWS_SECRET_ACCESS_KEY")

def fetch_weather_data(ti):
    df = pd.read_csv("https://api.weatherbit.io/v2.0/current?city=Paris&country=France&key=4c85a673040248d18c0ae8313ff1834d")
    print(f"Data extracted: {len(df)} rows")
    today = datetime.today()
    formatted = today.strftime("%Y-%m-%d")
    url = "./data/etalab_data-"+formatted
    df.to_json(url+".json", orient="records")
    ti.xcom_push(key="weather_dataset", value=url)

def transform_weather_data(ti):
    dataurl = ti.xcom_pull(task_ids="fetch_weather_data", key="weather_dataset")
    df = pd.read_json(dataurl)
    csv = df.to_csv(dataurl, index=False)
    s3 = boto3.client("s3", region_name=AWS_DEFAULT_REGION, aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
    s3.upload_file(filename=dataurl, bucket=S3_BUCKET, key="weather_data.json")
    return df

def create_weather_table():
    # Code to create the weather table in Postgres
    pass

def transfer_weather_data_to_postgres():
    # Code to transfer the weather data to Postgres
    pass

def fetch_status_data(ti):
    pass

def transform_status_data(ti):
    pass

def create_status_table():
    pass

def transfer_status_data_to_postgres():
    pass

default_args = {
    "owner": "airflow",
    "schedule_interval": "@daily",
    "start_date": datetime(2026, 7, 1),
}

with DAG(
    dag_id="citymapper",
    default_args=default_args,
    catchup=False,
    description="Get data for citymapper and transform it",
    tags=["demo", "etl"],
    
) as dag:
    start = EmptyOperator(task_id="start")

    with TaskGroup(group_id="status_branch") as branche_a:
        fetch_status_data = PythonOperator(task_id="fetch_status_data", python_callable=fetch_status_data)
        transform_status_data = PythonOperator(task_id="transform_status_data", python_callable=transform_status_data)
        #create_status_table = PostgresOperator(task_id="create_status_table", sql="CREATE TABLE IF NOT EXISTS status_data;")
        #transfer_status_data_to_postgres = S3ToPostgresOperator(task_id="transfer_status_data_to_postgres", s3_bucket="citymapper-jedha", s3_key="status_data.json", table="status_data", postgres_conn_id="my_postgres_conn")

        fetch_status_data >> transform_status_data #>> create_status_table >> transfer_status_data_to_postgres

    with TaskGroup(group_id="weather_branch") as branche_b:
        fetch_weather_data = PythonOperator(task_id="fetch_weather_data", python_callable=fetch_weather_data)
        transform_weather_data = PythonOperator(task_id="transform_weather_data", python_callable=transform_weather_data)
        create_weather_table = PostgresOperator(task_id="create_weather_table", sql="""
    CREATE TABLE IF NOT EXISTS weather (
        id SERIAL PRIMARY KEY,

        app_temp REAL,
        aqi INTEGER,
        city_name VARCHAR(100),
        clouds INTEGER,
        country_code CHAR(2),
        datetime VARCHAR(20),
        dewpt REAL,
        dhi REAL,
        dni REAL,
        elev_angle REAL,
        ghi REAL,
        gust REAL,
        h_angle REAL,
        lat DOUBLE PRECISION,
        lon DOUBLE PRECISION,
        ob_time TIMESTAMP,
        pod CHAR(1),
        precip REAL,
        pres REAL,
        rh INTEGER,
        slp REAL,
        snow REAL,
        solar_rad REAL,
        state_code VARCHAR(10),
        station VARCHAR(20),
        sunrise TIME,
        sunset TIME,
        temp REAL,
        timezone VARCHAR(100),
        ts BIGINT,
        uv REAL,
        vis REAL,

        weather_code INTEGER,
        weather_description VARCHAR(100),
        weather_icon VARCHAR(20),

        sources JSONB
    );
    """)
        transfer_weather_data_to_postgres = S3ToPostgresOperator(bucket="citymapper-jedha", key="weather_data.json", table="weather_data", postgres_conn_id="postgres_default", aws_conn_id="aws_default")

        fetch_weather_data >> transform_weather_data >> create_weather_table >> transfer_weather_data_to_postgres

    end = EmptyOperator(task_id="end")

start >> [branche_a, branche_b] >> end