from io import StringIO
import pandas as pd
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator

def extract_data(ti):
    df = pd.read_csv("https://www.data.gouv.fr/fr/datasets/r/5c4e1452-3850-4b59-b11c-3dd51d7fb8b5")
    print(f"Data extracted: {len(df)} rows")
    today = datetime.today()
    formatted = today.strftime("%Y-%m-%d")
    csv_url = "./data/data-"+formatted+".csv"
    df = df.drop(columns=["dep","reg","lib_dep","lib_reg","tx_pos","tx_incid","TO","R","hosp","rea","rad","dchosp","reg_rea","incid_rea","incid_rad","incid_dchosp","reg_incid_rea","pos","pos_7j","cv_dose1"])
    csv = df.to_csv(csv_url, index=False)
    ti.xcom_push(key="dataset", value=csv_url)

def transform_data(ti):
    dataurl = ti.xcom_pull(task_ids="extract_data", key="dataset")
    df = pd.read_csv(dataurl)
    df = df.groupby("date")["incid_hosp"].agg(["mean", "sum"]).reset_index()
    csv = df.to_csv(dataurl, index=False)

default_args = {
    "owner": "airflow",
    "schedule_interval": "@daily",
    "start_date": datetime(2020, 3, 18),
}

with DAG(
    dag_id="airflow_dag",
    default_args=default_args,
    schedule=None,
    catchup=False,
    description="Get data to be transformed",
    tags=["demo", "etl"],
    
) as dag:
    
    start = EmptyOperator(task_id="start")
    extract = PythonOperator(
        task_id="extract_data",
        python_callable=extract_data,
    )
    transform = PythonOperator(
        task_id="transform_data",
        python_callable=transform_data,
    )
    end = EmptyOperator(task_id="end")

start >> extract >> transform >> end