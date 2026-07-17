"""
DAG 5 — ETL COVID (data.gouv.fr)
================================
Un vrai pipeline ETL, de bout en bout :

- Extract   : téléchargement du CSV COVID-19 publié par data.gouv.fr
- Transform : calcul de la moyenne des nouvelles hospitalisations par jour
- Load      : export du résultat en CSV dans le dossier ./data (monté
              en volume dans le docker-compose)

Concepts : PythonOperator, XCom (échange de données entre tâches),
schedule quotidien, volume de données partagé.
"""

import logging
import os
from datetime import datetime

import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator

ENDPOINT = "https://www.data.gouv.fr/fr/datasets/r/5c4e1452-3850-4b59-b11c-3dd51d7fb8b5"
# Chemin absolu DANS le conteneur : ./data est monté sur /opt/airflow/data
DATA_FOLDER = "/opt/airflow/data"

default_args = {
    "owner": "airflow",
    "start_date": datetime(2026, 1, 1),
}


def _fetch_covid_data(ti):
    """Extract : télécharge les données COVID et les sauvegarde en CSV brut."""
    logging.info("Fetching COVID data")
    df = pd.read_csv(ENDPOINT)
    target_filename = os.path.join(
        DATA_FOLDER, f"raw-data-{datetime.now().strftime('%Y-%m-%d')}.csv"
    )
    df.to_csv(target_filename, index=False)
    # On pousse le nom du fichier dans XCom pour la tâche suivante
    ti.xcom_push(key="target_filename", value=target_filename)
    logging.info("Saved COVID data to %s", target_filename)


def _transform_covid_data(ti):
    """Transform + Load : calcule la moyenne des hospitalisations et exporte."""
    logging.info("Transforming COVID data")
    # On récupère le nom du fichier depuis XCom
    filename = ti.xcom_pull(task_ids="fetch_covid_data", key="target_filename")
    logging.info("target_filename: %s", filename)
    df = pd.read_csv(filename)
    # Moyenne des nouvelles hospitalisations par jour (colonne "incid_hosp")
    mean = df.groupby("date")["incid_hosp"].mean()
    mean_target_filename = os.path.join(
        DATA_FOLDER, f"mean-incid_hosp-{datetime.now().strftime('%Y-%m-%d')}.csv"
    )
    mean.to_csv(mean_target_filename)
    ti.xcom_push(key="mean_target_filename", value=mean_target_filename)
    logging.info(
        "Computed mean of new hospitalized cases per day and saved to %s",
        mean_target_filename,
    )


with DAG(
    dag_id="05_covid_etl",
    default_args=default_args,
    schedule="@daily",  # les données source sont mises à jour chaque jour
    catchup=False,
    tags=["demo", "etl"],
) as dag:
    fetch_covid_data = PythonOperator(
        task_id="fetch_covid_data", python_callable=_fetch_covid_data
    )

    transform_covid_data = PythonOperator(
        task_id="transform_covid_data", python_callable=_transform_covid_data
    )

    fetch_covid_data >> transform_covid_data
