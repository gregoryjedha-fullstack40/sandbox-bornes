"""
DAG 4 — Structure d'un ETL
==========================
Squelette d'un pipeline ETL réaliste, sans aucune logique métier :
deux sources extraites/transformées/chargées en parallèle, puis une
étape de fusion.

Objectif pédagogique : dessiner l'architecture d'un pipeline AVANT
d'écrire le code. Les EmptyOperator servent de placeholders.

Concepts : EmptyOperator, TaskGroup, design de pipeline.
"""

from datetime import datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup

default_args = {
    "owner": "airflow",
    "start_date": datetime(2026, 1, 1),
}

with DAG(
    dag_id="04_etl_structure",
    default_args=default_args,
    schedule=None,
    catchup=False,
    description="Squelette d'un ETL à deux sources parallèles",
    tags=["demo", "etl"],
) as dag:
    start = EmptyOperator(task_id="start")

    with TaskGroup(group_id="source_a") as source_a:
        extract = EmptyOperator(task_id="extract")
        transform = EmptyOperator(task_id="transform")
        load = EmptyOperator(task_id="load")

        extract >> transform >> load

    with TaskGroup(group_id="source_b") as source_b:
        extract = EmptyOperator(task_id="extract")
        transform = EmptyOperator(task_id="transform")
        load = EmptyOperator(task_id="load")

        extract >> transform >> load

    with TaskGroup(group_id="fusion") as fusion:
        merge = EmptyOperator(task_id="merge")
        publish = EmptyOperator(task_id="publish")

        merge >> publish

    end = EmptyOperator(task_id="end")

    start >> [source_a, source_b] >> fusion >> end
