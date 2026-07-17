"""
DAG 1 — Tâches parallèles
=========================
Premier contact avec la syntaxe des dépendances :
une tâche de départ, trois branches en parallèle, puis une jointure.

Concepts : BashOperator, dépendances avec >>, parallélisme.
"""

from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "airflow",
    "start_date": datetime(2026, 1, 1),
}

with DAG(
    dag_id="01_parallel_tasks",
    default_args=default_args,
    schedule=None,  # déclenchement manuel uniquement
    catchup=False,
    tags=["demo", "intro"],
) as dag:
    start_dag = BashOperator(task_id="start_dag", bash_command="echo 'Start!'")

    first_branch = BashOperator(
        task_id="first_branch", bash_command="echo 'First branch!'; sleep 1"
    )

    second_branch = BashOperator(
        task_id="second_branch", bash_command="echo 'Second branch!'; sleep 2"
    )

    third_branch = BashOperator(
        task_id="third_branch", bash_command="echo 'Third branch!'; sleep 3"
    )

    join_all = BashOperator(task_id="join_all", bash_command="echo 'Join all!'")

    start_dag >> [first_branch, second_branch, third_branch] >> join_all
