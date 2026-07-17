"""
DAG 2 — TaskGroups
==================
Organiser des tâches en groupes logiques : deux branches parallèles,
chacune contenant une séquence de trois tâches.

Concepts : TaskGroup, lisibilité du graphe, parallélisme de groupes.
"""

from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup

default_args = {
    "owner": "airflow",
    "start_date": datetime(2026, 1, 1),
}

with DAG(
    dag_id="02_taskgroups",
    default_args=default_args,
    schedule=None,
    catchup=False,
    tags=["demo", "intro"],
) as dag:
    start_dag = BashOperator(task_id="start_dag", bash_command="echo 'Start!'")

    with TaskGroup(group_id="first_group") as first_group:
        first_task = BashOperator(
            task_id="first_task", bash_command="echo 'First task!'; sleep 2"
        )
        second_task = BashOperator(
            task_id="second_task", bash_command="echo 'Second task!'; sleep 2"
        )
        third_task = BashOperator(
            task_id="third_task", bash_command="echo 'Third task!'; sleep 2"
        )

        first_task >> second_task >> third_task

    with TaskGroup(group_id="second_group") as second_group:
        first_task = BashOperator(
            task_id="first_task", bash_command="echo 'First task!'; sleep 2"
        )
        second_task = BashOperator(
            task_id="second_task", bash_command="echo 'Second task!'; sleep 2"
        )
        third_task = BashOperator(
            task_id="third_task", bash_command="echo 'Third task!'; sleep 2"
        )

        first_task >> second_task >> third_task

    end_dag = BashOperator(task_id="end_dag", bash_command="echo 'End!'")

    start_dag >> [first_group, second_group] >> end_dag
