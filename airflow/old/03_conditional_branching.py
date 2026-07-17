"""
DAG 3 — Branching conditionnel
==============================
Une fonction Python décide, à l'exécution, quelle branche suivre.
Les branches non choisies passent en état "skipped".

Concepts : BranchPythonOperator, trigger_rule pour rejoindre des
branches partiellement skippées.
"""

import random
from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import BranchPythonOperator

default_args = {
    "owner": "airflow",
    "start_date": datetime(2026, 1, 1),
}


def _choose_branch():
    """Retourne le task_id de la branche à exécuter."""
    random_value = random.choice([1, 2, 3])
    if random_value == 1:
        return "first_branch"
    elif random_value == 2:
        return "second_branch"
    return "third_branch"


with DAG(
    dag_id="03_conditional_branching",
    default_args=default_args,
    schedule=None,
    catchup=False,
    tags=["demo", "intro"],
) as dag:
    start_dag = BashOperator(task_id="start_dag", bash_command="echo 'Start!'")

    choose_branch = BranchPythonOperator(
        task_id="choose_branch",
        python_callable=_choose_branch,
    )

    first_branch = BashOperator(
        task_id="first_branch", bash_command="echo 'First branch!'; sleep 1"
    )

    second_branch = BashOperator(
        task_id="second_branch", bash_command="echo 'Second branch!'; sleep 2"
    )

    third_branch = BashOperator(
        task_id="third_branch", bash_command="echo 'Third branch!'; sleep 3"
    )

    # none_failed_min_one_success : la jointure s'exécute même si
    # certaines branches amont sont "skipped" (comportement normal
    # après un BranchPythonOperator).
    join_all = BashOperator(
        task_id="join_all",
        bash_command="echo 'Join all!'",
        trigger_rule="none_failed_min_one_success",
    )

    start_dag >> choose_branch
    choose_branch >> [first_branch, second_branch, third_branch] >> join_all
