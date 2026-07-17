"""
DAG 6 — Démo MLflow : prédiction batch
======================================
Exemple de DAG "MLOps" : charger un modèle depuis un Model Registry
MLflow et l'utiliser pour faire des prédictions.

⚠️ Ce DAG nécessite une configuration préalable dans l'UI Airflow
(voir README, sections Connexions & Variables) :

- Connexion `aws_default`  (credentials AWS, pour l'artifact store S3)
- Variable  `MLFLOW_TRACKING_URI` (URL de votre serveur MLflow)
- Un modèle enregistré dans le Model Registry avec l'alias `production`

Concepts : Connexions, Variables, hooks, intégration MLflow.
"""

import logging
from datetime import datetime

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator

DATA_URL = (
    "https://full-stack-assets.s3.eu-west-3.amazonaws.com/Deployment/ibm_hr_attrition.xlsx"
)
# Modèle enregistré dans le Model Registry, pointé par son alias
MODEL_URI = "models:/ibm_attrition_detector@production"


def run_prediction():
    # Bonne pratique : les imports lourds (mlflow, pandas, boto3) sont
    # faits DANS la fonction, pas en haut du fichier. Le scheduler
    # parse tous les DAGs en continu : des imports lourds au niveau
    # module ralentissent tout Airflow.
    import boto3
    import mlflow
    import pandas as pd
    from airflow.providers.amazon.aws.hooks.s3 import S3Hook

    # === 1️⃣ Config MLflow Tracking URI (depuis les Variables Airflow) ===
    mlflow_tracking_uri = Variable.get("MLFLOW_TRACKING_URI")
    mlflow.set_tracking_uri(mlflow_tracking_uri)
    logging.info("Using MLflow Tracking URI: %s", mlflow_tracking_uri)

    # === 2️⃣ Config AWS (depuis la connexion Airflow `aws_default`) ===
    s3_hook = S3Hook(aws_conn_id="aws_default")
    creds = s3_hook.get_credentials()
    conn = s3_hook.get_connection("aws_default")
    region = (conn.extra_dejson or {}).get("region_name", "eu-west-3")
    boto3.setup_default_session(
        aws_access_key_id=creds.access_key,
        aws_secret_access_key=creds.secret_key,
        region_name=region,
    )
    logging.info("AWS credentials chargés depuis la connexion Airflow")

    # === 3️⃣ Charger le modèle depuis le Model Registry ===
    model = mlflow.sklearn.load_model(MODEL_URI)
    logging.info("Modèle récupéré : %s", MODEL_URI)

    # === 4️⃣ Charger la donnée ===
    df = pd.read_excel(DATA_URL, index_col=0)
    logging.info("Données récupérées : %s lignes", len(df))

    # === 5️⃣ Faire une prédiction ===
    preds = model.predict(df.head(5))
    logging.info("Sample predictions : %s", preds)


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2026, 1, 1),
    "retries": 0,
}

with DAG(
    dag_id="06_mlflow_predict",
    default_args=default_args,
    schedule=None,  # déclenchement manuel uniquement
    catchup=False,
    tags=["demo", "mlflow", "aws"],
) as dag:
    predict_task = PythonOperator(
        task_id="predict_with_mlflow_model",
        python_callable=run_prediction,
    )
