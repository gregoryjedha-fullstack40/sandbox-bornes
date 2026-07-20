"""DAG de collecte des données Bornes VE Paris + entraînement DBSCAN.

Deux tâches :
1. reimport_data — appelle bornes_arrondissements.uploadS3(), qui recollecte
   toutes les sources (Belib', Gireve/IRVE, Enedis, population, VE) en forçant
   la collecte "live" (pas de lecture du cache S3), calcule la pression et les
   projections, persiste tout dans bornes.db (SQLite) via database.sauvegarder_*,
   puis uploade les CSV rafraîchis sur S3.
2. train_dbscan — relit la table `bornes` fraîchement écrite, entraîne un
   DBSCAN sur les coordonnées des points de charge (mêmes paramètres que
   pages/recommandations.py) et logue paramètres, métriques et modèle sur le
   serveur MLflow "jedhaflow40".

Pré-requis d'environnement (à définir sur les workers Airflow) :
- BORNES_PROJECT_DIR : chemin absolu du checkout de ce repo (pour
  `import etl`/`import database` et pour que les chemins relatifs
  "./data/..." utilisés par etl.py résolvent correctement).
- S3_BUCKET, AWS_REGION : mêmes variables que l'appli Streamlit.
- Credentials AWS accessibles à boto3 (rôle IAM ou variables standard).
- MLFLOW_TRACKING_URI : URL du serveur MLflow jedhaflow40 (défaut :
  https://gregoryjedha-jedhaflow40.hf.space, celle utilisée par l'appli).
- Dépendances Python : celles de requirements.txt (pandas, requests, boto3,
  scikit-learn, mlflow, ...) installées dans l'environnement Airflow, plus
  apache-airflow.
"""
from __future__ import annotations

import os
import sys
import time
from datetime import datetime, timedelta

from airflow.decorators import dag, task

PROJECT_DIR = os.environ.get("BORNES_PROJECT_DIR", os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if PROJECT_DIR not in sys.path:
    sys.path.insert(0, PROJECT_DIR)

MLFLOW_TRACKING_URI = os.environ.get("MLFLOW_TRACKING_URI", "https://gregoryjedha-jedhaflow40.hf.space")

# Mêmes valeurs par défaut que le curseur DBSCAN de pages/recommandations.py
DBSCAN_EPS_METERS = 300
DBSCAN_MIN_SAMPLES = 2

default_args = {
    "owner": "data-team",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
}


def _setup_bornes():
    """Importe etl une fois le sys.path prêt, et se place dans le repo.

    etl.py et database.py écrivent des fichiers sous "./data/..." et
    "bornes.db" (chemins relatifs) : il faut que le CWD du worker soit le
    repo pour que ces écritures aboutissent au bon endroit plutôt que dans
    le home Airflow.
    """
    os.chdir(PROJECT_DIR)
    os.makedirs(os.path.join(PROJECT_DIR, "data"), exist_ok=True)
    import bornes_arrondissements
    return bornes_arrondissements


def _log_with_retry(fn, *args, retries=6, delay=3, max_delay=20, **kwargs):
    """Retry an MLflow call on transient server/artifact-store errors.

    Un Space HF endormi peut mettre 30 à 60s à répondre correctement après
    son réveil (500 en attendant) : backoff exponentiel plafonné pour laisser
    ce temps de réveil plutôt qu'abandonner après quelques secondes. Même
    logique que pages/recommandations.py.
    """
    import mlflow

    for attempt in range(1, retries + 1):
        try:
            return fn(*args, **kwargs)
        except mlflow.exceptions.MlflowException:
            if attempt == retries:
                raise
            time.sleep(min(delay * 2 ** (attempt - 1), max_delay))


@dag(
    dag_id="collecte_bornes_ve_paris",
    description="Recollecte les données Bornes VE Paris (bornes_arrondissements.uploadS3) puis "
                "entraîne et logue un modèle DBSCAN sur MLflow (jedhaflow40).",
    schedule="0 5,14 * * *",  # tous les jours à 5h (heure du worker) — ajuster si besoin
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["bornes-ve", "paris", "etl", "mlflow"],
)
def collecte_bornes_ve_paris():

    @task
    def reimport_data() -> None:
        extraction = _setup_bornes()
        extraction.uploadS3()

    @task
    def train_dbscan() -> None:
        import numpy as np
        import mlflow
        import mlflow.sklearn
        from sklearn.cluster import DBSCAN

        _setup_bornes()
        import database

        df = database.charger_bornes()
        df = df.dropna(subset=["latitude", "longitude"])

        coords_rad = np.radians(df[["latitude", "longitude"]].values)
        eps_rad = DBSCAN_EPS_METERS / 6_371_000
        model = DBSCAN(eps=eps_rad, min_samples=DBSCAN_MIN_SAMPLES, metric="haversine")
        labels = model.fit_predict(coords_rad)

        nb_clusters = len(set(labels)) - (1 if -1 in labels else 0)
        nb_bruit = int((labels == -1).sum())

        mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
        mlflow.set_experiment(f"Bornes_DBSCAN_{datetime.now():%Y%m%d}")

        if mlflow.active_run() is not None:
            mlflow.end_run()

        with mlflow.start_run(run_name=f"DBSCAN_Paris_Airflow_{datetime.now():%Y%m%d_%H%M}"):
            _log_with_retry(mlflow.log_params, {
                "eps_m": DBSCAN_EPS_METERS,
                "eps_rad": eps_rad,
                "min_samples": DBSCAN_MIN_SAMPLES,
                "metric": "haversine",
                "nb_bornes": len(df),
            })
            _log_with_retry(mlflow.log_metrics, {
                "clusters": nb_clusters,
                "noise_points": nb_bruit,
            })
            _log_with_retry(mlflow.sklearn.log_model, sk_model=model, artifact_path="DBSCAN_Paris")

    reimport_data() >> train_dbscan()


collecte_bornes_ve_paris()
