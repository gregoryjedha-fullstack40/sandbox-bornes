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
   serveur MLflow "jedhaflow40". Le modèle est enregistré dans le Model
   Registry sous le nom `DBSCAN_Paris` et tagué "challenger" ; s'il obtient un
   meilleur silhouette score (hors points de bruit) que la version taguée
   "production", il devient la nouvelle "production". pages/recommandations.py
   charge ensuite `models:/DBSCAN_Paris@production` pour ses prédictions.

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

# Nom sous lequel le modèle DBSCAN est enregistré dans le Model Registry MLflow.
# pages/recommandations.py charge la version taguée "production" via
# models:/{REGISTERED_MODEL_NAME}@production.
REGISTERED_MODEL_NAME = "DBSCAN_Paris"

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

    import mlflow

    for attempt in range(1, retries + 1):
        try:
            return fn(*args, **kwargs)
        except mlflow.exceptions.MlflowException:
            if attempt == retries:
                raise
            time.sleep(min(delay * 2 ** (attempt - 1), max_delay))


def _silhouette_or_none(coords_rad, labels):
    """Silhouette score (haversine) sur les points non-bruit, ou None si non calculable.

    Sert de métrique de qualité pour comparer un modèle "challenger" au modèle
    "production" en place : nécessite au moins 2 clusters distincts et 3 points
    non-bruit, sinon silhouette_score lève une ValueError.
    """
    from sklearn.metrics import silhouette_score

    mask = labels != -1
    if mask.sum() < 3 or len(set(labels[mask])) < 2:
        return None
    try:
        return float(silhouette_score(coords_rad[mask], labels[mask], metric="haversine"))
    except ValueError:
        return None


@dag(
    dag_id="collecter_bornes",
    description="Recollecte les données Bornes VE Paris (bornes_arrondissements.uploadS3) puis "
                "entraîne et logue un modèle DBSCAN sur MLflow (jedhaflow40).",
    schedule="0 5,14 * * *",  # tous les jours à 5h et 14h (heure du worker) — ajuster si besoin
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
        from mlflow.tracking import MlflowClient
        from sklearn.cluster import DBSCAN
        from sklearn.metrics import silhouette_score

        MODEL_NAME = "dbscan_bornes"
        client = MlflowClient()

        def evaluate(labels, X):
            mask = labels != -1
            n_clusters = len(set(labels[mask]))
            if n_clusters < 2 or mask.sum() <= n_clusters:
                return float("nan")
            return silhouette_score(X[mask], labels[mask], metric="haversine")

        def alias_metric(alias):
            try:
                mv = client.get_model_version_by_alias(MODEL_NAME, alias)
            except Exception:
                return None                      # alias absent (ton cas "production not found")
            return client.get_run(mv.run_id).data.metrics.get("silhouette")

        run = mlflow.start_run()
        try:
            model = DBSCAN(eps=..., min_samples=...).fit(X)
            score = evaluate(model.labels_, X)

            # validation MÉTIER : c'est ICI qu'on décide ce qui compte comme "réussi"
            if np.isnan(score):
                raise ValueError("Clustering dégénéré (< 2 clusters exploitables)")

            mlflow.log_metric("silhouette", score)
            mlflow.sklearn.log_model(model, "model")
            mv = mlflow.register_model(f"runs:/{run.info.run_id}/model", MODEL_NAME)

            # 1) le nouveau est toujours challenger
            client.set_registered_model_alias(MODEL_NAME, "challenger", mv.version)

            # 2) promu production seulement s'il bat l'actuel (ou s'il n'y a pas de prod)
            prod = alias_metric("production")
            if prod is None or score > prod:
                client.set_registered_model_alias(MODEL_NAME, "production", mv.version)

            mlflow.end_run(status="FINISHED")
        except Exception as e:
            mlflow.set_tag("failure_reason", str(e))
            mlflow.end_run(status="FAILED")
            raise

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
        silhouette = _silhouette_or_none(coords_rad, labels)

        mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
        mlflow.set_experiment(f"Bornes_DBSCAN_{datetime.now():%Y%m%d}")

        if mlflow.active_run() is not None:
            mlflow.end_run()

        client = MlflowClient()

        with mlflow.start_run(run_name=f"DBSCAN_Paris_Airflow_{datetime.now():%Y%m%d_%H%M}"):
            _log_with_retry(mlflow.log_params, {
                "eps_m": DBSCAN_EPS_METERS,
                "eps_rad": eps_rad,
                "min_samples": DBSCAN_MIN_SAMPLES,
                "metric": "haversine",
                "nb_bornes": len(df),
            })
            metrics = {"clusters": nb_clusters, "noise_points": nb_bruit}
            if silhouette is not None:
                metrics["silhouette"] = silhouette
            _log_with_retry(mlflow.log_metrics, metrics)

            model_info = _log_with_retry(
                mlflow.sklearn.log_model,
                sk_model=model,
                artifact_path="DBSCAN_Paris",
                registered_model_name=REGISTERED_MODEL_NAME,
            )

        # ─── Comparaison challenger vs production ───
        # Le modèle qu'on vient d'entraîner devient le "challenger" du registry.
        # S'il fait mieux que la version taguée "production" (silhouette plus
        # élevée sur les points non-bruit), il devient la nouvelle "production".
        # S'il n'existe pas encore de "production" (premier run), on promeut
        # directement le challenger.
        new_version = str(model_info.registered_model_version)
        _log_with_retry(
            client.set_registered_model_alias, REGISTERED_MODEL_NAME, "challenger", new_version
        )

        try:
            # Peu de retries ici : une absence d'alias "production" (premier run)
            # est une réponse "not found" normale, pas une erreur transitoire à
            # ré-essayer en boucle.
            prod_version = _log_with_retry(
                client.get_model_version_by_alias,
                REGISTERED_MODEL_NAME, "production",
                retries=2, delay=2, max_delay=4,
            )
            prod_run = client.get_run(prod_version.run_id)
            prod_silhouette = prod_run.data.metrics.get("silhouette")
        except mlflow.exceptions.MlflowException:
            prod_version = None
            prod_silhouette = None

        promote = prod_version is None or (
            silhouette is not None and (prod_silhouette is None or silhouette > prod_silhouette)
        )

        if promote:
            _log_with_retry(
                client.set_registered_model_alias, REGISTERED_MODEL_NAME, "production", new_version
            )
            _log_with_retry(client.delete_registered_model_alias, REGISTERED_MODEL_NAME, "challenger")

    reimport_data() >> train_dbscan()


collecte_bornes_ve_paris()
