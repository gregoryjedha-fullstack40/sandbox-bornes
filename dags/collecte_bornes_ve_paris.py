"""DAG de collecte des données Bornes VE Paris.

Contexte
--------
etl.collect_data() lit dans cet ordre de priorité : (1) cache S3
(raw/data/<source>.csv), (2) API publiques Belib'/IRVE Gireve en direct,
(3) l'ancien serveur Airflow ALH — devenu indisponible, conservé en tout
dernier recours. Ce DAG remplace ce rôle : il tourne à la place de l'ancien
serveur ALH et republie sur S3 des données fraîches, pour que l'appli
Streamlit (conteneur HF Spaces, éphémère) n'ait jamais à dépendre des API
publiques en direct ni du serveur legacy pour se (re)construire.

Deux phases :
1. extract_*  — rappelle les fetchers bas niveau d'etl.py (aucun d'eux ne lit
   le cache S3) et republie chaque source brute sous raw/data/<source>.csv,
   exactement la clé que lecture_s3() va relire.
2. build_*/compute_* — rappelle les fonctions haut niveau d'etl.py
   (recuperer_liste_stations_belib/gireve, fusionner_sources,
   calculer_pression, calculer_projections) en mode "cache non forcé" : comme
   la phase 1 vient de rafraîchir le cache S3, ces fonctions lisent les
   données fraîches sans re-frapper les API publiques une seconde fois.

Ce DAG n'écrit pas dans bornes.db (SQLite) : cette base vit dans le
conteneur Streamlit éphémère et est reconstruite par
database.assurer_donnees_disponibles() au démarrage, à partir du cache S3
que ce DAG maintient à jour. Le voir comme une piste de suivi : si le
cold-start de l'appli doit accélérer, on pourra faire lire à
assurer_donnees_disponibles() directement bornes.csv/pression.csv/
projections_*.csv publiés ici plutôt que tout recalculer en process.

Pré-requis d'environnement (à définir sur les workers Airflow) :
- BORNES_PROJECT_DIR : chemin absolu du checkout de ce repo (pour
  `import etl` et pour que les chemins relatifs "./data/..." utilisés par
  etl.py résolvent correctement).
- S3_BUCKET, AWS_REGION : mêmes variables que l'appli Streamlit.
- Credentials AWS accessibles à boto3 (rôle IAM ou variables standard).
- Dépendances Python : celles de requirements.txt (pandas, requests, boto3,
  ...) installées dans l'environnement Airflow, plus apache-airflow.
"""
from __future__ import annotations

import os
import sys
from datetime import datetime, timedelta

from airflow.decorators import dag, task

PROJECT_DIR = os.environ.get("BORNES_PROJECT_DIR", os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if PROJECT_DIR not in sys.path:
    sys.path.insert(0, PROJECT_DIR)

default_args = {
    "owner": "data-team",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
}


def _setup():
    """Importe etl une fois le sys.path prêt, et se place dans le repo.

    etl.py écrit des fichiers de contrôle sous "./data/..." (chemins
    relatifs) : il faut que le CWD du worker soit le repo pour que ces
    écritures aboutissent au bon endroit plutôt que dans le home Airflow.
    """
    os.chdir(PROJECT_DIR)
    os.makedirs(os.path.join(PROJECT_DIR, "data"), exist_ok=True)
    import etl
    return etl


def _upload_s3(df, source: str) -> int:
    """Republie un DataFrame vers raw/data/<source>.csv, la clé lue par etl.lecture_s3()."""
    import io
    import boto3

    bucket = os.environ.get("S3_BUCKET", "")
    region = os.environ.get("AWS_REGION", "eu-north-1")
    if not bucket:
        raise RuntimeError("S3_BUCKET n'est pas configuré : impossible de publier le cache.")

    buffer = io.StringIO()
    df.to_csv(buffer, index=False)
    boto3.client("s3", region_name=region).put_object(
        Bucket=bucket,
        Key=f"raw/data/{source}.csv",
        Body=buffer.getvalue().encode("utf-8-sig"),
    )
    print(f"Publié raw/data/{source}.csv sur S3 ({len(df)} lignes)")
    return len(df)


@dag(
    dag_id="collecte_bornes_ve_paris",
    description="Rafraîchit le cache S3 (raw/data/*.csv) consommé par l'appli Streamlit "
                "Paris Bornes, en remplacement de l'ancien serveur Airflow ALH.",
    schedule="0 5 * * *",  # tous les jours à 5h (heure du worker) — ajuster si besoin
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["bornes-ve", "paris", "etl"],
)
def collecte_bornes_ve_paris():

    # ---- Phase 1 : rafraîchir le cache brut par source (parallèle) ----

    @task
    def extract_belib_stat() -> None:
        etl = _setup()
        _upload_s3(etl.fetch_belib_stat(), "belib_stat")

    @task
    def extract_belib_rt() -> None:
        etl = _setup()
        _upload_s3(etl.fetch_belib_rt(), "belib_rt")

    @task
    def extract_irve_conso() -> None:
        etl = _setup()
        _upload_s3(etl.fetch_irve_conso(), "irve_conso")

    @task
    def extract_irve_dyn() -> None:
        etl = _setup()
        _upload_s3(etl.fetch_irve_dyn(), "irve_dyn")

    @task
    def extract_energie() -> None:
        etl = _setup()
        annee = datetime.now().year - 1  # dernière année Enedis publiée
        _upload_s3(etl.enedis_paris_data(annee, force=True), "energie")

    @task
    def extract_vehicules() -> list[dict]:
        etl = _setup()
        df = etl.recuperer_vehicules_electriques()
        _upload_s3(df, "vehicules")
        return df.to_dict(orient="records")

    # ---- Phase 2 : reconstruire les jeux dérivés à partir du cache frais ----

    @task
    def build_stations() -> list[dict]:
        """Recombine Belib' + Gireve en lisant le cache S3 que la phase 1 vient d'écrire
        (force=False : on évite de re-frapper les API publiques une 2e fois)."""
        etl = _setup()
        stations_belib = etl.recuperer_liste_stations_belib(maj_airflow=False)
        stations_gireve = etl.recuperer_liste_stations_gireve(force=False)
        stations = etl.fusionner_sources(stations_belib, stations_gireve)
        _upload_s3(stations, "bornes")
        return stations.to_dict(orient="records")

    @task
    def compute_pression(stations_records: list[dict], vehicules_records: list[dict]) -> list[dict]:
        import pandas as pd
        etl = _setup()
        pression = etl.calculer_pression(pd.DataFrame(stations_records), pd.DataFrame(vehicules_records))
        _upload_s3(pression, "pression")
        return pression.to_dict(orient="records")

    @task
    def compute_projections(pression_records: list[dict]) -> None:
        import pandas as pd
        etl = _setup()
        df_arrdt, df_paris = etl.calculer_projections(pd.DataFrame(pression_records))
        if not df_arrdt.empty:
            _upload_s3(df_arrdt, "projections_arrdt")
        if not df_paris.empty:
            _upload_s3(df_paris, "projections_paris")

    # ---- Dépendances ----

    raw_stations_sources = [extract_belib_stat(), extract_belib_rt(), extract_irve_conso(), extract_irve_dyn()]
    vehicules = extract_vehicules()
    extract_energie()

    stations = build_stations()
    raw_stations_sources >> stations

    pression = compute_pression(stations, vehicules)
    compute_projections(pression)


collecte_bornes_ve_paris()
