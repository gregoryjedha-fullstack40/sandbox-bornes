"""
recommandations.py — Zones recommandées pour de nouvelles bornes VE à Paris
Analyse DBSCAN : identification des zones sous-couvertes croisées avec la pression VE.
"""

import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import json
import os
import sys
import time
import mlflow
import mlflow.sklearn
from datetime import datetime

# Ajouter le dossier parent au path pour importer database
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
import database

database.assurer_donnees_disponibles()

# ─── Chargement des données ───

@st.cache_data(ttl=300)
def charger_bornes():
    engine = database.get_engine()
    df = pd.read_sql("SELECT * FROM bornes", engine)
    df["num_arrondissement"] = pd.to_numeric(df["num_arrondissement"], errors="coerce")
    df = df.dropna(subset=["num_arrondissement", "latitude", "longitude"])
    df["num_arrondissement"] = df["num_arrondissement"].astype(int)
    return df


@st.cache_data(ttl=300)
def charger_pression():
    engine = database.get_engine()
    df = pd.read_sql("SELECT * FROM pression", engine)
    df["num_arrondissement"] = df["num_arrondissement"].astype(int)
    return df


@st.cache_data(ttl=3600)
def charger_geojson():
    geojson_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "arrondissements.geojson")
    if os.path.exists(geojson_path):
        with open(geojson_path, "r", encoding="utf-8") as f:
            return json.load(f)
    return None


bornes = charger_bornes()
pression = charger_pression()
geojson = charger_geojson()

if bornes.empty or pression.empty or geojson is None:
    st.error("Données manquantes. Lancez d'abord l'ETL pour alimenter la base.")
    st.stop()

distance_max = 300
resolution = 0.003
nb_resultats = 100

with st.sidebar:
    st.markdown("## ⚡ Pondération")
    poids_pression = st.slider(
        "Pression VE vs couverture géographique",
        min_value=0.0,
        max_value=1.0,
        value=0.9,
        step=0.05,
        help="0 = priorité à la couverture géographique (distance à la borne la plus proche). "
             "1 = priorité à la pression VE (demande dans l'arrondissement).",
    )

    st.markdown("## 🧩 Clustering")
    min_bornes_cluster = st.slider(
        "Nombre minimal de bornes par cluster",
        min_value=2,
        max_value=5,
        value=2,
        step=1,
        help="Seuil `min_samples` du DBSCAN : nombre de bornes proches requis "
             "pour former un cluster (zone considérée comme couverte).",
    )

from shapely.geometry import Point, shape

@st.cache_data(ttl=3600)
def generer_grille_paris(_geojson, resolution):
    """Génère une grille de points uniquement à l'intérieur de Paris."""
    polygones = []
    for feature in _geojson["features"]:
        poly = shape(feature["geometry"])
        arr_num = feature["properties"]["c_ar"]
        polygones.append((arr_num, poly))
    
    lat_min, lat_max = 48.815, 48.905
    lon_min, lon_max = 2.22, 2.47
    
    lats = np.arange(lat_min, lat_max, resolution)
    lons = np.arange(lon_min, lon_max, resolution)
    
    points = []
    for lat in lats:
        for lon in lons:
            p = Point(lon, lat)
            for arr_num, poly in polygones:
                if poly.contains(p):
                    points.append({
                        "latitude": lat,
                        "longitude": lon,
                        "num_arrondissement": arr_num,
                    })
                    break
    
    return pd.DataFrame(points)


with st.spinner("Génération de la grille intra-muros..."):
    grille = generer_grille_paris(geojson, resolution)


# ─── Étape 2 : DBSCAN sur les bornes existantes ───
# On identifie les clusters de bornes (zones bien couvertes)
# et les points de bruit (bornes isolées).

from sklearn.cluster import DBSCAN
from sklearn.neighbors import BallTree

coords = bornes[["latitude", "longitude"]].values
coords_rad = np.radians(coords)

# eps en radians : distance_max en mètres / rayon Terre
eps_rad = distance_max / 6_371_000
db = DBSCAN(eps=eps_rad, min_samples=min_bornes_cluster, metric="haversine")

bornes_clusters = db.fit_predict(coords_rad)
mlflow.log_params({
        "eps_m": distance_max,
        "eps_rad": eps_rad,
        "min_samples": min_bornes_cluster,
        "metric": "haversine",
        "resolution": resolution,
        "poids_pression": poids_pression
})

nb_clusters = len(set(bornes_clusters)) - (1 if -1 in bornes_clusters else 0)
nb_bruit = (bornes_clusters == -1).sum()


# ─── Étape 3 : Distance de chaque point candidat à la borne la plus proche ───

tree = BallTree(coords_rad, metric="haversine")
distances, _ = tree.query(np.radians(grille[["latitude", "longitude"]].values), k=1)

# Conversion radians → mètres (rayon Terre = 6 371 000 m)
grille["distance_borne_m"] = (distances.flatten() * 6_371_000).round(0)

#Suppression des résultats aberrants ou des points périphériques
grille = grille[grille["distance_borne_m"]<500]

# ─── Étape 4 : Score de priorité pondéré ───
# On croise la couverture spatiale (distance) avec la demande (pression VE/borne).
# Pondération ajustable via le curseur de la barre latérale.

grille = grille.merge(
    pression[["num_arrondissement", "pression", "nb_ve", "nb_pdc"]],
    on="num_arrondissement",
    how="left",
)

# Filtrer : garder uniquement les zones à plus de distance_max d'une borne
candidats = grille[grille["distance_borne_m"] > distance_max].copy()

if candidats.empty:
    st.success(f"Toutes les zones de Paris sont couvertes à moins de {distance_max}m d'une borne.")
    st.stop()

# Normaliser les deux scores entre 0 et 1
candidats["score_couverture"] = (
    candidats["distance_borne_m"] / candidats["distance_borne_m"].max()
).round(3)

candidats["score_pression"] = (
    candidats["pression"] / candidats["pression"].max()
).round(3)

# Score final pondéré
candidats["score_priorite"] = (
    (1 - poids_pression) * candidats["score_couverture"]
    + poids_pression * candidats["score_pression"]
).round(3)

candidats = candidats.sort_values("score_priorite", ascending=False)

# Label arrondissement
candidats["arr_label"] = candidats["num_arrondissement"].apply(
    lambda x: f"{int(x)}{'er' if x == 1 else 'e'} arr."
)

# ─── Affichage ───

# KPIs
k1, k2, k3, k4 = st.columns(4)
with k1:
    st.metric("Clusters de bornes", nb_clusters)
with k2:
    st.metric("Bornes isolées", nb_bruit)
with k3:
    st.metric("Zones candidates", len(candidats))
with k4:
    top_arr = candidats.groupby("num_arrondissement").size().idxmax()
    st.metric("Arrdt le plus sous-couvert", f"{top_arr}e")

# Explication des poids
st.info(
    f"**Pondération actuelle** : {round(poids_pression * 100)}% pression (demande VE) "
    f"+ {round((1 - poids_pression) * 100)}% couverture (distance). "
    f"Ajustez le curseur dans la barre latérale."
)

# Carte
st.markdown("### Carte des emplacements prioritaires")

top = candidats.head(nb_resultats)

fig = px.scatter_map(
    top,
    lat="latitude",
    lon="longitude",
    color="score_priorite",
    size="distance_borne_m",
    color_continuous_scale="YlOrRd",
    zoom=12,
    height=650,
    hover_data={
        "arr_label": True,
        "distance_borne_m": True,
        "pression": True,
        "nb_ve": True,
        "nb_pdc": True,
        "score_couverture": True,
        "score_pression": True,
        "score_priorite": True,
        "latitude": False,
        "longitude": False,
    },
    labels={
        "arr_label": "Arrondissement",
        "distance_borne_m": "Distance borne la + proche (m)",
        "pression": "Pression VE/borne",
        "nb_ve": "VE dans l'arrondissement",
        "nb_pdc": "Bornes dans l'arrondissement",
        "score_couverture": "Score couverture",
        "score_pression": "Score pression",
        "score_priorite": "Score final",
    },
)
fig.update_layout(
    mapbox_style="open-street-map",
    margin=dict(l=0, r=0, t=0, b=0),
)
st.plotly_chart(fig, width='stretch', config={"responsive": True})


def _log_with_retry(fn, *args, retries=6, delay=3, max_delay=20, **kwargs):
    """Retry an MLflow call on transient server/artifact-store errors.

    Un Space HF endormi peut mettre 30 à 60s à répondre correctement après
    son réveil (500 en attendant) : backoff exponentiel plafonné pour laisser
    ce temps de réveil plutôt qu'abandonner après quelques secondes.
    """
    for attempt in range(1, retries + 1):
        try:
            return fn(*args, **kwargs)
        except mlflow.exceptions.MlflowException:
            if attempt == retries:
                raise
            time.sleep(min(delay * 2 ** (attempt - 1), max_delay))


"""if st.button("Enregistrer dans MLflow"):
    if mlflow.active_run() is not None:
        mlflow.end_run()

    mlflow.set_tracking_uri("https://gregoryjedha-jedhaflow40.hf.space")
    mlflow.set_experiment(f"Bornes_DBSCAN_{datetime.now():%Y%m%d}")

    try:
        with mlflow.start_run(run_name=f"DBSCAN_Paris_{datetime.now():%Y%m%d}"):
            _log_with_retry(mlflow.log_metrics, {
                    "clusters": nb_clusters,
                    "noise_points": nb_bruit,
                    "candidate_zones": len(candidats),
                    "avg_priority": candidats["score_priorite"].mean()
            })

            # Chaque artefact est loggé indépendamment : si l'un échoue (ex: artifact
            # store temporairement indisponible), les autres et les métriques restent enregistrés.
            artifact_errors = []
            for label, upload in [
                ("modèle DBSCAN", lambda: _log_with_retry(
                    mlflow.sklearn.log_model, sk_model=db, artifact_path="DBSCAN_Paris"
                )),
                ("candidats.csv", lambda: (
                    candidats.to_csv("candidats.csv", index=False),
                    _log_with_retry(mlflow.log_artifact, "candidats.csv"),
                )),
                ("carte_priorites.html", lambda: (
                    fig.write_html("carte_priorites.html"),
                    _log_with_retry(mlflow.log_artifact, "carte_priorites.html"),
                )),
            ]:
                try:
                    upload()
                except mlflow.exceptions.MlflowException as e:
                    artifact_errors.append(f"{label} : {e}")

        if artifact_errors:
            st.warning(
                "Run enregistré dans MLflow (métriques et paramètres inclus), mais certains "
                "artefacts n'ont pas pu être uploadés après plusieurs tentatives (serveur/artifact "
                "store indisponible) :\n" + "\n".join(f"- {err}" for err in artifact_errors)
            )
        else:
            st.success("Run enregistré dans MLflow.")
    except mlflow.exceptions.MlflowException as e:
        st.error(f"Échec de l'enregistrement MLflow (serveur/artifact store indisponible) : {e}")
"""

# Répartition par arrondissement
col_bar, col_table = st.columns([3, 2])

with col_bar:
    st.markdown("### Zones candidates par arrondissement")
    par_arr = (
        candidats.groupby(["num_arrondissement", "arr_label"])
        .agg(
            nb_zones=("score_priorite", "count"),
            score_moyen=("score_priorite", "mean"),
            distance_max=("distance_borne_m", "max"),
        )
        .reset_index()
        .sort_values("nb_zones", ascending=True)
    )
    
    fig_bar = px.bar(
        par_arr,
        x="nb_zones",
        y="arr_label",
        orientation="h",
        color="score_moyen",
        color_continuous_scale="YlOrRd",
        height=500,
        text="nb_zones",
        labels={
            "nb_zones": "Zones à équiper",
            "arr_label": "",
            "score_moyen": "Score moyen",
        },
    )
    fig_bar.update_traces(textposition="outside")
    fig_bar.update_layout(margin=dict(l=0, r=40, t=10, b=0), showlegend=False)
    st.plotly_chart(fig_bar, width='stretch', config={"responsive": True})

with col_table:
    st.markdown("### Top 10 emplacements")
    st.dataframe(
        candidats.head(10)[[
            "arr_label", "latitude", "longitude",
            "distance_borne_m", "pression", "score_priorite",
        ]]
        .rename(columns={
            "arr_label": "Arrdt",
            "distance_borne_m": "Distance (m)",
            "pression": "Pression",
            "score_priorite": "Score",
        })
        .style.background_gradient(subset=["Score"], cmap="YlOrRd")
        .format({
            "Distance (m)": "{:.0f}",
            "Pression": "{:.1f}",
            "Score": "{:.3f}",
            "latitude": "{:.4f}",
            "longitude": "{:.4f}",
        }),
        width='stretch',
        hide_index=True,
        height=500,
    )


# ─── Méthodologie ───

with st.expander("📝 Méthodologie DBSCAN"):
    st.markdown(f"""
    **Algorithme** : DBSCAN (Density-Based Spatial Clustering of Applications with Noise)
    
    **Paramètres** :
    - `eps` = {distance_max}m (converti en {eps_rad:.6f} radians)
    - `min_samples` = {min_bornes_cluster} bornes minimum par cluster
    - Métrique = haversine (distance sur la surface terrestre)
    
    **Pourquoi haversine ?**
    Les coordonnées GPS sont sur une sphère. L'euclidienne traiterait 1° de latitude (111 km) 
    et 1° de longitude (~73 km à Paris) comme identiques, faussant les distances de 34%.
    Haversine donne directement des distances en mètres.
    
    **Pourquoi {distance_max}m ?**
    C'est la distance moyenne de marche qu'un conducteur accepte entre son stationnement 
    et une borne de recharge (source : AVERE France). En dessous, la zone est considérée couverte.
    
    **Score de priorité** :
    - Score couverture (distance normalisée) × {round((1 - poids_pression) * 100)}%
    - Score pression (VE/borne normalisé) × {round(poids_pression * 100)}%
    
    **Filtrage intra-muros** :
    La grille de points candidats est contrainte aux polygones des 20 arrondissements 
    via Shapely, excluant les communes limitrophes (Vincennes, Boulogne, etc.).
    
    **Résultats** :
    - {nb_clusters} clusters de bornes identifiés
    - {nb_bruit} bornes isolées (hors cluster)
    - {len(candidats)} zones candidates à plus de {distance_max}m d'une borne
    """)