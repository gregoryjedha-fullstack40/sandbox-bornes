# DAGs Airflow — Bornes VE Paris

## `collecte_bornes_ve_paris.py`

Remplace l'ancien serveur Airflow ALH (`alh-consulting.com`, devenu
indisponible) comme fournisseur du cache S3 que `etl.collect_data()` lit en
priorité. Ce DAG tourne dans une instance Airflow séparée de l'appli
Streamlit (celle-ci est déployée sur Hugging Face Spaces, un conteneur
Docker sans état) — il n'a pas vocation à être packagé avec l'appli.

### Déploiement

1. Installer Airflow (non listé dans `requirements.txt`, qui est le
   requirements de l'appli Streamlit) : `pip install apache-airflow`, plus
   les dépendances déjà utilisées par `etl.py` (`pandas`, `requests`,
   `boto3`) dans le même environnement Python que les workers Airflow.
2. Copier ou monter ce repo sur les workers, puis définir la variable
   d'environnement `BORNES_PROJECT_DIR` pointant vers sa racine (le DAG
   l'ajoute à `sys.path` pour pouvoir faire `import etl`).
3. Définir `S3_BUCKET` et `AWS_REGION` (mêmes valeurs que l'appli
   Streamlit), et donner aux workers des credentials AWS valides
   (rôle IAM ou variables standard `AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY`).
4. Déposer `collecte_bornes_ve_paris.py` dans le `dags_folder` Airflow (ou
   pointer `dags_folder` vers ce dossier).

### Ce que le DAG fait / ne fait pas

- Republie chaque source brute (`belib_stat`, `belib_rt`, `irve_conso`,
  `irve_dyn`, `energie`, `vehicules`) sous `raw/data/<source>.csv` sur S3 —
  la clé exacte lue par `etl.lecture_s3()`.
- Recalcule et republie les jeux dérivés (`bornes`, `pression`,
  `projections_arrdt`, `projections_paris`) à partir de ce cache frais.
- N'écrit **pas** dans `bornes.db` (SQLite) : cette base est reconstruite
  par `database.assurer_donnees_disponibles()` au démarrage du conteneur
  Streamlit, à partir du cache S3 que ce DAG maintient à jour.
- Ne touche pas aux données `population` (INSEE) : `etl.py` n'a pas de
  source publique en direct pour ce jeu de données, il reste géré/déposé
  manuellement sur S3 (`raw/data/paris_population.csv`).

### Prochaine étape possible (non faite ici)

Le cold-start de l'appli Streamlit (`bornes_arrondissements.py`, lancé par
`database.assurer_donnees_disponibles()`) recalcule aujourd'hui
`bornes`/`pression`/`projections` en process à chaque redémarrage du
conteneur. Une fois ce DAG en place, ces mêmes résultats sont déjà
disponibles sur S3 (`raw/data/bornes.csv`, `pression.csv`,
`projections_*.csv`) : faire lire ces caches à l'appli au lieu de tout
recalculer accélérerait son démarrage. Changement côté appli, hors périmètre
de ce DAG.
