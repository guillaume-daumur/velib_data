"""
Cloud Function (2e génération) — Cloud Storage → BigQuery silver, en temps réel.

Déclenchée à chaque nouveau fichier JSON déposé dans le bucket raw
(gs://<bucket>/velib_disponibilite/AAAA/MM/JJ/epoch-N.json). Charge le fichier
dans une table de staging temporaire (1 par invocation), puis MERGE vers la
table silver en dédoublonnant sur (stationcode, request_timestamp) — ne garde
que la ligne au kafka_timestamp le plus ancien en cas de doublon.

Le filtre `DATE(target.request_timestamp) = @batch_date` dans le MERGE permet
à BigQuery d'élaguer les partitions : chaque exécution ne scanne que la
journée concernée dans la table silver, pas tout l'historique.

Variables d'environnement (à définir au déploiement) :
  GCP_PROJECT_ID     : projet GCP
  BQ_SILVER_DATASET  : dataset de la table silver (défaut "silver")
  BQ_STAGING_DATASET : dataset des tables de staging temporaires (défaut "staging")
"""

import logging
import os
import uuid

import functions_framework
from google.cloud import bigquery

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("velib-silver-loader")

PROJECT_ID      = os.environ["GCP_PROJECT_ID"]
SILVER_DATASET  = os.environ.get("BQ_SILVER_DATASET", "silver")
STAGING_DATASET = os.environ.get("BQ_STAGING_DATASET", "staging")
SILVER_TABLE    = "velib_disponibilite"
RAW_PREFIX      = "velib_disponibilite/"

# Schéma du fichier JSON brut (voir pyspark/spark_streaming_job.py : parse_dispo + _make_gcs_writer)
STAGING_SCHEMA = [
    bigquery.SchemaField("request_timestamp", "TIMESTAMP"),
    bigquery.SchemaField("kafka_timestamp", "TIMESTAMP"),
    bigquery.SchemaField("dataset_id", "STRING"),
    bigquery.SchemaField("stationcode", "STRING"),
    bigquery.SchemaField("name", "STRING"),
    bigquery.SchemaField("is_installed", "STRING"),
    bigquery.SchemaField("capacity", "INTEGER"),
    bigquery.SchemaField("numdocksavailable", "INTEGER"),
    bigquery.SchemaField("numbikesavailable", "INTEGER"),
    bigquery.SchemaField("mechanical", "INTEGER"),
    bigquery.SchemaField("ebike", "INTEGER"),
    bigquery.SchemaField("is_renting", "STRING"),
    bigquery.SchemaField("is_returning", "STRING"),
    bigquery.SchemaField("duedate", "STRING"),
    bigquery.SchemaField("coordonnees_geo", "RECORD", fields=[
        bigquery.SchemaField("lat", "FLOAT"),
        bigquery.SchemaField("lon", "FLOAT"),
    ]),
    bigquery.SchemaField("nom_arrondissement_communes", "STRING"),
    bigquery.SchemaField("code_insee_commune", "STRING"),
    bigquery.SchemaField("station_opening_hours", "STRING"),
    bigquery.SchemaField("insert_timestamp", "TIMESTAMP"),
]

# `{{staging_table}}` reste littéral après le f-string (échappé), substitué plus bas via .format()
MERGE_SQL_TEMPLATE = f"""
MERGE `{PROJECT_ID}.{SILVER_DATASET}.{SILVER_TABLE}` AS target
USING (
  SELECT
    stationcode,
    request_timestamp,
    name,
    is_installed = 'OUI' AS is_installed,
    capacity,
    numdocksavailable,
    numbikesavailable,
    mechanical,
    ebike,
    is_renting = 'OUI' AS is_renting,
    is_returning = 'OUI' AS is_returning,
    SAFE_CAST(duedate AS TIMESTAMP) AS duedate,
    coordonnees_geo.lat AS lat,
    coordonnees_geo.lon AS lon,
    nom_arrondissement_communes,
    code_insee_commune,
    station_opening_hours,
    kafka_timestamp,
    insert_timestamp
  FROM `{{staging_table}}`
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY stationcode, request_timestamp
    ORDER BY kafka_timestamp ASC
  ) = 1
) AS source
ON  target.stationcode = source.stationcode
AND target.request_timestamp = source.request_timestamp
-- Élagage de partition explicite : borne le scan de `target` à la seule
-- journée de ce batch, au lieu de toute la table silver.
AND DATE(target.request_timestamp) = @batch_date
WHEN MATCHED AND source.kafka_timestamp < target.kafka_timestamp THEN
  UPDATE SET
    name = source.name, is_installed = source.is_installed, capacity = source.capacity,
    numdocksavailable = source.numdocksavailable, numbikesavailable = source.numbikesavailable,
    mechanical = source.mechanical, ebike = source.ebike,
    is_renting = source.is_renting, is_returning = source.is_returning,
    duedate = source.duedate, lat = source.lat, lon = source.lon,
    nom_arrondissement_communes = source.nom_arrondissement_communes,
    code_insee_commune = source.code_insee_commune,
    station_opening_hours = source.station_opening_hours,
    kafka_timestamp = source.kafka_timestamp,
    insert_timestamp = source.insert_timestamp,
    silver_updated_at = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN
  INSERT (
    stationcode, request_timestamp, name, is_installed, capacity, numdocksavailable,
    numbikesavailable, mechanical, ebike, is_renting, is_returning, duedate, lat, lon,
    nom_arrondissement_communes, code_insee_commune, station_opening_hours,
    kafka_timestamp, insert_timestamp, silver_updated_at
  )
  VALUES (
    source.stationcode, source.request_timestamp, source.name, source.is_installed,
    source.capacity, source.numdocksavailable, source.numbikesavailable, source.mechanical,
    source.ebike, source.is_renting, source.is_returning, source.duedate, source.lat, source.lon,
    source.nom_arrondissement_communes, source.code_insee_commune, source.station_opening_hours,
    source.kafka_timestamp, source.insert_timestamp, CURRENT_TIMESTAMP()
  )
"""


@functions_framework.cloud_event
def on_new_raw_file(cloud_event):
    """Point d'entrée — déclenché par l'événement Cloud Storage `finalized`."""
    data = cloud_event.data
    bucket_name = data["bucket"]
    object_name = data["name"]

    if not object_name.startswith(RAW_PREFIX) or not object_name.endswith(".json"):
        logger.info("Ignoré (hors périmètre) : %s", object_name)
        return

    # Chemin attendu : velib_disponibilite/AAAA/MM/JJ/epoch-N.json
    parts = object_name.split("/")
    if len(parts) != 5:
        logger.error("Chemin inattendu, impossible d'en extraire la date : %s", object_name)
        return
    _, year, month, day, _ = parts
    batch_date = f"{year}-{month}-{day}"

    client = bigquery.Client(project=PROJECT_ID)
    staging_table_id = f"{PROJECT_ID}.{STAGING_DATASET}.stg_{SILVER_TABLE}_{uuid.uuid4().hex}"
    gcs_uri = f"gs://{bucket_name}/{object_name}"

    logger.info("Chargement %s → %s", gcs_uri, staging_table_id)
    load_job = client.load_table_from_uri(
        gcs_uri,
        staging_table_id,
        job_config=bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            schema=STAGING_SCHEMA,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        ),
    )
    load_job.result()

    try:
        merge_sql = MERGE_SQL_TEMPLATE.format(staging_table=staging_table_id)
        query_job = client.query(
            merge_sql,
            job_config=bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("batch_date", "DATE", batch_date),
                ]
            ),
        )
        query_job.result()
        logger.info(
            "MERGE terminé pour %s (%d lignes affectées)",
            gcs_uri, query_job.num_dml_affected_rows,
        )
    finally:
        client.delete_table(staging_table_id, not_found_ok=True)
