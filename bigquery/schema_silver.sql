-- ============================================================
-- Vélib' — Couche silver : table typée et dédoublonnée
-- ============================================================
-- Alimentée en temps réel par la Cloud Function cloud_functions/silver_loader
-- à chaque nouveau fichier JSON déposé dans gs://<bucket>/velib_disponibilite/...
--
-- Prérequis : créer manuellement les datasets "silver" et "staging" dans la
-- console BigQuery (même région que le bucket, ex. europe-west1) avant
-- d'exécuter ce DDL. Le dataset "staging" doit avoir une expiration de table
-- par défaut (ex. 1 jour) : filet de sécurité si le nettoyage échoue.
--
-- Remplace MY_PROJECT_ID par ton vrai projet GCP.
-- ============================================================

CREATE TABLE IF NOT EXISTS `MY_PROJECT_ID.silver.velib_disponibilite`
(
  -- Clé de dédoublonnement : une ligne par (stationcode, request_timestamp)
  stationcode           STRING     OPTIONS(description='Code unique de la station'),
  request_timestamp     TIMESTAMP  OPTIONS(description='Timestamp UTC du cycle de polling API (clé de dédoublonnement)'),

  -- Champs station, typés (plus de STRING "OUI"/"NON" brut)
  name                  STRING,
  is_installed          BOOL       OPTIONS(description='Station installée'),
  capacity              INT64,
  numdocksavailable     INT64,
  numbikesavailable     INT64,
  mechanical            INT64,
  ebike                 INT64,
  is_renting            BOOL       OPTIONS(description='Station en location'),
  is_returning          BOOL       OPTIONS(description='Station accepte les retours'),
  duedate               TIMESTAMP,
  lat                   FLOAT64,
  lon                   FLOAT64,
  nom_arrondissement_communes STRING,
  code_insee_commune    STRING,
  station_opening_hours STRING,

  -- Métadonnées de traçabilité
  kafka_timestamp       TIMESTAMP  OPTIONS(description='Timestamp assigné par le broker Kafka — utilisé pour garder la version la plus ancienne en cas de doublon'),
  insert_timestamp      TIMESTAMP  OPTIONS(description='Timestamp d\'écriture du fichier raw dans Cloud Storage par PySpark'),
  silver_updated_at     TIMESTAMP  OPTIONS(description='Timestamp de la dernière écriture/mise à jour dans cette table silver')
)
PARTITION BY DATE(request_timestamp)
CLUSTER BY stationcode
OPTIONS(
  description = 'Disponibilité Vélib\' — typée et dédoublonnée (1 ligne par station/request_timestamp), alimentée en temps réel depuis Cloud Storage',
  require_partition_filter = false
);
