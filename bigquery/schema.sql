-- ============================================================
-- Vélib' — Création du dataset "raw" et des 2 tables BigQuery
-- ============================================================
-- À coller dans l'éditeur SQL de la console BigQuery :
-- https://console.cloud.google.com/bigquery
--
-- Remplace MY_PROJECT_ID par ton vrai projet GCP.
-- Les deux instructions CREATE TABLE peuvent être exécutées
-- l'une après l'autre dans le même éditeur.
-- ============================================================


-- ── 1. Dataset "raw" ─────────────────────────────────────────
-- La console BigQuery ne crée pas les datasets via SQL.
-- Crée-le manuellement :
--   → Panneau gauche : clic sur ton projet → "Créer un ensemble de données"
--   → ID : raw
--   → Région : europe-west1  (ou celle de ton choix)
--   → Laisser les autres options par défaut → Créer
-- Puis exécute les CREATE TABLE ci-dessous.
-- ─────────────────────────────────────────────────────────────


-- ── 2. Table : disponibilité temps réel ──────────────────────
CREATE TABLE IF NOT EXISTS `MY_PROJECT_ID.raw.velib_disponibilite`
(
  -- Métadonnées pipeline
  request_timestamp     TIMESTAMP  OPTIONS(description='Timestamp UTC du début de l\'appel API par le producer'),
  insert_timestamp      TIMESTAMP  OPTIONS(description='Timestamp UTC d\'insertion dans BigQuery par PySpark'),
  dataset_id            STRING     OPTIONS(description='Identifiant du dataset source Opendatasoft'),

  -- Champs station
  stationcode           STRING     OPTIONS(description='Code unique de la station'),
  name                  STRING     OPTIONS(description='Nom de la station'),
  is_installed          STRING     OPTIONS(description='OUI si la station est installée'),
  capacity              INT64      OPTIONS(description='Nombre total de bornettes'),
  numdocksavailable     INT64      OPTIONS(description='Bornettes disponibles'),
  numbikesavailable     INT64      OPTIONS(description='Vélos disponibles'),
  mechanical            INT64      OPTIONS(description='Vélos mécaniques disponibles'),
  ebike                 INT64      OPTIONS(description='Vélos électriques disponibles'),
  is_renting            STRING     OPTIONS(description='OUI si la station loue des vélos'),
  is_returning          STRING     OPTIONS(description='OUI si la station accepte des retours'),
  duedate               STRING     OPTIONS(description='Date de dernière mise à jour du statut'),
  coordonnees_geo       STRUCT<lat FLOAT64, lon FLOAT64>  OPTIONS(description='Coordonnées GPS de la station'),
  nom_arrondissement_communes  STRING  OPTIONS(description='Nom de l\'arrondissement ou commune'),
  code_insee_commune    STRING     OPTIONS(description='Code INSEE de la commune'),
  station_opening_hours STRING     OPTIONS(description='Horaires d\'ouverture de la station')
)
PARTITION BY DATE(request_timestamp)
CLUSTER BY stationcode
OPTIONS(
  description = 'Disponibilité temps réel des stations Vélib\' — données brutes ingérées via Kafka + PySpark',
  require_partition_filter = false
);


-- ── 3. Table : emplacements des stations ─────────────────────
CREATE TABLE IF NOT EXISTS `MY_PROJECT_ID.raw.velib_stations`
(
  -- Métadonnées pipeline
  request_timestamp     TIMESTAMP  OPTIONS(description='Timestamp UTC du début de l\'appel API par le producer'),
  insert_timestamp      TIMESTAMP  OPTIONS(description='Timestamp UTC d\'insertion dans BigQuery par PySpark'),
  dataset_id            STRING     OPTIONS(description='Identifiant du dataset source Opendatasoft'),

  -- Champs station (données quasi-statiques)
  stationcode           STRING     OPTIONS(description='Code unique de la station'),
  name                  STRING     OPTIONS(description='Nom de la station'),
  capacity              INT64      OPTIONS(description='Nombre total de bornettes'),
  coordonnees_geo       STRUCT<lat FLOAT64, lon FLOAT64>  OPTIONS(description='Coordonnées GPS de la station'),
  nom_arrondissement_communes  STRING  OPTIONS(description='Nom de l\'arrondissement ou commune'),
  code_insee_commune    STRING     OPTIONS(description='Code INSEE de la commune'),
  station_opening_hours STRING     OPTIONS(description='Horaires d\'ouverture de la station'),
  duedate               STRING     OPTIONS(description='Date de dernière mise à jour')
)
PARTITION BY DATE(request_timestamp)
CLUSTER BY stationcode
OPTIONS(
  description = 'Emplacements et caractéristiques fixes des stations Vélib\' — données brutes'
);
