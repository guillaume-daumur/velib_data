-- ============================================================
-- Vélib' — Monitoring : santé du pipeline (Kafka, KRaft, PySpark, producers)
-- ============================================================
-- Alimentée par monitoring/monitor.py (streaming insert, cf. monitoring/README.md).
-- Une ligne par composant vérifié, à chaque cycle du script de monitoring.
--
-- À coller dans l'éditeur SQL de la console BigQuery :
-- https://console.cloud.google.com/bigquery
--
-- Prérequis : créer manuellement le dataset "monitoring" (voir monitoring/README.md
-- étape 1) avant d'exécuter ce DDL.
--
-- Si la table existe déjà mais SANS schéma (créée à vide depuis la console,
-- ex. erreur "destination table has no schema" côté monitor.py) : le
-- CREATE TABLE IF NOT EXISTS ci-dessous ne fera rien tant qu'elle existe.
-- Il faut d'abord la supprimer :
--   DROP TABLE IF EXISTS `velib-data-498413.monitoring.pipeline_health`;
-- ============================================================

CREATE TABLE IF NOT EXISTS `velib-data-498413.monitoring.pipeline_health`
(
  check_timestamp     TIMESTAMP  OPTIONS(description='Timestamp UTC du cycle de vérification'),
  component           STRING     OPTIONS(description='Composant vérifié : kafka, kraft_controller, pyspark_velib_disponibilite, producer_velib_disponibilite'),
  status               STRING     OPTIONS(description='healthy | degraded | down'),
  latency_ms           FLOAT64    OPTIONS(description='Temps de réponse du check (ex. appel AdminClient Kafka), NULL si non applicable'),
  lag_seconds          FLOAT64    OPTIONS(description='Ancienneté de la dernière donnée traitée/publiée pour ce composant'),
  lag_messages         INT64      OPTIONS(description='Retard en nombre de messages Kafka non encore traités (pyspark uniquement), NULL sinon'),
  details              JSON       OPTIONS(description='Détails bruts du check (brokers, offsets, partitions...)'),
  error_message        STRING     OPTIONS(description='Message d\'erreur si status != healthy, NULL sinon'),
  night_mode_active    BOOL       OPTIONS(description='Vrai si le mode nuit (creux 2h-5h, lun-ven) était actif au moment du check — évite d\'interpréter le ralentissement volontaire comme une panne')
)
PARTITION BY DATE(check_timestamp)
CLUSTER BY component, status
OPTIONS(
  description = 'Santé du pipeline Vélib\' (Kafka/KRaft, PySpark Structured Streaming, producers) — alimentée par monitoring/monitor.py',
  require_partition_filter = false,
  -- Données opérationnelles, pas de valeur au-delà de quelques semaines de recul.
  partition_expiration_days = 90
);
