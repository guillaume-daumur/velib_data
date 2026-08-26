"""
Vélib' Pipeline Health Monitor

Boucle infinie : à chaque cycle, vérifie la santé de Kafka, du contrôleur KRaft,
du stream PySpark (velib_disponibilite) et de la fraîcheur des données publiées
par le producer — puis envoie une ligne par composant dans BigQuery
(`monitoring.pipeline_health`) via streaming insert.

Le script ralentit lui-même sa fréquence pendant le mode nuit (2h-5h, lun-ven,
heure de Paris — cf. config.is_night_mode_active), au même rythme que les
producers, pour limiter l'empreinte carbone du pipeline dans son ensemble.
"""

import json
import logging
import time
from datetime import datetime, timezone

from google.cloud import bigquery

import config
import health_checks as hc

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger("velib-monitor")

TABLE_ID = f"{config.GCP_PROJECT_ID}.{config.BQ_MONITORING_DATASET}.{config.BQ_MONITORING_TABLE}"


def run_checks() -> list[dict]:
    """Exécute tous les checks et retourne les lignes prêtes à insérer dans BigQuery."""
    now = datetime.now(timezone.utc)
    night_mode = config.is_night_mode_active()

    kafka_result = hc.check_kafka()
    rows = [
        kafka_result,
        hc.kraft_result_from_kafka(kafka_result),
        hc.check_pyspark_stream(config.TOPIC_DISPO),
        hc.check_producer_freshness(config.TOPIC_DISPO, config.DISPO_INTERVAL_SECONDS),
    ]

    for row in rows:
        row["check_timestamp"] = now.isoformat()
        row["night_mode_active"] = night_mode
        # BigQuery attend une chaîne JSON pour une colonne de type JSON.
        row["details"] = json.dumps(row.get("details") or {}, ensure_ascii=False)

    return rows


def send_to_bigquery(client: bigquery.Client, rows: list[dict]) -> None:
    errors = client.insert_rows_json(TABLE_ID, rows)
    if errors:
        logger.error(f"Erreurs d'insertion BigQuery : {errors}")
    else:
        logger.info(f"{len(rows)} lignes envoyées → {TABLE_ID}")


def wait_for_kafka(max_retries: int = 20, delay_seconds: int = 5) -> None:
    """Bloque jusqu'à ce que le broker Kafka soit joignable (même logique que producers/producer.py)."""
    logger.info(f"En attente de Kafka sur {config.KAFKA_BOOTSTRAP_SERVERS}...")
    for attempt in range(1, max_retries + 1):
        result = hc.check_kafka()
        if result["status"] == "healthy":
            logger.info(f"Kafka prêt (tentative {attempt}).")
            return
        logger.warning(f"Kafka pas encore prêt ({attempt}/{max_retries}) : {result['error_message']}")
        time.sleep(delay_seconds)
    logger.warning("Kafka injoignable après plusieurs tentatives — démarrage du monitoring quand même "
                    "(les checks Kafka remonteront 'down' tant qu'il n'est pas prêt).")


if __name__ == "__main__":
    wait_for_kafka()

    bq_client = bigquery.Client(project=config.GCP_PROJECT_ID)
    logger.info(f"Monitoring démarré — envoi vers {TABLE_ID}.")

    while True:
        try:
            rows = run_checks()
            for row in rows:
                suffix = f" — {row['error_message']}" if row.get("error_message") else ""
                logger.info(f"{row['component']}: {row['status']}{suffix}")
            send_to_bigquery(bq_client, rows)
        except Exception as exc:
            logger.error(f"Erreur dans la boucle de monitoring : {exc}", exc_info=True)

        interval = config.get_effective_monitor_interval()
        if interval != config.MONITOR_INTERVAL_SECONDS:
            logger.info(f"Mode nuit actif (creux 2h-5h, lun-ven) — prochain cycle dans {interval}s "
                        f"au lieu de {config.MONITOR_INTERVAL_SECONDS}s.")
        time.sleep(interval)
