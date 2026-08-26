"""
Checks de santé du pipeline Vélib' : Kafka, KRaft, PySpark Structured Streaming,
et fraîcheur des données publiées par les producers.

Chaque fonction retourne un dict "brut" (sans check_timestamp ni night_mode_active,
ajoutés par monitor.py au moment de l'envoi) au format :
{
    "component":     str,
    "status":        "healthy" | "degraded" | "down",
    "latency_ms":    float | None,
    "lag_seconds":   float | None,
    "lag_messages":  int | None,
    "details":       dict,
    "error_message": str | None,
}
"""

import glob
import json
import logging
import os
import time

from confluent_kafka import Consumer, TopicPartition
from confluent_kafka.admin import AdminClient

import config

logger = logging.getLogger("velib-monitor")


def _empty_result(component: str, status: str, error_message: str | None = None, **extra) -> dict:
    row = {
        "component": component,
        "status": status,
        "latency_ms": None,
        "lag_seconds": None,
        "lag_messages": None,
        "details": {},
        "error_message": error_message,
    }
    row.update(extra)
    return row


# ── Kafka / KRaft ────────────────────────────────────────────────────────────────

def check_kafka() -> dict:
    """
    Interroge les métadonnées du cluster via AdminClient. En mode KRaft combiné
    (broker + controller dans le même process, cf. docker-compose.yml), une requête
    de métadonnées qui aboutit prouve à la fois que le broker répond ET que le
    quorum KRaft a élu un contrôleur actif (controller_id renseigné).
    """
    admin = AdminClient({"bootstrap.servers": config.KAFKA_BOOTSTRAP_SERVERS})
    start = time.monotonic()
    try:
        metadata = admin.list_topics(timeout=config.KAFKA_CHECK_TIMEOUT_SECONDS)
        latency_ms = (time.monotonic() - start) * 1000
        return {
            "component": "kafka",
            "status": "healthy",
            "latency_ms": round(latency_ms, 1),
            "lag_seconds": None,
            "lag_messages": None,
            "details": {
                "broker_count": len(metadata.brokers),
                "topic_count": len(metadata.topics),
                "controller_id": metadata.controller_id,
            },
            "error_message": None,
        }
    except Exception as exc:
        logger.warning(f"Check Kafka en échec : {exc}")
        return _empty_result("kafka", "down", error_message=str(exc))


def kraft_result_from_kafka(kafka_result: dict) -> dict:
    """
    Dérive le statut du contrôleur KRaft à partir du résultat check_kafka(), sans
    appel réseau supplémentaire (le controller_id fait déjà partie des métadonnées
    Kafka retournées ci-dessus). Exposé comme composant distinct dans BigQuery pour
    pouvoir suivre/alerter dessus indépendamment si l'architecture évolue un jour
    vers un contrôleur KRaft dédié (process séparé du broker).
    """
    row = dict(kafka_result)
    row["component"] = "kraft_controller"
    if row["status"] == "healthy" and row["details"].get("controller_id", -1) < 0:
        row["status"] = "down"
        row["error_message"] = "Aucun contrôleur KRaft actif (controller_id manquant)."
    return row


def get_topic_high_watermark(topic: str) -> int | None:
    """Retourne le dernier offset publié (high watermark) sur la partition 0 du topic."""
    consumer = Consumer({
        "bootstrap.servers": config.KAFKA_BOOTSTRAP_SERVERS,
        "group.id": "velib-monitor-watermark",
        "enable.auto.commit": False,
    })
    try:
        _low, high = consumer.get_watermark_offsets(
            TopicPartition(topic, 0), timeout=config.KAFKA_CHECK_TIMEOUT_SECONDS, cached=False,
        )
        return high
    except Exception as exc:
        logger.warning(f"Impossible de lire le watermark de '{topic}' : {exc}")
        return None
    finally:
        consumer.close()


# ── PySpark Structured Streaming ──────────────────────────────────────────────────

def _read_latest_committed_offset(topic: str) -> tuple[int | None, float | None]:
    """
    Lit le dernier offset committé par Spark pour ce topic à partir du fichier de
    checkpoint `<CHECKPOINT_BASE>/<topic>/offsets/<batchId>` (le plus récent).

    Format d'un fichier offsets Spark (Kafka source) :
      ligne 0 : "v1"
      ligne 1 : métadonnées JSON du batch (watermark, timestamp...)
      ligne 2 : JSON des offsets par topic/partition, ex. {"velib_disponibilite":{"0":1234}}
    """
    offsets_dir = os.path.join(config.CHECKPOINT_BASE, topic, "offsets")
    batch_files = [
        f for f in glob.glob(os.path.join(offsets_dir, "*"))
        if os.path.basename(f).isdigit()
    ]
    if not batch_files:
        return None, None

    latest_file = max(batch_files, key=lambda f: int(os.path.basename(f)))
    mtime = os.path.getmtime(latest_file)
    with open(latest_file, "r", encoding="utf-8") as fh:
        lines = fh.read().splitlines()

    offsets_by_topic = json.loads(lines[2])
    committed_offset = offsets_by_topic.get(topic, {}).get("0")
    return committed_offset, mtime


def check_pyspark_stream(topic: str) -> dict:
    """
    Santé du stream PySpark consommant `topic` : fraîcheur du dernier checkpoint
    committé, et retard (en nombre de messages) par rapport au dernier offset
    publié dans Kafka.
    """
    component = f"pyspark_{topic}"
    try:
        committed_offset, mtime = _read_latest_committed_offset(topic)
        if committed_offset is None:
            return _empty_result(
                component, "down",
                error_message="Aucun checkpoint trouvé — le job n'a encore traité aucun micro-batch.",
            )

        lag_seconds = time.time() - mtime
        high_watermark = get_topic_high_watermark(topic)
        lag_messages = None if high_watermark is None else max(high_watermark - committed_offset, 0)

        threshold = config.PYSPARK_STALENESS_THRESHOLD_SECONDS
        if lag_seconds <= threshold:
            status = "healthy"
        elif lag_seconds <= threshold * 3:
            status = "degraded"
        else:
            status = "down"

        return {
            "component": component,
            "status": status,
            "latency_ms": None,
            "lag_seconds": round(lag_seconds, 1),
            "lag_messages": lag_messages,
            "details": {
                "committed_offset": committed_offset,
                "kafka_high_watermark": high_watermark,
            },
            "error_message": None,
        }
    except Exception as exc:
        logger.warning(f"Check PySpark '{topic}' en échec : {exc}")
        return _empty_result(component, "down", error_message=str(exc))


# ── Fraîcheur des données publiées par les producers ──────────────────────────────

def check_producer_freshness(topic: str, expected_interval_seconds: int) -> dict:
    """
    Lit le dernier message publié sur `topic` et compare son timestamp Kafka à
    maintenant. Sert de proxy à la santé du producer : s'il ne publie plus rien,
    l'âge du dernier message grandit sans limite.

    La tolérance est doublée (et quadruplée en mode nuit, via NIGHT_MODE_MULTIPLIER)
    par rapport à l'intervalle nominal, pour absorber la gigue normale entre deux
    cycles sans déclencher de faux positifs.
    """
    component = f"producer_{topic}"
    consumer = Consumer({
        "bootstrap.servers": config.KAFKA_BOOTSTRAP_SERVERS,
        "group.id": f"velib-monitor-{topic}",
        "enable.auto.commit": False,
    })
    try:
        tp = TopicPartition(topic, 0)
        low, high = consumer.get_watermark_offsets(
            tp, timeout=config.KAFKA_CHECK_TIMEOUT_SECONDS, cached=False,
        )
        if high <= low:
            return _empty_result(component, "down", error_message="Aucun message publié sur ce topic.")

        tp.offset = high - 1
        consumer.assign([tp])
        msg = consumer.poll(timeout=config.KAFKA_CHECK_TIMEOUT_SECONDS)
        if msg is None or msg.error():
            return _empty_result(
                component, "down",
                error_message=f"Impossible de lire le dernier message ({msg.error() if msg else 'timeout'}).",
            )

        _ts_type, kafka_ts_ms = msg.timestamp()
        age_seconds = time.time() - (kafka_ts_ms / 1000)

        multiplier = config.NIGHT_MODE_MULTIPLIER if config.is_night_mode_active() else 1
        tolerance = expected_interval_seconds * multiplier * 2

        if age_seconds <= tolerance:
            status = "healthy"
        elif age_seconds <= tolerance * 3:
            status = "degraded"
        else:
            status = "down"

        return {
            "component": component,
            "status": status,
            "latency_ms": None,
            "lag_seconds": round(age_seconds, 1),
            "lag_messages": None,
            "details": {"latest_offset": high},
            "error_message": None,
        }
    except Exception as exc:
        logger.warning(f"Check producer '{topic}' en échec : {exc}")
        return _empty_result(component, "down", error_message=str(exc))
    finally:
        consumer.close()
