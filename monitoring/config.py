"""
Configuration centralisée du script de monitoring Vélib'.
Les valeurs sensibles ou variables sont lues depuis les variables d'environnement.
"""

import os
from datetime import datetime
from zoneinfo import ZoneInfo

# ── Kafka ──────────────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_CHECK_TIMEOUT_SECONDS = int(os.getenv("KAFKA_CHECK_TIMEOUT_SECONDS", "10"))

TOPIC_DISPO = "velib_disponibilite"

# ── BigQuery ───────────────────────────────────────────────────────────────────
GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID", "MY_PROJECT_ID")
BQ_MONITORING_DATASET = os.getenv("BQ_MONITORING_DATASET", "monitoring")
BQ_MONITORING_TABLE = "pipeline_health"

# ── PySpark (lecture des checkpoints locaux, montés en lecture seule) ──────────
CHECKPOINT_BASE = os.getenv("SPARK_CHECKPOINT_DIR", "/app/data/checkpoints")
# Au-delà de ce seuil sans nouveau micro-batch committé, un stream est "degraded" ;
# au-delà de 3x ce seuil, il est considéré "down".
PYSPARK_STALENESS_THRESHOLD_SECONDS = int(os.getenv("PYSPARK_STALENESS_THRESHOLD_SECONDS", "120"))

# ── Intervalle nominal du producer (référence pour juger de la fraîcheur des
#    données publiées — doit matcher producers/config.py) ──
DISPO_INTERVAL_SECONDS = int(os.getenv("DISPO_INTERVAL_SECONDS", "60"))

# ── Boucle de monitoring ─────────────────────────────────────────────────────────
MONITOR_INTERVAL_SECONDS = int(os.getenv("MONITOR_INTERVAL_SECONDS", "60"))

# ── Mode nuit : réduction de la fréquence d'envoi pour limiter l'empreinte carbone ──
# Doit rester cohérent avec producers/config.py (mêmes variables d'environnement),
# afin que les seuils de fraîcheur des checks producers ne déclenchent pas de faux
# positifs pendant le ralentissement volontaire nocturne.
NIGHT_MODE_ENABLED = os.getenv("NIGHT_MODE_ENABLED", "true").lower() == "true"
NIGHT_MODE_START_HOUR = int(os.getenv("NIGHT_MODE_START_HOUR", "2"))
NIGHT_MODE_END_HOUR = int(os.getenv("NIGHT_MODE_END_HOUR", "5"))
NIGHT_MODE_MULTIPLIER = float(os.getenv("NIGHT_MODE_MULTIPLIER", "4"))
NIGHT_MODE_TZ = ZoneInfo(os.getenv("NIGHT_MODE_TIMEZONE", "Europe/Paris"))


def is_night_mode_active(now: datetime | None = None) -> bool:
    """
    Vrai du lundi au vendredi, entre NIGHT_MODE_START_HOUR et NIGHT_MODE_END_HOUR
    (heure de Paris) — créneau de très faible usage Vélib' où le pipeline ralentit
    volontairement ses envois pour réduire son empreinte carbone.
    """
    if not NIGHT_MODE_ENABLED:
        return False
    now = (now or datetime.now(NIGHT_MODE_TZ)).astimezone(NIGHT_MODE_TZ)
    return now.weekday() < 5 and NIGHT_MODE_START_HOUR <= now.hour < NIGHT_MODE_END_HOUR


def get_effective_monitor_interval() -> int:
    """Espace les cycles de monitoring pendant le mode nuit, comme le reste du pipeline."""
    if is_night_mode_active():
        return int(MONITOR_INTERVAL_SECONDS * NIGHT_MODE_MULTIPLIER)
    return MONITOR_INTERVAL_SECONDS
