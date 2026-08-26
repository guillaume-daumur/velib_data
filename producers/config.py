"""
Configuration centralisée du producer Vélib'.
Les valeurs sensibles ou variables sont lues depuis les variables d'environnement.
"""

import os
from datetime import datetime
from zoneinfo import ZoneInfo

# ── API Open Data Paris ────────────────────────────────────────────────────────
BASE_URL = "https://opendata.paris.fr/api/explore/v2.1/catalog/datasets"

# L'API "emplacement des stations" est un sous-ensemble strict de l'API
# "disponibilité en temps réel" (mêmes champs stationcode/name/capacity/
# coordonnees_geo/station_opening_hours, la seconde ajoutant les champs
# dynamiques). Le périmètre ne retient donc que cette seule API — un seul
# producteur Kafka, un seul topic, sans perte d'information.
DATASET_DISPO = "velib-disponibilite-en-temps-reel"

# Champs à sélectionner pour le dataset disponibilité (réduction du payload)
FIELDS_DISPO = [
    "stationcode",
    "name",
    "is_installed",
    "capacity",
    "numdocksavailable",
    "numbikesavailable",
    "mechanical",
    "ebike",
    "is_renting",
    "is_returning",
    "duedate",
    "coordonnees_geo",
    "nom_arrondissement_communes",
    "code_insee_commune",
    "station_opening_hours",
]

# ── Kafka ──────────────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")

TOPIC_DISPO = "velib_disponibilite"

# ── Intervalle de polling ──────────────────────────────────────────────────────
DISPO_INTERVAL_SECONDS = int(os.getenv("DISPO_INTERVAL_SECONDS", "60"))

# ── Paramètres d'appel API ─────────────────────────────────────────────────────
API_RECORDS_LIMIT = 100    # Nombre de records par page (pagination)
API_TIMEZONE = "Europe/Paris"

# ── Mode nuit : réduction de la fréquence d'envoi pour limiter l'empreinte carbone ──
# Créneau de très faible usage Vélib' (2h-5h, du lundi au vendredi) : on multiplie
# volontairement les intervalles de polling pour réduire le volume de données qui
# traverse tout le pipeline en aval (Kafka, PySpark, GCS, BigQuery).
NIGHT_MODE_ENABLED = os.getenv("NIGHT_MODE_ENABLED", "true").lower() == "true"
NIGHT_MODE_START_HOUR = int(os.getenv("NIGHT_MODE_START_HOUR", "2"))
NIGHT_MODE_END_HOUR = int(os.getenv("NIGHT_MODE_END_HOUR", "5"))
NIGHT_MODE_MULTIPLIER = float(os.getenv("NIGHT_MODE_MULTIPLIER", "4"))
NIGHT_MODE_TZ = ZoneInfo(os.getenv("NIGHT_MODE_TIMEZONE", API_TIMEZONE))


def is_night_mode_active(now: datetime | None = None) -> bool:
    """Vrai du lundi au vendredi, entre NIGHT_MODE_START_HOUR et NIGHT_MODE_END_HOUR (heure de Paris)."""
    if not NIGHT_MODE_ENABLED:
        return False
    now = (now or datetime.now(NIGHT_MODE_TZ)).astimezone(NIGHT_MODE_TZ)
    return now.weekday() < 5 and NIGHT_MODE_START_HOUR <= now.hour < NIGHT_MODE_END_HOUR


def get_effective_interval(base_interval_seconds: int) -> int:
    """Multiplie l'intervalle nominal pendant le mode nuit, sinon le retourne tel quel."""
    if is_night_mode_active():
        return int(base_interval_seconds * NIGHT_MODE_MULTIPLIER)
    return base_interval_seconds
