"""
Configuration centralisée du producer Vélib'.
Les valeurs sensibles ou variables sont lues depuis les variables d'environnement.
"""

import os

# ── API Open Data Paris ────────────────────────────────────────────────────────
BASE_URL = "https://opendata.paris.fr/api/explore/v2.1/catalog/datasets"

DATASET_DISPO = "velib-disponibilite-en-temps-reel"
DATASET_EMPLACE = "velib-emplacement-des-stations"

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
TOPIC_STATIONS = "velib_stations"

# ── Intervalles de polling ─────────────────────────────────────────────────────
DISPO_INTERVAL_SECONDS = int(os.getenv("DISPO_INTERVAL_SECONDS", "60"))
STATIONS_INTERVAL_SECONDS = int(os.getenv("STATIONS_INTERVAL_SECONDS", "21600"))

# ── Paramètres d'appel API ─────────────────────────────────────────────────────
API_RECORDS_LIMIT = 100    # Nombre de records par page (pagination)
API_TIMEZONE = "Europe/Paris"
