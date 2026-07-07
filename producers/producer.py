"""
Vélib' Kafka Producer

Deux boucles tournent en parallèle dans des threads séparés :
  - run_dispo_loop  : disponibilité temps réel, toutes les DISPO_INTERVAL_SECONDS
  - run_stations_loop : emplacements des stations, toutes les STATIONS_INTERVAL_SECONDS

Format de chaque message Kafka :
{
    "dataset_id": str,
    "ingestion_timestamp": str (ISO-8601 UTC),
    "record": str (JSON du record brut de l'API)
}
"""

import json
import logging
import threading
import time
from datetime import datetime, timezone

import requests
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

import config

# ── Logging ────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger("velib-producer")


# ── Kafka helpers ──────────────────────────────────────────────────────────────

def wait_for_kafka(max_retries: int = 20, delay_seconds: int = 5) -> None:
    """Bloque jusqu'à ce que le broker Kafka soit joignable."""
    logger.info(f"En attente de Kafka sur {config.KAFKA_BOOTSTRAP_SERVERS}...")
    for attempt in range(1, max_retries + 1):
        try:
            admin = AdminClient({"bootstrap.servers": config.KAFKA_BOOTSTRAP_SERVERS})
            admin.list_topics(timeout=5)
            logger.info(f"Kafka prêt (tentative {attempt}).")
            return
        except Exception as exc:
            logger.warning(f"Kafka pas encore prêt ({attempt}/{max_retries}) : {exc}")
            time.sleep(delay_seconds)
    raise RuntimeError(f"Impossible de joindre Kafka après {max_retries} tentatives.")


def ensure_topics(topics: list[str]) -> None:
    """Crée les topics Kafka s'ils n'existent pas encore."""
    admin = AdminClient({"bootstrap.servers": config.KAFKA_BOOTSTRAP_SERVERS})
    new_topics = [NewTopic(t, num_partitions=1, replication_factor=1) for t in topics]
    futures = admin.create_topics(new_topics)
    for topic, future in futures.items():
        try:
            future.result()
            logger.info(f"Topic '{topic}' créé.")
        except Exception as exc:
            # Le topic existe déjà : c'est normal
            logger.info(f"Topic '{topic}' déjà existant ou ignoré : {exc}")


def make_producer() -> Producer:
    """Construit et retourne un Producer confluent-kafka."""
    return Producer({
        "bootstrap.servers": config.KAFKA_BOOTSTRAP_SERVERS,
        "client.id": "velib-producer",
        "retries": 5,
        "retry.backoff.ms": 1000,
    })


def delivery_report(err, msg) -> None:
    """Callback appelé par confluent-kafka après livraison (succès ou échec)."""
    if err:
        logger.error(f"Échec livraison sur {msg.topic()} : {err}")
    else:
        logger.debug(
            f"Livré → {msg.topic()} [partition {msg.partition()}] offset {msg.offset()}"
        )


# ── API helpers ────────────────────────────────────────────────────────────────

def fetch_all_records(dataset_id: str, fields: list[str] | None = None) -> list[dict]:
    """
    Récupère tous les records d'un dataset via pagination (limit + offset).

    Endpoint : GET {BASE_URL}/{dataset_id}/records
    Paramètres : limit, offset, timezone, select (optionnel).

    Alternative possible avec l'endpoint export :
        GET {BASE_URL}/{dataset_id}/exports/json
    → retourne tout le dataset en une seule requête, sans pagination.
    """
    url = f"{config.BASE_URL}/{dataset_id}/records"
    records: list[dict] = []
    offset = 0

    while True:
        params: dict = {
            "limit": config.API_RECORDS_LIMIT,
            "offset": offset,
            "timezone": config.API_TIMEZONE,
        }
        if fields:
            params["select"] = ",".join(fields)

        try:
            resp = requests.get(url, params=params, timeout=30)
            resp.raise_for_status()
        except requests.RequestException as exc:
            logger.error(f"Erreur API {dataset_id} à l'offset {offset} : {exc}")
            break

        batch: list[dict] = resp.json().get("results", [])
        if not batch:
            break  # plus de données

        records.extend(batch)
        logger.debug(f"{len(records)} records récupérés depuis {dataset_id}...")

        if len(batch) < config.API_RECORDS_LIMIT:
            break  # dernière page

        offset += config.API_RECORDS_LIMIT

    logger.info(f"Total récupéré depuis '{dataset_id}' : {len(records)} records.")
    return records


# ── Publication Kafka ──────────────────────────────────────────────────────────

def publish_records(
    producer: Producer,
    topic: str,
    dataset_id: str,
    records: list[dict],
    request_timestamp: str,
) -> None:
    """
    Envoie chaque record dans Kafka sous forme d'un message JSON enveloppé.

    Structure du message :
    {
        "dataset_id"        : identifiant du dataset source,
        "request_timestamp" : timestamp UTC du début de l'appel API (ISO-8601),
        "record"            : JSON string du record brut de l'API
    }

    Le champ 'record' est sérialisé en JSON string (et non en objet imbriqué)
    pour simplifier le parsing côté PySpark avec from_json().
    """
    for i, record in enumerate(records):
        message = {
            "dataset_id":        dataset_id,
            "request_timestamp": request_timestamp,
            "record":            json.dumps(record, ensure_ascii=False),
        }
        producer.produce(
            topic=topic,
            value=json.dumps(message, ensure_ascii=False).encode("utf-8"),
            callback=delivery_report,
        )
        # Poll régulier pour traiter les callbacks et vider le buffer interne
        if (i + 1) % 100 == 0:
            producer.poll(0)

    producer.flush()
    logger.info(f"Publié {len(records)} records → topic '{topic}'.")


# ── Boucles des deux producers ─────────────────────────────────────────────────

def run_dispo_loop(producer: Producer) -> None:
    """
    Boucle infinie : récupère la disponibilité temps réel et publie dans Kafka.
    Fréquence : DISPO_INTERVAL_SECONDS (défaut 60s).
    """
    logger.info("Thread disponibilité démarré.")
    while True:
        try:
            request_ts = datetime.now(timezone.utc).isoformat()
            logger.info("Récupération des données de disponibilité...")
            records = fetch_all_records(config.DATASET_DISPO, fields=config.FIELDS_DISPO)
            if records:
                publish_records(producer, config.TOPIC_DISPO, config.DATASET_DISPO, records, request_ts)
        except Exception as exc:
            logger.error(f"Erreur dans la boucle disponibilité : {exc}", exc_info=True)

        logger.info(f"Prochaine récupération disponibilité dans {config.DISPO_INTERVAL_SECONDS}s.")
        time.sleep(config.DISPO_INTERVAL_SECONDS)


def run_stations_loop(producer: Producer) -> None:
    """
    Boucle infinie : récupère les emplacements des stations et publie dans Kafka.
    Données quasi-statiques → fréquence STATIONS_INTERVAL_SECONDS (défaut 6h).
    Premier appel au démarrage immédiat.
    """
    logger.info("Thread stations démarré.")
    while True:
        try:
            request_ts = datetime.now(timezone.utc).isoformat()
            logger.info("Récupération des données de stations...")
            records = fetch_all_records(config.DATASET_EMPLACE)
            if records:
                publish_records(producer, config.TOPIC_STATIONS, config.DATASET_EMPLACE, records, request_ts)
        except Exception as exc:
            logger.error(f"Erreur dans la boucle stations : {exc}", exc_info=True)

        logger.info(f"Prochaine récupération stations dans {config.STATIONS_INTERVAL_SECONDS}s.")
        time.sleep(config.STATIONS_INTERVAL_SECONDS)


# ── Point d'entrée ─────────────────────────────────────────────────────────────

if __name__ == "__main__":
    # 1. Attendre que Kafka soit opérationnel (complète le healthcheck Docker)
    wait_for_kafka()

    # 2. Créer les topics explicitement (auto-create Kafka est aussi activé)
    ensure_topics([config.TOPIC_DISPO, config.TOPIC_STATIONS])

    # 3. Créer le producer Kafka partagé entre les deux threads
    producer = make_producer()

    # 4. Lancer les deux boucles dans des threads daemon
    threads = [
        threading.Thread(
            target=run_dispo_loop,
            args=(producer,),
            daemon=True,
            name="dispo-producer",
        ),
        threading.Thread(
            target=run_stations_loop,
            args=(producer,),
            daemon=True,
            name="stations-producer",
        ),
    ]
    for t in threads:
        t.start()

    logger.info("Les deux threads producer sont démarrés. En attente...")

    # 5. Maintenir le thread principal vivant
    try:
        while True:
            time.sleep(60)
    except KeyboardInterrupt:
        logger.info("Arrêt du producer.")
