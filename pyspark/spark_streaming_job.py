"""
Vélib' PySpark Structured Streaming Consumer — sortie BigQuery

Lit deux topics Kafka, parse les messages JSON, et écrit dans BigQuery :
  - velib_disponibilite → GCP_PROJECT_ID.raw.velib_disponibilite
  - velib_stations      → GCP_PROJECT_ID.raw.velib_stations

Variables d'environnement :
  KAFKA_BOOTSTRAP_SERVERS       : adresse du broker (défaut : kafka:9092)
  SPARK_CHECKPOINT_DIR          : répertoire des checkpoints (défaut : /app/data/checkpoints)
  SPARK_MASTER_URL              : master Spark (défaut : local[2], utiliser spark://spark-master:7077 en cluster)
  GCP_PROJECT_ID                : projet GCP
  GCP_BQ_DATASET                : dataset BigQuery cible (défaut : raw)
  GOOGLE_APPLICATION_CREDENTIALS: chemin vers la clé de service account
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp
from pyspark.sql.types import (
    DoubleType, IntegerType, StringType,
    StructField, StructType,
)

# ── Configuration ──────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
GCP_PROJECT_ID          = os.getenv("GCP_PROJECT_ID", "MY_PROJECT_ID")
BQ_DATASET              = os.getenv("GCP_BQ_DATASET", "raw")

# SPARK_MASTER_URL permet de basculer entre le mode local (POC) et le mode cluster.
# - "local[2]"                  : POC, Spark tourne dans le même process Python, 2 threads
# - "spark://spark-master:7077" : cluster Spark standalone (docker-compose.cluster.yml)
SPARK_MASTER_URL = os.getenv("SPARK_MASTER_URL", "local[2]")

# SPARK_CHECKPOINT_DIR : répertoire de checkpoint injecté via variable d'environnement.
# Cela permet de configurer le chemin sans modifier le code ni rebuilder l'image.
CHECKPOINT_BASE = os.getenv("SPARK_CHECKPOINT_DIR", "/app/data/checkpoints")
OUTPUT_BASE     = os.getenv("RAW_OUTPUT_DIR", "/app/data/raw")
BQ_ENABLED      = os.getenv("BQ_ENABLED", "false").lower() == "true"

TOPIC_DISPO    = "velib_disponibilite"
TOPIC_STATIONS = "velib_stations"

# Uniquement le connecteur Kafka — BigQuery est géré via le client Python (google-cloud-bigquery),
# sans JAR Spark, ce qui évite toute incompatibilité de version.
SPARK_PACKAGES = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1"

# ── Schémas ────────────────────────────────────────────────────────────────────
ENVELOPE_SCHEMA = StructType([
    StructField("dataset_id",        StringType(), True),
    StructField("request_timestamp", StringType(), True),
    StructField("record",            StringType(), True),
])

DISPO_RECORD_SCHEMA = StructType([
    StructField("stationcode",                  StringType(),  True),
    StructField("name",                         StringType(),  True),
    StructField("is_installed",                 StringType(),  True),
    StructField("capacity",                     IntegerType(), True),
    StructField("numdocksavailable",            IntegerType(), True),
    StructField("numbikesavailable",            IntegerType(), True),
    StructField("mechanical",                   IntegerType(), True),
    StructField("ebike",                        IntegerType(), True),
    StructField("is_renting",                   StringType(),  True),
    StructField("is_returning",                 StringType(),  True),
    StructField("duedate",                      StringType(),  True),
    StructField("coordonnees_geo", StructType([
        StructField("lat", DoubleType(), True),
        StructField("lon", DoubleType(), True),
    ]), True),
    StructField("nom_arrondissement_communes",  StringType(),  True),
    StructField("code_insee_commune",           StringType(),  True),
    StructField("station_opening_hours",        StringType(),  True),
])

STATIONS_RECORD_SCHEMA = StructType([
    StructField("stationcode",                  StringType(),  True),
    StructField("name",                         StringType(),  True),
    StructField("capacity",                     IntegerType(), True),
    StructField("coordonnees_geo", StructType([
        StructField("lat", DoubleType(), True),
        StructField("lon", DoubleType(), True),
    ]), True),
    StructField("nom_arrondissement_communes",  StringType(),  True),
    StructField("code_insee_commune",           StringType(),  True),
    StructField("station_opening_hours",        StringType(),  True),
    StructField("duedate",                      StringType(),  True),
])


# ── SparkSession ───────────────────────────────────────────────────────────────

def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("VelibStructuredStreaming")
        .master(SPARK_MASTER_URL)
        .config("spark.jars.packages", SPARK_PACKAGES)
        # Réduire le nombre de partitions shuffle (adapté à un cluster léger)
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )


# ── Lecture Kafka ──────────────────────────────────────────────────────────────

def read_kafka_stream(spark: SparkSession, topic: str):
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", topic)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )


# ── Parsing ────────────────────────────────────────────────────────────────────

def parse_dispo(raw_df):
    env = (
        raw_df.select(
            from_json(col("value").cast(StringType()), ENVELOPE_SCHEMA).alias("env"),
        )
        .select(
            col("env.dataset_id"),
            to_timestamp(col("env.request_timestamp")).alias("request_timestamp"),
            from_json(col("env.record"), DISPO_RECORD_SCHEMA).alias("r"),
        )
    )
    return env.select(
        "request_timestamp", "dataset_id",
        col("r.stationcode"), col("r.name"), col("r.is_installed"),
        col("r.capacity"), col("r.numdocksavailable"), col("r.numbikesavailable"),
        col("r.mechanical"), col("r.ebike"),
        col("r.is_renting"), col("r.is_returning"), col("r.duedate"),
        col("r.coordonnees_geo"),
        col("r.nom_arrondissement_communes"), col("r.code_insee_commune"),
        col("r.station_opening_hours"),
    )


def parse_stations(raw_df):
    env = (
        raw_df.select(
            from_json(col("value").cast(StringType()), ENVELOPE_SCHEMA).alias("env"),
        )
        .select(
            col("env.dataset_id"),
            to_timestamp(col("env.request_timestamp")).alias("request_timestamp"),
            from_json(col("env.record"), STATIONS_RECORD_SCHEMA).alias("r"),
        )
    )
    return env.select(
        "request_timestamp", "dataset_id",
        col("r.stationcode"), col("r.name"), col("r.capacity"),
        col("r.coordonnees_geo"),
        col("r.nom_arrondissement_communes"), col("r.code_insee_commune"),
        col("r.station_opening_hours"), col("r.duedate"),
    )


# ── Écriture BigQuery ──────────────────────────────────────────────────────────

def _make_bq_writer(table_name: str):
    """
    Retourne une fonction foreachBatch qui écrit vers BigQuery via le client Python.
    Utilise insert_rows_json (streaming insert API) — aucun JAR Spark requis.
    Les timestamps Spark sont convertis en chaînes ISO 8601 avant l'envoi.
    """
    def write_batch(df, epoch_id):
        if df.rdd.isEmpty():
            return
        from datetime import datetime, timezone
        from google.cloud import bigquery

        insert_ts = datetime.now(timezone.utc).isoformat()
        rows = []
        for row in df.collect():
            d = row.asDict(recursive=True)
            if d.get("request_timestamp") is not None and hasattr(d["request_timestamp"], "isoformat"):
                d["request_timestamp"] = d["request_timestamp"].isoformat()
            d["insert_timestamp"] = insert_ts
            rows.append(d)

        if not rows:
            return

        client = bigquery.Client(project=GCP_PROJECT_ID)
        table_ref = f"{GCP_PROJECT_ID}.{BQ_DATASET}.{table_name}"
        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
        job = client.load_table_from_json(rows, table_ref, job_config=job_config)
        job.result()
        print(f"[BQ] {len(rows)} lignes insérées → {table_ref}", flush=True)

    return write_batch


def write_stream(df, table: str, checkpoint_path: str, query_name: str):
    """
    Écrit un DataFrame streaming vers BigQuery (foreachBatch, si BQ_ENABLED=true)
    ou en JSON local (si BQ_ENABLED=false).

    CHECKPOINT : Spark sauvegarde les offsets Kafka traités à chaque micro-batch.
    Si le container redémarre, Spark reprend exactement au dernier offset commité —
    aucune perte ni doublon. Chaque stream a son propre répertoire de checkpoint.
    """
    base = (
        df.writeStream
        .outputMode("append")
        .option("checkpointLocation", checkpoint_path)
        .queryName(query_name)
        .trigger(processingTime="30 seconds")
    )

    if BQ_ENABLED:
        return base.foreachBatch(_make_bq_writer(table)).start()
    else:
        return base.format("json").option("path", f"{OUTPUT_BASE}/{table}").start()


# ── Point d'entrée ─────────────────────────────────────────────────────────────

if __name__ == "__main__":
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    import sys
    mode = "BigQuery" if BQ_ENABLED else f"JSON local ({OUTPUT_BASE})"
    print(f"[VelibStreaming] Output mode : {mode}", flush=True)

    # ── Stream disponibilité ───────────────────────────────────────────────────
    query_dispo = write_stream(
        parse_dispo(read_kafka_stream(spark, TOPIC_DISPO)),
        table="velib_disponibilite",
        checkpoint_path=f"{CHECKPOINT_BASE}/velib_disponibilite",
        query_name="writer_dispo",
    )

    # ── Stream stations ────────────────────────────────────────────────────────
    query_stations = write_stream(
        parse_stations(read_kafka_stream(spark, TOPIC_STATIONS)),
        table="velib_stations",
        checkpoint_path=f"{CHECKPOINT_BASE}/velib_stations",
        query_name="writer_stations",
    )

    # Bloque jusqu'à l'arrêt d'un des deux streams (erreur ou shutdown)
    spark.streams.awaitAnyTermination()
