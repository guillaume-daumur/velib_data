# Matrice de couverture des compétences

Ce document relie chaque compétence évaluée aux éléments concrets du projet.

> **Niveaux de couverture :**
> - ✅ **Couvert** — démontrable directement dans le projet
> - ⚠️ **Couvert partiellement** — fonctionnel en POC, limité par l'architecture mono-nœud
> - 🔵 **Couvert dans le cluster** — nécessite `docker-compose.cluster.yml`
> - 🔲 **Non couvert** — hors périmètre de ce projet (Kubernetes, monitoring avancé, etc.)

---

## Tableau des compétences

| # | Compétence évaluée | Élément du projet | Niveau | Limites | Amélioration possible |
|---|---|---|---|---|---|
| 1 | **Déployer une plateforme de stockage / traitement Big Data** | `docker-compose.yml` : Kafka + PySpark Structured Streaming + BigQuery | ✅ Couvert | Déployé localement sur Docker, pas sur cloud managé | Déployer sur GKE / Dataproc / Cloud Run |
| 2 | **Configurer des clusters de nœuds** | `docker-compose.cluster.yml` : 3 brokers Kafka (KRaft) + 1 master + 2 workers Spark | 🔵 Couvert dans le cluster | Cluster local Docker, pas un vrai cluster distribué sur plusieurs hosts | Kubernetes avec Node Pools, ou Confluent Cloud + Dataproc |
| 3 | **Justifier le nombre de nœuds** | `docs/architecture_poc_vs_cluster.md` : tableau POC vs cluster, règle du quorum Kafka, parallélisme Spark | ✅ Couvert | Documentation seule, pas de benchmark chiffré | Ajouter des métriques de débit pour valider empiriquement |
| 4 | **Assurer la réplication** | `docker-compose.cluster.yml` : `KAFKA_DEFAULT_REPLICATION_FACTOR=3`, `MIN_INSYNC_REPLICAS=2`, volumes séparés par broker | 🔵 Couvert dans le cluster | POC : replication factor = 1 (pas de vraie réplication) | Activer la réplication dans le POC si ≥ 3 brokers disponibles |
| 5 | **Garantir la disponibilité** | `restart: unless-stopped` + healthchecks + `depends_on: condition: service_healthy` | ⚠️ Couvert partiellement | Single point of failure sur le broker Kafka unique (POC) | 3 brokers Kafka + HA Spark Master (ZooKeeper) |
| 6 | **Montée en charge** | `docker-compose.cluster.yml` : ajout de workers Spark, partitions Kafka configurables | 🔵 Couvert dans le cluster | Scaling manuel uniquement (pas d'autoscaling) | Kubernetes HPA + KEDA pour autoscaling basé sur le lag Kafka |
| 7 | **Répartition de charge** | Kafka : distribution des messages sur les partitions (round-robin) ; Spark : une tâche par partition | 🔵 Couvert dans le cluster | En POC, tout tourne sur un seul executor Spark | Configurer explicitement le nombre de partitions Kafka = nombre de workers |
| 8 | **Tolérance aux pannes** | Checkpoints Spark (`/app/data/checkpoints`) + volumes persistants + `restart: unless-stopped` | ⚠️ Couvert partiellement | Pas de tolérance à la panne du broker Kafka unique | Cluster Kafka 3 brokers + checkpoints sur GCS |
| 9 | **Tests de panne** | `scripts/test_restart_pyspark.sh`, `test_restart_producers.sh`, `test_restart_kafka.sh` | ✅ Couvert | Tests manuels, pas de tests automatisés ni de chaos engineering | Chaos Monkey, Gremlin, ou scripts de test automatisés en CI |
| 10 | **Réexécution des tâches** | Checkpoints Spark : reprise exactement au dernier offset Kafka traité ; rétention Kafka 7j pour replay | ✅ Couvert | Reprise possible seulement dans la fenêtre de rétention Kafka | Augmenter `KAFKA_LOG_RETENTION_HOURS`, archiver dans GCS pour replay illimité |
| 11 | **Plan de récupération des données** | `docs/fault_tolerance_and_recovery.md` : 5 scénarios de panne avec commandes de diagnostic et recovery | ✅ Couvert | Plan manuel, pas d'automatisation de la récupération | Runbooks automatisés, alerting (PagerDuty), SLA définis |
| 12 | **Automatiser le déploiement** | `docker compose up --build` : déploiement complet en 1 commande | ✅ Couvert | Déploiement local uniquement, pas de CI/CD | GitHub Actions + Cloud Build pour déploiement automatique |
| 13 | **Documenter l'architecture** | `README.md`, `docs/architecture_poc_vs_cluster.md`, `docs/fault_tolerance_and_recovery.md`, commentaires dans le code | ✅ Couvert | — | Schéma C4 / Lucidchart pour visualisation avancée |
| 14 | **Superviser les services** | `docker compose ps`, `docker compose logs -f`, healthchecks Docker | ⚠️ Couvert partiellement | Supervision basique, pas de métriques ni d'alerting | Prometheus + Grafana (Kafka JMX exporter, Spark metrics) |

---

## Détail par compétence clé

### Configurer des clusters de nœuds (compétence 2)

**Élément du projet :** `docker-compose.cluster.yml`

```yaml
# 3 brokers Kafka en KRaft
kafka-1, kafka-2, kafka-3
  → KAFKA_CONTROLLER_QUORUM_VOTERS: "1@kafka-1:9093,2@kafka-2:9093,3@kafka-3:9093"
  → KAFKA_DEFAULT_REPLICATION_FACTOR: "3"

# Cluster Spark standalone
spark-master  → coordonne le cluster
spark-worker-1, spark-worker-2  → exécutent les tâches
```

**Pour le démontrer :**
```bash
docker compose -f docker-compose.cluster.yml up --build
# Vérifier les brokers :
docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka-1:9092,kafka-2:9092,kafka-3:9092 \
  --describe --topic velib_disponibilite
# → affiche les 3 réplicas et le leader de chaque partition
```

---

### Justifier le nombre de nœuds (compétence 3)

**Règle du quorum Kafka KRaft :** `N = 2f + 1` nœuds pour tolérer `f` pannes.

| Nœuds | Pannes tolérées |
|-------|-----------------|
| 1     | 0               |
| **3** | **1** ← choix du projet |
| 5     | 2               |

**Règle Spark workers :** 1 worker par partition Kafka consommée simultanément (idéalement).

```
2 topics × ~1-2 partitions chacun = 2 workers minimum pour le parallélisme optimal.
```

---

### Tolérance aux pannes (compétence 8)

**Mécanismes actifs dans le POC :**

```
1. restart: unless-stopped       → Docker redémarre le container automatiquement
2. healthcheck + depends_on      → démarrage ordonné, évite les démarrages prématurés
3. Checkpoints Spark             → reprise exacte au dernier offset Kafka traité
4. Volume kafka-data             → messages Kafka survivent aux redémarrages Docker
5. Volume ./data/checkpoints     → checkpoints Spark survivent aux redémarrages Docker
```

**Ce qui nécessite le cluster :**
```
6. Réplication Kafka (factor 3)  → panne d'1 broker sur 3 transparente
7. Workers Spark multiples       → panne d'1 worker → tâches réassignées
```

---

### Tests de panne (compétence 9)

Les 3 scripts dans `scripts/` testent chaque point de défaillance :

```bash
bash scripts/test_restart_kafka.sh      # → POC : pipeline interrompu (SPOF)
bash scripts/test_restart_pyspark.sh    # → reprise depuis les checkpoints
bash scripts/test_restart_producers.sh  # → reprise automatique des appels API
```

Ces scripts sont commentés pour expliquer ce que l'on observe et pourquoi.

---

### Réexécution des tâches (compétence 9-bis)

**Scénario 1 — PySpark redémarre normalement :**
Spark reprend depuis le dernier offset Kafka sauvegardé dans le checkpoint.
Les messages qui ont transité dans Kafka pendant l'arrêt sont traités rétroactivement
(tant qu'ils sont dans la rétention Kafka).

**Scénario 2 — Checkpoint corrompu :**
```bash
rm -rf ./data/checkpoints/velib_disponibilite
# → Spark repart de "latest" (messages de l'interruption perdus)
# ou : modifier startingOffsets: "earliest" pour rejouer depuis le début de la rétention
```

**Scénario 3 — Replay complet depuis Kafka :**
Kafka conserve les messages 7 jours par défaut.
Pour rejouer :
```python
.option("startingOffsets", "earliest")   # dans spark_streaming_job.py
```

---

## Compétences non couvertes (hors périmètre)

| Compétence             | Pourquoi hors périmètre                   | Outil approprié                  |
|------------------------|-------------------------------------------|----------------------------------|
| Autoscaling            | Nécessite Kubernetes + KEDA               | GKE + KEDA + Kafka consumer lag  |
| Monitoring avancé      | Hors périmètre du projet pédagogique      | Prometheus + Grafana             |
| CI/CD                  | Hors périmètre du projet pédagogique      | GitHub Actions + Cloud Build     |
| Sécurité Kafka (TLS)   | Complexifie inutilement le POC            | Kafka avec TLS + SASL            |
| IAM et secrets         | Simplifié ici (fichier JSON monté)        | GCP Secret Manager + Workload Identity |
| SLA / SLO              | Pas de définition formelle                | Définir avec les parties prenantes |
