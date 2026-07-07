# Architecture POC vs Cluster — Justification des nœuds

## Schéma global

```
POC (docker-compose.yml)               Cluster (docker-compose.cluster.yml)
─────────────────────────────          ──────────────────────────────────────────
API Vélib'                             API Vélib'
   │                                      │
producers (1 container)               producers (1 container)
   │                                      │
kafka (1 broker)                      kafka-1 ─┐
   │                                  kafka-2 ─┼─ 3 brokers, réplication factor 3
pyspark (local[2])                    kafka-3 ─┘
   │                                      │
data/raw/  (local)                    spark-master
                                      spark-worker-1 ─┐ 2 workers
                                      spark-worker-2 ─┘
                                          │
                                      BigQuery / data/raw/
```

---

## Architecture POC — 3 containers

### Pourquoi ce choix

Le POC (Proof of Concept) est conçu pour **démontrer le flux de données de bout en bout** :

```
API Vélib' → producers Python → Kafka → PySpark Structured Streaming → data/raw/
```

Avec 3 containers seulement, on peut :
- lancer le pipeline en une commande (`docker compose up --build`),
- observer les données entrer dans Kafka et en sortir après transformation,
- vérifier que le parsing JSON fonctionne,
- tester la persistance des checkpoints,
- prototyper la logique métier sans overhead d'infrastructure.

### Justification des 3 nœuds

| Container   | Rôle unique                          | Pourquoi isolé                                         |
|-------------|--------------------------------------|--------------------------------------------------------|
| `kafka`     | Broker de messagerie                 | Infrastructure réseau partagée entre producers et consumer |
| `producers` | Collecte API + publication Kafka     | Cycle de vie indépendant du traitement, peut être scaled séparément |
| `pyspark`   | Traitement streaming + écriture sink | Isolé pour permettre de swapper le sink (local → BigQuery) sans toucher aux autres |

**Pourquoi KRaft (sans Zookeeper) ?**
Kafka 3.x supporte le mode KRaft natif. Cela supprime le besoin d'un 4ème container Zookeeper, réduisant la complexité du POC de 25 %.

### Ce que le POC démontre

- Le flux de données complet de l'API à la donnée persistée.
- La séparation producteur / broker / consommateur (architecture découplée).
- Les checkpoints Spark pour la reprise après crash (tolérance aux pannes basique).
- La configuration via variables d'environnement (portabilité).
- L'isolation des services via Docker network.

### Limites du POC

| Limite                            | Explication                                                                 |
|-----------------------------------|-----------------------------------------------------------------------------|
| Pas de vraie réplication Kafka    | 1 seul broker → `replication_factor=1` → si le broker tombe, les données non encore consommées sont perdues |
| Pas de haute disponibilité Kafka  | Un seul point de panne pour tout le pipeline                                |
| Spark en mode `local[2]`          | Spark tourne dans un seul process Python, 2 threads — pas de vrai cluster Spark |
| Pas de répartition de charge Spark| Toutes les partitions Kafka sont traitées par un seul executor               |
| Pas d'autoscaling                 | La capacité est fixée par les ressources du container                        |
| Producers non redondés            | Si le container `producers` plante, plus d'ingestion tant qu'il ne redémarre pas |

---

## Architecture Cluster — 7 services

### Justification des nœuds

#### Pourquoi 3 brokers Kafka ?

**Règle du quorum Kafka KRaft :** pour tolérer `f` pannes, il faut `2f + 1` nœuds.

| Nombre de brokers | Pannes tolérées | Réplication factor max utile |
|-------------------|-----------------|-------------------------------|
| 1 (POC)           | 0               | 1                             |
| 3 (cluster)       | **1**           | 3                             |
| 5                 | 2               | 5                             |

Avec 3 brokers et `replication.factor=3` :
- chaque partition du topic est copiée sur les 3 brokers,
- si **kafka-2** tombe, les données sont toujours disponibles sur kafka-1 et kafka-3,
- Kafka élit automatiquement un nouveau leader de partition,
- le pipeline **n'est pas interrompu**.

```
Topic velib_disponibilite (3 partitions, replication factor 3) :
  Partition 0 : Leader=kafka-1, Réplicas=[kafka-1, kafka-2, kafka-3]
  Partition 1 : Leader=kafka-2, Réplicas=[kafka-1, kafka-2, kafka-3]
  Partition 2 : Leader=kafka-3, Réplicas=[kafka-1, kafka-2, kafka-3]
```

Les producers envoient vers `kafka-1:9092,kafka-2:9092,kafka-3:9092` (bootstrap servers).
Kafka distribue automatiquement les messages selon les leaders de partitions.

#### Pourquoi 1 Spark Master ?

Le Master Spark standalone est le coordinateur du cluster :
- il reçoit les demandes de soumission de jobs (`spark-submit`),
- il alloue des ressources (CPU, RAM) aux workers,
- il monitore l'état des workers (health, tasks en cours),
- il expose l'UI de supervision sur le port 8080.

En production, le master peut être rendu haute disponibilité via ZooKeeper ou en mode KRaft Spark. Pour ce projet, un seul master est suffisant.

#### Pourquoi 2 Spark Workers ?

Les workers exécutent les calculs distribués assignés par le master :
- chaque worker traite un sous-ensemble des partitions Kafka,
- avec 2 workers, le job de streaming est parallélisé sur 2 machines,
- ajouter un 3ème worker double quasiment le débit de traitement.

**Relation partitions Kafka → parallelisme Spark :**

```
Topic velib_disponibilite (3 partitions)
  ↓
Spark lit en streaming → 3 tâches parallèles
  ↓
spark-worker-1 traite partitions 0, 2
spark-worker-2 traite partition 1
```

#### Pourquoi les producers restent sur 1 container en POC ?

Les producers sont **stateless** : ils appellent une API REST et publient dans Kafka.
En cas de crash, `restart: unless-stopped` les relance automatiquement.

En production, on pourrait :
- dockeriser plusieurs replicas de producers (Kubernetes Deployment),
- répartir la charge de polling sur plusieurs instances,
- ou utiliser un scheduler (Airflow) pour orchestrer les appels API.

### Montée en charge — comment scaler ?

| Composant          | Action                                      | Effet                                          |
|--------------------|---------------------------------------------|------------------------------------------------|
| Kafka (débit)      | Augmenter le nombre de **partitions**       | Plus de parallélisme pour les consommateurs    |
| Kafka (fiabilité)  | Ajouter des **brokers** (kafka-4, kafka-5)  | Plus de réplication, meilleure disponibilité   |
| Spark (calcul)     | Ajouter des **workers** (spark-worker-3...) | Plus d'executors, traitement plus rapide       |
| Spark (mémoire)    | Augmenter `SPARK_WORKER_MEMORY`             | Traitement de micro-batchs plus larges         |
| Producers (débit)  | Déployer plusieurs replicas                 | Ingestion parallèle depuis l'API               |
| Rétention Kafka    | Configurer `KAFKA_LOG_RETENTION_HOURS`      | Conserver les messages plus longtemps (replay) |

### Répartition de charge

```
Producers → Kafka (3 brokers, round-robin par partition)
         → Spark (1 task par partition Kafka, distribué sur les workers)
         → BigQuery (écriture directe, parallélisée par executor)
```

Kafka assure la répartition de la **production** (les messages sont distribués sur les partitions selon la clé ou round-robin).

Spark assure la répartition du **traitement** (chaque partition est traitée par un executor différent).
