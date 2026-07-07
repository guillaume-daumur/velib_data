# Vélib' Data Engineering — Pipeline Kafka + PySpark

Pipeline de streaming temps réel pour les données Vélib' Paris.

```
API Open Data Paris
       │
       ├─ disponibilité (toutes les 60s)  ──► topic velib_disponibilite ──►┐
       │                                                                    ├─► PySpark Structured Streaming ──► BigQuery (raw)
       └─ stations (toutes les 6h)        ──► topic velib_stations      ──►┘
```

---

## Architecture — 3 containers (POC)

| Container   | Rôle                                                              |
|-------------|-------------------------------------------------------------------|
| `kafka`     | Broker Kafka (mode KRaft, sans Zookeeper)                        |
| `producers` | Appelle l'API Vélib', envoie les records JSON dans Kafka          |
| `pyspark`   | Lit Kafka en streaming, parse les messages, écrit dans BigQuery   |

Pour la version distribuée multi-nœuds, voir [Lancer la version cluster](#lancer-la-version-cluster-optionnelle).

---

## Prérequis

- [Docker Desktop](https://www.docker.com/products/docker-desktop/) installé et démarré
- Accès internet (API Vélib' + téléchargement JARs Spark au premier démarrage)

---

## Démarrage rapide

```bash
# Construire les images et démarrer tous les containers
docker compose up --build

# Ou en arrière-plan
docker compose up --build -d
```

**Ordre de démarrage garanti :**
1. `kafka` démarre et passe le healthcheck
2. `producers` et `pyspark` démarrent seulement quand Kafka est `healthy`

---

## Arrêt

```bash
# Arrêter les containers (conserve les données et les volumes)
docker compose down

# Arrêter ET supprimer les volumes (efface les messages Kafka et les checkpoints)
docker compose down -v
```

---

## Vérification des logs

```bash
# Logs du producer
docker compose logs -f producers

# Logs du consumer PySpark
docker compose logs -f pyspark

# Logs de Kafka
docker compose logs -f kafka

# Tous les logs en même temps
docker compose logs -f
```

---

## Structure des données produites

```
data/
├── raw/
│   ├── velib_disponibilite/   ← JSON (1 micro-batch = 1 fichier, toutes les 30s)
│   └── velib_stations/
└── checkpoints/
    ├── velib_disponibilite/   ← état du streaming Spark (reprise après crash)
    └── velib_stations/
```

---

## Variables d'environnement disponibles

| Variable                        | Défaut                  | Description                              |
|---------------------------------|-------------------------|------------------------------------------|
| `KAFKA_BOOTSTRAP_SERVERS`       | `kafka:9092`            | Adresse du broker Kafka                  |
| `DISPO_INTERVAL_SECONDS`        | `60`                    | Fréquence de polling disponibilité (s)   |
| `STATIONS_INTERVAL_SECONDS`     | `21600`                 | Fréquence de polling stations (6h en s)  |
| `SPARK_CHECKPOINT_DIR`          | `/app/data/checkpoints` | Répertoire des checkpoints Spark         |
| `RAW_OUTPUT_DIR`                | `/app/data/raw`         | Répertoire de sortie JSON local          |
| `SPARK_MASTER_URL`              | `local[2]`              | Master Spark (`spark://...` en cluster)  |
| `GCP_PROJECT_ID`                | `MY_PROJECT_ID`         | Projet GCP pour BigQuery                 |
| `GOOGLE_APPLICATION_CREDENTIALS`| `/secrets/sa-key.json`  | Clé de service account GCP              |

---

## Commandes utiles

```bash
# Voir les topics Kafka créés
docker exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --list

# Lire les derniers messages d'un topic
docker exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic velib_disponibilite \
  --from-beginning --max-messages 5

# Rebuilder un seul service
docker compose build pyspark && docker compose up -d pyspark
```

---

## Justification architecture POC vs production

### Ce que démontre le POC à 3 containers

| Compétence démontrée            | Comment                                                      |
|---------------------------------|--------------------------------------------------------------|
| Architecture découplée          | 3 services indépendants qui communiquent via Kafka           |
| Streaming temps réel            | PySpark Structured Streaming lit Kafka en continu            |
| Tolérance aux pannes basique    | Checkpoints Spark + `restart: unless-stopped`                |
| Persistance des données         | Volumes Docker pour Kafka et checkpoints                     |
| Configuration externalisée      | Variables d'environnement dans `docker-compose.yml`          |
| Déploiement reproductible       | `docker compose up --build` en 1 commande                    |

### Pourquoi ce n'est pas un vrai cluster

| Limite                          | Explication                                                  |
|---------------------------------|--------------------------------------------------------------|
| 1 seul broker Kafka             | Point unique de panne : si Kafka tombe, tout s'arrête        |
| Réplication factor = 1          | Pas de copie des données sur d'autres brokers                |
| Spark en `local[2]`             | Pas de vrai parallélisme distribué — 2 threads dans 1 process|
| Pas de répartition de charge    | Tous les traitements sur la même machine                     |
| Pas d'autoscaling               | Capacité fixe par les ressources du container                |

### Ce qu'apporte la version cluster (`docker-compose.cluster.yml`)

- **3 brokers Kafka** avec réplication factor 3 → tolérance à la panne d'1 broker
- **1 Spark Master + 2 Workers** → traitement distribué et parallèle
- **Volumes séparés par broker** → isolation des données
- **Bootstrap servers multiples** → les producers et PySpark basculent automatiquement sur un broker disponible

Documentation complète : [docs/architecture_poc_vs_cluster.md](docs/architecture_poc_vs_cluster.md)

---

## Tolérance aux pannes et reprise après crash

### Mécanismes actifs dans le POC

| Mécanisme                       | Effet                                                        |
|---------------------------------|--------------------------------------------------------------|
| `restart: unless-stopped`       | Docker redémarre automatiquement les containers crashés      |
| Healthchecks + `depends_on`     | Démarrage ordonné, évite les connexions prématurées          |
| Checkpoints Spark               | PySpark reprend exactement au dernier offset Kafka traité    |
| Volume `kafka-data`             | Les messages Kafka survivent aux `docker compose restart`    |
| Volume `./data/checkpoints`     | Les checkpoints survivent aux recreations de container       |

### Commandes de diagnostic

```bash
# État de tous les services
docker compose ps

# Logs Kafka (problèmes de broker)
docker compose logs -f kafka

# Logs producers (erreurs d'API ou de connexion Kafka)
docker compose logs -f producers

# Logs PySpark (erreurs de parsing ou d'écriture)
docker compose logs -f pyspark
```

### Scripts de test de panne

```bash
# Test 1 : crash PySpark → vérifier reprise depuis les checkpoints
bash scripts/test_restart_pyspark.sh

# Test 2 : crash producers → vérifier reprise des appels API
bash scripts/test_restart_producers.sh

# Test 3 : crash Kafka → observer le SPOF et la reprise (POC)
bash scripts/test_restart_kafka.sh
```

Documentation complète : [docs/fault_tolerance_and_recovery.md](docs/fault_tolerance_and_recovery.md)

---

## Lancer la version cluster optionnelle

La version cluster démontre une architecture distribuée avec 3 brokers Kafka et un cluster Spark.

```bash
# Démarrer le cluster (7 services)
docker compose -f docker-compose.cluster.yml up --build

# Arrêter le cluster
docker compose -f docker-compose.cluster.yml down -v

# Logs d'un service spécifique
docker compose -f docker-compose.cluster.yml logs -f kafka-1
docker compose -f docker-compose.cluster.yml logs -f spark-master
```

**Services du cluster :**

| Service         | Rôle                                          |
|-----------------|-----------------------------------------------|
| `kafka-1/2/3`   | 3 brokers Kafka, réplication factor 3         |
| `spark-master`  | Coordinateur du cluster Spark (UI : :8080)    |
| `spark-worker-1/2` | Executeurs Spark (2 workers, 2 CPU, 2 Go) |
| `producers`     | Producers Python → bootstrap 3 brokers        |
| `spark-job`     | Job de streaming → `spark://spark-master:7077`|

---

## BigQuery — connexion

Voir le guide complet dans [bigquery/schema.sql](bigquery/schema.sql).

Résumé :
1. Créer le dataset `raw` dans la console BigQuery
2. Coller et exécuter le DDL de `bigquery/schema.sql` (remplacer `MY_PROJECT_ID`)
3. Créer un service account avec les rôles **BigQuery Data Editor** + **BigQuery Job User**
4. Télécharger la clé JSON → `secrets/sa-key.json`
5. Mettre à jour `GCP_PROJECT_ID` dans `docker-compose.yml`
6. Relancer : `docker compose up --build`

---

## Documentation

| Document                                                              | Contenu                                      |
|-----------------------------------------------------------------------|----------------------------------------------|
| [docs/architecture_poc_vs_cluster.md](docs/architecture_poc_vs_cluster.md) | Justification des nœuds, montée en charge     |
| [docs/fault_tolerance_and_recovery.md](docs/fault_tolerance_and_recovery.md) | Tolérance aux pannes, plan de récupération   |
| [docs/competency_mapping.md](docs/competency_mapping.md)             | Matrice de couverture des compétences        |
| [bigquery/schema.sql](bigquery/schema.sql)                           | DDL BigQuery pour les 2 tables raw           |

---

## Données source

- **Disponibilité** : [velib-disponibilite-en-temps-reel](https://opendata.paris.fr/explore/dataset/velib-disponibilite-en-temps-reel) — actualisé ~chaque minute, aucune clé API.
- **Stations** : [velib-emplacement-des-stations](https://opendata.paris.fr/explore/dataset/velib-emplacement-des-stations) — données quasi-statiques.
