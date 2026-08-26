# monitoring — Santé du pipeline (Kafka, KRaft, PySpark, producers)

Script Python (`monitor.py`) qui tourne en boucle à côté du pipeline et envoie,
à chaque cycle, une ligne par composant vérifié vers BigQuery
(`monitoring.pipeline_health`) via streaming insert.

## Composants vérifiés

| component                          | Ce qui est mesuré                                                                 |
|-------------------------------------|-------------------------------------------------------------------------------------|
| `kafka`                             | Joignabilité du broker (AdminClient), latence, nb de brokers/topics                |
| `kraft_controller`                  | Dérivé du check Kafka (`controller_id` des métadonnées — mode KRaft combiné)       |
| `pyspark_velib_disponibilite`       | Fraîcheur du dernier checkpoint committé + retard en messages vs Kafka             |
| `producer_velib_disponibilite`      | Âge du dernier message publié sur le topic (proxy de santé du producer)            |

Le détail des seuils (`status` healthy/degraded/down) est dans
[`health_checks.py`](health_checks.py).

## Pourquoi un envoi direct vers BigQuery (pas via Kafka → Spark → GCS) ?

Le monitoring doit rester utilisable **même quand Kafka ou Spark sont en panne** —
c'est justement ce qu'il doit détecter et remonter. Le faire transiter par le
pipeline qu'il surveille créerait une dépendance circulaire (si Kafka tombe, plus
aucune donnée de santé ne remonterait). Le script écrit donc directement dans
BigQuery avec `insert_rows_json`, indépendamment de l'état du reste du pipeline.

## Procédure pas à pas — création de la table BigQuery

### 1. Créer le dataset `monitoring`

Sur [console.cloud.google.com/bigquery](https://console.cloud.google.com/bigquery) :

- Panneau gauche → clic sur ton projet → **Créer un ensemble de données**
- ID du dataset : `monitoring`
- Région : `europe-west1` (idéalement la même que les autres datasets/bucket)
- Laisser les autres options par défaut → **Créer**

### 2. Créer la table `pipeline_health`

Ouvrir [`bigquery/schema_monitoring.sql`](../bigquery/schema_monitoring.sql),
remplacer `MY_PROJECT_ID` par ton vrai projet GCP, puis coller/exécuter tout le
`CREATE TABLE` dans l'éditeur de requêtes BigQuery.

Vérifier que la table existe :

```sql
SELECT table_name, ddl
FROM `MY_PROJECT_ID.monitoring.INFORMATION_SCHEMA.TABLES`
WHERE table_name = 'pipeline_health';
```

### 3. Donner au service account les droits d'écriture

Le script réutilise le même service account que le job PySpark
(`secrets/sa-key.json`, déjà monté dans le container `pyspark`). Vérifier/ajouter
les rôles IAM sur ce compte, au niveau projet (**IAM et administration → IAM →
Accorder l'accès**) :

- **BigQuery Data Editor** — nécessaire pour `insert_rows_json`
- **BigQuery Job User** — nécessaire pour toute requête (facultatif si tu ne fais
  qu'insérer, mais utile si tu interroges la table depuis un autre outil avec ce SA)

Si tu préfères un compte dédié (principe du moindre privilège), crée un service
account `velib-monitor` avec uniquement ces deux rôles, génère une clé JSON et
place-la dans `secrets/monitor-sa-key.json`, puis adapte
`GOOGLE_APPLICATION_CREDENTIALS` dans `docker-compose.yml` pour le service
`monitoring`.

### 4. Lancer le service

Le service `monitoring` est déjà déclaré dans `docker-compose.yml` (et
`docker-compose.cluster.yml`). Il démarre avec le reste du pipeline :

```bash
docker compose up --build
```

Ou, indépendamment, en local (hors Docker) :

```bash
cd monitoring
pip install -r requirements.txt
export KAFKA_BOOTSTRAP_SERVERS=localhost:9092
export GCP_PROJECT_ID=MY_PROJECT_ID
export GOOGLE_APPLICATION_CREDENTIALS=../secrets/sa-key.json
export SPARK_CHECKPOINT_DIR=../data/checkpoints
python monitor.py
```

### 5. Vérifier

```sql
-- Dernier état de chaque composant
SELECT component, status, lag_seconds, lag_messages, error_message, check_timestamp
FROM `MY_PROJECT_ID.monitoring.pipeline_health`
QUALIFY ROW_NUMBER() OVER (PARTITION BY component ORDER BY check_timestamp DESC) = 1
ORDER BY component;

-- Incidents des dernières 24h
SELECT check_timestamp, component, status, error_message
FROM `MY_PROJECT_ID.monitoring.pipeline_health`
WHERE status != 'healthy'
  AND check_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 DAY)
ORDER BY check_timestamp DESC;
```

### 6. (Optionnel) Brancher un dashboard

Looker Studio (gratuit) se connecte directement à
`MY_PROJECT_ID.monitoring.pipeline_health` comme source BigQuery — pratique pour
visualiser le statut par composant et le `lag_seconds` dans le temps sans écrire
de code.

## Mode nuit (réduction d'empreinte carbone)

Le script réduit lui-même sa propre fréquence de vérification/envoi pendant le
mode nuit (voir section dédiée dans le README racine / `producers/config.py`),
et élargit la tolérance de fraîcheur des checks producers en conséquence — pour
ne pas remonter de fausses alertes "down" pendant le ralentissement volontaire.
