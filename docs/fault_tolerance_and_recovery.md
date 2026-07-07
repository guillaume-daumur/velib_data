# Tolérance aux pannes et plan de récupération

## Vue d'ensemble

```
Composant    Mécanisme de tolérance         Couvert dans POC ?
──────────   ──────────────────────────     ──────────────────
Docker       restart: unless-stopped        ✅ Oui
Kafka        Volume persistant              ✅ Oui (données conservées)
Kafka        Réplication multi-brokers      ❌ Non (1 seul broker en POC)
PySpark      Checkpoints Structured Stream  ✅ Oui
PySpark      Cluster Spark (failover)       ❌ Non (local[2] en POC)
BigQuery     Réplication GCP native         ✅ (côté GCP, hors pipeline)
```

---

## 1. Tolérance aux pannes dans le POC

### Ce qui est couvert

#### `restart: unless-stopped`
Tous les services ont `restart: unless-stopped` dans `docker-compose.yml`.

- Si un container crash (erreur Python, OOM, etc.), Docker le redémarre automatiquement.
- Si la machine hôte redémarre, Docker redémarre les containers au boot.
- Limite : ne fonctionne que sur la même machine, pas en cas de panne du host.

#### Volume persistant Kafka (`kafka-data`)
```yaml
volumes:
  - kafka-data:/var/lib/kafka/data
```
Les messages Kafka sont écrits sur disque dans le volume nommé `kafka-data`.
- Un `docker compose restart kafka` **conserve tous les messages** non expirés.
- Un `docker compose down` (sans `-v`) **conserve le volume**.
- Un `docker compose down -v` **supprime le volume** (à éviter en production).

La rétention par défaut est de 7 jours (`KAFKA_LOG_RETENTION_HOURS=168`).
Pendant cette période, un consumer peut rejouer les messages depuis le début.

#### Checkpoints PySpark
```
/app/data/checkpoints/velib_disponibilite/
/app/data/checkpoints/velib_stations/
```
Les checkpoints Spark Structured Streaming sauvegardent :
1. **Les offsets Kafka** traités avec succès → pas de double traitement ni de perte.
2. **L'état du stream** (pour les opérations stateful futures comme les fenêtres temporelles).
3. **Les métadonnées du job** → Spark peut reconnecter les sources/sinks au redémarrage.

Ces répertoires sont montés sur le host (`./data/checkpoints`) via un volume bind mount.
Un `docker compose restart pyspark` ou `docker compose down && docker compose up` reprend
exactement au dernier offset traité.

#### Healthchecks
Les healthchecks vérifient que chaque service est fonctionnel avant de déclarer le container "healthy" :
- `producers` et `pyspark` attendent que `kafka` soit `healthy` avant de démarrer.
- Si Kafka prend du temps à démarrer (cas fréquent au premier lancement), les autres services attendent.

### Ce qui n'est pas couvert dans le POC

| Risque                          | Impact                                                    | Solution cluster                         |
|---------------------------------|-----------------------------------------------------------|------------------------------------------|
| Panne du broker Kafka unique    | Pipeline 100% interrompu, messages en vol perdus          | 3 brokers + réplication factor 3         |
| Panne du host Docker            | Tous les containers tombent simultanément                 | Kubernetes / Docker Swarm multi-host     |
| Corruption du volume Kafka      | Perte de tous les messages non encore consommés           | Réplication + backup S3/GCS              |
| Corruption des checkpoints Spark| Obligation de relancer depuis le début ou from latest     | Voir section "Checkpoints corrompus"     |
| Surcharge du container pyspark  | OOM → crash → redémarrage → latence                      | Cluster Spark avec scaling               |

---

## 2. Tolérance aux pannes en architecture cluster

### Kafka — 3 brokers + réplication

```
Topic velib_disponibilite, partition 0 :
  Leader : kafka-1
  ISR    : [kafka-1, kafka-2, kafka-3]   ← In-Sync Replicas

→ kafka-2 tombe
  Leader : kafka-1 (inchangé)
  ISR    : [kafka-1, kafka-3]            ← kafka-2 retiré des ISR
  Écriture possible car min.insync.replicas = 2 (≤ 2 ISR restants)

→ kafka-2 revient
  ISR    : [kafka-1, kafka-2, kafka-3]   ← kafka-2 rattrape son retard (replication)
```

**Paramètres clés :**
- `KAFKA_DEFAULT_REPLICATION_FACTOR=3` → chaque topic répliqué sur 3 brokers
- `KAFKA_MIN_INSYNC_REPLICAS=2` → au moins 2 replicas doivent acquitter l'écriture
- `KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=3` → le topic interne `__consumer_offsets` aussi répliqué

### Spark — master + workers

- Si un **worker tombe**, Spark réassigne ses tâches aux autres workers (failover automatique).
- Si le **master tombe**, les jobs en cours continuent sur les workers jusqu'à ce que le master redémarre, mais aucun nouveau job ne peut être soumis. (Pour HA master : configurer Spark HA avec ZooKeeper.)
- Les **checkpoints** survivent à la panne d'un worker grâce au stockage partagé (volume Docker ou GCS).

---

## 3. Plan de récupération après panne

### Protocole de diagnostic

**Étape 1 — Identifier le service en erreur**
```bash
# État de tous les services
docker compose ps

# Logs en temps réel (remplacer <service> par kafka/producers/pyspark)
docker compose logs -f <service>

# Dernières 50 lignes d'un service
docker compose logs --tail=50 <service>
```

**Étape 2 — Redémarrer le service défaillant**
```bash
# Redémarrage propre d'un service (conserve volumes et state)
docker compose restart <service>

# Si le service refuse de redémarrer, le recréer
docker compose up -d --force-recreate <service>
```

### Scénario A — Panne du container Kafka

```bash
# 1. Observer les erreurs côté producers et pyspark
docker compose logs --tail=20 producers   # → "Kafka not ready" ou erreurs de connexion
docker compose logs --tail=20 pyspark     # → "Connection refused" ou "Broker unavailable"

# 2. Redémarrer Kafka
docker compose restart kafka

# 3. Attendre le healthcheck (15-30s)
docker compose ps   # attendre que kafka passe en "healthy"

# 4. Les producers reprennent automatiquement grâce à wait_for_kafka() + retry
docker compose logs -f producers   # → "Kafka prêt" puis "Published N records"

# 5. PySpark reprend depuis les derniers checkpoints
docker compose logs -f pyspark   # → "Starting query writer_dispo from checkpoint"
```

**Point de vigilance (POC, 1 seul broker) :**
Les messages publiés par les producers PENDANT la panne de Kafka sont perdus,
car il n'y a pas de broker disponible pour les recevoir.
En cluster avec 3 brokers, la panne d'un seul broker est transparente.

### Scénario B — Panne du container PySpark

```bash
# 1. Vérifier l'état
docker compose ps                           # pyspark = "Exit 1" ou "Restarting"
docker compose logs --tail=30 pyspark       # lire l'erreur (OOM ? JAR manquant ?)

# 2. Redémarrer
docker compose restart pyspark

# 3. Vérifier la reprise depuis les checkpoints
docker compose logs -f pyspark
# On doit voir : "Resuming query from checkpoint offset kafka:<offset>"
# Les données publiées dans Kafka pendant la panne sont rejouées automatiquement
# (grâce aux checkpoints + rétention Kafka de 7 jours).
```

### Scénario C — Panne du container Producers

```bash
# 1. Kafka et PySpark continuent de tourner normalement
# 2. Redémarrer le producer
docker compose restart producers
docker compose logs -f producers
# → "Kafka prêt" puis "Fetching disponibilité data..."

# Note : les appels API manqués pendant la panne ne sont pas rejoués.
# La donnée Vélib' est temps réel : les snapshots manqués sont simplement absents
# de la base de données. Il n'y a pas de mécanisme de rattrapage prévu.
```

### Scénario D — Checkpoints Spark corrompus

Symptôme : PySpark démarre mais échoue immédiatement avec une erreur de checkpoint.

```bash
# 1. Sauvegarder les checkpoints corrompus
mv ./data/checkpoints ./data/checkpoints_backup_$(date +%Y%m%d_%H%M%S)

# 2. Recréer le répertoire vide
mkdir -p ./data/checkpoints

# 3. Redémarrer PySpark (il repartira de "latest" sur les topics Kafka)
docker compose restart pyspark
docker compose logs -f pyspark

# 4. Optionnel : rejouer depuis le début si les messages sont dans la rétention Kafka
# → modifier startingOffsets: "earliest" dans spark_streaming_job.py,
#   puis remettre "latest" une fois le rattrapage terminé.
```

**Attention :** repartir de "latest" signifie que les messages publiés entre le dernier
checkpoint valide et maintenant ne seront pas traités. En architecture cluster avec
`startingOffsets: "earliest"` et une rétention suffisante, on peut rejouer toute la
fenêtre de rétention Kafka (7 jours par défaut).

### Scénario E — Vérification post-récupération complète

```bash
# 1. Tous les services sont sains ?
docker compose ps

# 2. Les topics Kafka existent et reçoivent des messages ?
docker exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --describe --topic velib_disponibilite

# 3. Des messages arrivent dans le topic ?
docker exec kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 --describe --all-groups

# 4. Les fichiers data/raw/ sont bien écrits ?
ls -lh data/raw/velib_disponibilite/
ls -lh data/raw/velib_stations/

# 5. Les checkpoints sont bien à jour ?
ls -lh data/checkpoints/velib_disponibilite/
```
