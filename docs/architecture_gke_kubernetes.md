# Architecture cible — Kubernetes (GKE)

> **Statut : documentée, non déployée.** Cette architecture décrit la cible
> haute disponibilité du pipeline si son cycle de vie devait se prolonger
> au-delà de la phase actuelle. Elle n'a pas été mise en œuvre sur le projet
> réel : au moment de la rédaction, le pipeline n'avait plus qu'une semaine
> d'exploitation prévue, ce qui ne justifiait ni le coût ni le temps
> d'ingénierie d'une migration (voir [Pourquoi ne pas la déployer maintenant](#pourquoi-ne-pas-la-déployer-maintenant)).
> Le POC (`docker-compose.yml`) et le cluster Docker Compose
> (`docker-compose.cluster.yml`, voir [architecture_poc_vs_cluster.md](architecture_poc_vs_cluster.md))
> restent les architectures réellement développées et testées.

## Pourquoi Kubernetes (GKE), et pas des VPS externes

Deux façons d'obtenir une vraie tolérance de panne au niveau **infrastructure**
(pas seulement applicative) ont été évaluées : répartir Kafka/Spark sur
plusieurs VPS d'un hébergeur tiers (ex. OVH), ou les répartir sur un cluster
GKE. Le choix retenu pour la cible est **GKE**, pour rester sur un seul
fournisseur cloud :

| Critère | Multi-cloud (OVH + GCP) | Tout GCP (GKE) |
|---|---|---|
| Réseau Kafka ↔ Spark ↔ Cloud Storage ↔ BigQuery | Traverse l'Internet public entre deux fournisseurs — latence variable, egress facturé des deux côtés | Reste sur le réseau interne Google (VPC) — gratuit, faible latence |
| Sécurité / IAM | Deux périmètres à sécuriser et auditer (OVH + GCP), clés/secrets pour relier les deux | Un seul plan IAM, Workload Identity de bout en bout, pas de clé à faire transiter entre fournisseurs |
| Conformité RGPD (localisation FR/UE, 1.4 du rapport) | OVH (France) est conforme, mais l'audit de conformité porte sur **deux** fournisseurs distincts | Un seul fournisseur, un seul périmètre à auditer (`europe-west9`, Paris) |
| Exploitation | Deux consoles, deux systèmes de monitoring/logs, une facturation séparée | Cloud Logging/Monitoring couvre tout, une seule facture |
| Coût brut | Nettement moins cher (voir chiffrage ci-dessous) | Plus cher, mais le delta achète de la simplicité d'exploitation et de conformité |

Le choix GKE assume donc explicitement un **surcoût financier** en échange
d'une réduction de la complexité opérationnelle et du périmètre de
conformité — un arbitrage cohérent avec un projet qui doit rester
auditable par une seule DSI/DPO (voir 1.4 du rapport).

## Architecture

```
GKE régional (europe-west9, 3 zones : a, b, c)
  │
  ├─ node pool "kafka"   (3 nœuds, 1 broker/nœud, anti-affinité de zone)
  │     └─ Strimzi Operator → Kafka CR (KRaft, replication=3, min.insync.replicas=2)
  │
  ├─ node pool "spark"   (2 nœuds, driver + executors)
  │     └─ Spark Operator → SparkApplication (Structured Streaming, restartPolicy: Always)
  │
  └─ node pool "apps"    (1 nœud, producers + monitoring — stateless, légers)

Cloud Storage (raw) ──trigger──► Cloud Function silver_loader ──► BigQuery (silver, monitoring)
   (inchangé : ces deux services restent serverless, hors du cluster GKE)
```

Les brokers Kafka et les executors Spark tournent en pods sur les nœuds du
node pool dédié ; Kubernetes place lui-même les pods, et les reprogramme sur
un autre nœud du pool si l'un d'eux devient indisponible.

## Procédure de déploiement

### 1. Créer le cluster régional

```bash
gcloud container clusters create velib-data-cluster \
  --region europe-west9 \
  --num-nodes 1 \
  --node-locations europe-west9-a,europe-west9-b,europe-west9-c \
  --machine-type e2-standard-2 \
  --workload-pool velib-data-498413.svc.id.goog \
  --enable-autorepair \
  --enable-autoupgrade
```

`--region` + `--node-locations` sur 3 zones garantit la répartition
multi-zone. `--workload-pool` active Workload Identity : les pods
s'authentifient auprès de GCP (BigQuery, Cloud Storage) via un compte de
service GCP lié à un compte de service Kubernetes, sans clé JSON montée —
répond à l'exigence IAM/Secret Manager du DPO (2.1 du rapport).

Node pools dédiés (isoler Kafka évite qu'un pic de charge Spark n'affame les
brokers) :

```bash
gcloud container node-pools create kafka-pool \
  --cluster velib-data-cluster --region europe-west9 \
  --num-nodes 1 --node-locations europe-west9-a,europe-west9-b,europe-west9-c \
  --machine-type e2-medium

gcloud container node-pools create spark-pool \
  --cluster velib-data-cluster --region europe-west9 \
  --num-nodes 1 --machine-type e2-standard-2
```

### 2. Installer l'opérateur Kafka (Strimzi)

```bash
kubectl create namespace kafka
helm repo add strimzi https://strimzi.io/charts/
helm install strimzi-operator strimzi/strimzi-kafka-operator -n kafka
```

### 3. Déployer le cluster Kafka (KRaft, 3 brokers, anti-affinité de zone)

```yaml
# kafka-nodepool.yaml
apiVersion: kafka.strimzi.io/v1beta2
kind: KafkaNodePool
metadata:
  name: broker
  labels: { strimzi.io/cluster: velib-kafka }
spec:
  replicas: 3
  roles: [broker, controller]
  storage:
    type: persistent-claim
    size: 20Gi
    class: standard-rwo
  template:
    pod:
      affinity:
        podAntiAffinity:
          requiredDuringSchedulingIgnoredDuringExecution:
            - topologyKey: topology.kubernetes.io/zone
              labelSelector:
                matchLabels: { strimzi.io/cluster: velib-kafka }
---
# kafka-cluster.yaml
apiVersion: kafka.strimzi.io/v1beta2
kind: Kafka
metadata:
  name: velib-kafka
  annotations: { strimzi.io/node-pools: enabled, strimzi.io/kraft: enabled }
spec:
  kafka:
    version: 3.8.0
    replicas: 3
    listeners:
      - { name: plain, port: 9092, type: internal, tls: false }
    config:
      default.replication.factor: 3
      min.insync.replicas: 2
      offsets.topic.replication.factor: 3
      transaction.state.log.replication.factor: 3
  entityOperator:
    topicOperator: {}
```

```bash
kubectl apply -f kafka-nodepool.yaml -f kafka-cluster.yaml -n kafka
kubectl wait kafka/velib-kafka --for=condition=Ready --timeout=300s -n kafka
```

Le `podAntiAffinity` sur `topology.kubernetes.io/zone` garantit que les 3
brokers atterrissent sur 3 zones différentes — sans cette contrainte, rien
n'empêche le scheduler de les regrouper sur les mêmes nœuds, ce qui
annulerait le bénéfice de la haute disponibilité recherchée.

```yaml
apiVersion: kafka.strimzi.io/v1beta2
kind: KafkaTopic
metadata:
  name: velib_disponibilite
  labels: { strimzi.io/cluster: velib-kafka }
spec:
  partitions: 3
  replicas: 3
```

### 4. Déployer le job Spark (Structured Streaming, long-running)

```bash
helm repo add spark-operator https://kubeflow.github.io/spark-operator
helm install spark-operator spark-operator/spark-operator -n spark-operator --create-namespace
```

```yaml
apiVersion: sparkoperator.k8s.io/v1beta2
kind: SparkApplication
metadata: { name: velib-streaming }
spec:
  type: Python
  mode: cluster
  image: europe-west9-docker.pkg.dev/velib-data-498413/velib/pyspark:latest
  mainApplicationFile: local:///app/spark_streaming_job.py
  sparkVersion: 3.5.1
  restartPolicy: { type: Always }
  driver: { serviceAccount: velib-pipeline-ksa }
  executor: { instances: 2 }
  envVars:
    KAFKA_BOOTSTRAP_SERVERS: velib-kafka-kafka-bootstrap.kafka.svc:9092
    GCS_ENABLED: "true"
    GCP_PROJECT_ID: velib-data-498413
    GCS_BUCKET_NAME: velib-data-498413-raw
```

`restartPolicy: Always` remplace `restart: unless-stopped` — en cas de panne
d'un **nœud** (pas seulement du process), K8s reprogramme le driver ailleurs
dans le pool `spark-pool`, ce que Docker Compose ne peut pas faire.

### 5. Déployer producers et monitoring

`Deployment` K8s classiques (stateless), `serviceAccountName` en Workload
Identity, variables d'environnement inchangées par rapport à
`docker-compose.yml` — seul `KAFKA_BOOTSTRAP_SERVERS` pointe vers le service
Kubernetes exposé par Strimzi (`velib-kafka-kafka-bootstrap.kafka.svc:9092`)
au lieu de `kafka:9092`.

### 6. Autoscaling

```bash
kubectl autoscale deployment producers --min=1 --max=3 --cpu-percent=70
```

Pour un scaling piloté par le retard de consommation Kafka plutôt que le
CPU (plus pertinent pour ce pipeline) : **KEDA**, avec un `ScaledObject` sur
le trigger `kafka` (consumer group lag).

## Coûts détaillés

> Estimations basées sur les tarifs publics GCP on-demand, région
> `europe-west9` (Paris), hors remises d'engagement/usage prolongé. À
> vérifier précisément sur le [simulateur de prix GCP](https://cloud.google.com/products/calculator)
> avant toute décision budgétaire réelle — ces chiffres sont des ordres de
> grandeur, pas un devis.

| Poste | Détail | Coût mensuel estimé |
|---|---|---|
| Control plane GKE (régional) | $0,10/h × 730h — fixe, non couvert par le cluster gratuit (réservé aux clusters zonaux/Autopilot) | ≈ 73 $ |
| Node pool `kafka` | 3 × e2-medium (2 vCPU, 4 Go) × 730h | ≈ 74 $ |
| Node pool `spark` | 2 × e2-standard-2 (2 vCPU, 8 Go) × 730h | ≈ 98 $ |
| Node pool `apps` | 1 × e2-small × 730h | ≈ 15 $ |
| Disques persistants Kafka | 3 × 20 Go SSD (`pd-ssd`) | ≈ 11 $ |
| Artifact Registry (images Docker) | ~2-3 Go d'images | < 1 $ |
| **Sous-total infra GKE** | | **≈ 270-280 $/mois** |
| Cloud Function `silver_loader` | Volume actuel largement dans le free tier (2M invocations + 400 000 Go-s de calcul offerts/mois) | ≈ 0 $ |
| BigQuery (silver + monitoring) | Dans le free tier (10 Go stockage + 1 To de requêtes offerts/mois) à ce volume | ≈ 0 $ |
| Cloud Storage (raw) | Fichiers JSON de quelques Ko, volume hebdomadaire minime | quelques centimes |
| **Total estimé** | | **≈ 270-280 $/mois** |

### Comparaison avec les alternatives

| Option | Coût mensuel | HA infrastructure (reprise auto) | Réseau inter-service |
|---|---|---|---|
| **GKE régional (retenu)** | ≈ 270-280 $ | ✅ Automatique | Interne GCP |
| 3 VPS OVH (12 €/mois chacun) | ≈ 39 $ (36 €) | ❌ Manuelle | Internet public (OVH ↔ GCP) |
| 3 VM Compute Engine manuelles (sans K8s) | ≈ 190-200 $ (pas de frais de control plane) | ❌ Manuelle | Interne GCP |

Le delta GKE vs VM Compute Engine manuelles (≈ +80-90 $/mois, essentiellement
le control plane) achète exactement la reprise automatique en cas de panne
de nœud — c'est le service que Kubernetes facture. Le delta GKE vs OVH
(≈ +230 $/mois) achète en plus la simplicité d'un fournisseur unique
(réseau, IAM, conformité — voir tableau de justification plus haut).

## Pourquoi ne pas la déployer maintenant

Le pipeline documenté dans ce projet n'a plus qu'une semaine d'exploitation
prévue au moment de la rédaction. Sur cet horizon :

- **Coût infra sur 1 semaine** : negligeable dans l'absolu (≈ 65-70 $ pour
  GKE sur 7 jours) — ce n'est pas le facteur bloquant.
- **Coût d'ingénierie** : mettre en place Strimzi, l'opérateur Spark, migrer
  la configuration, tester le quorum multi-zone et la reprise sur panne
  demande plusieurs jours d'effort — pour un pipeline qui s'arrête dans la
  semaine, ce temps n'est pas récupérable.

La décision retenue est donc de **documenter cette architecture cible sans
la déployer**, et de conserver le pipeline actuel (POC ou cluster Docker
Compose sur VM unique) jusqu'à la fin de son cycle de vie. Ce choix est lui
aussi documenté ici pour tracer explicitement l'arbitrage coût/bénéfice —
voir la section "Contrôle des coûts et des performances" du rapport (4.7).

## Voir aussi

- [architecture_poc_vs_cluster.md](architecture_poc_vs_cluster.md) — architectures réellement développées et testées (POC, cluster Docker Compose)
- [fault_tolerance_and_recovery.md](fault_tolerance_and_recovery.md) — tolérance aux pannes des architectures actuelles
- [competency_mapping.md](competency_mapping.md) — Kubernetes y est déjà cité comme piste d'amélioration pour l'autoscaling et la configuration de clusters
