# silver_loader — Cloud Storage → BigQuery silver (temps réel)

Cloud Function 2e génération, déclenchée sur chaque nouveau fichier JSON déposé
par le job PySpark dans `gs://<bucket>/velib_disponibilite/...`.

## Flux de données

```
GCS (raw JSON)  ──trigger finalize──►  Cloud Function
                                            │
                                            ├─► load_table_from_uri  ──► table staging (temporaire, 1 par invocation)
                                            │
                                            └─► MERGE (dédoublonnage + typage) ──► silver.velib_disponibilite
                                                                                        │
                                                                              (staging supprimée juste après)
```

## Pourquoi une table de staging ?

C'est une zone de transit temporaire, pas une destination finale :

1. **BigQuery ne transforme pas pendant un chargement.** Un `load_table_from_uri`
   copie les données telles quelles dans une table — il ne peut pas appliquer de
   typage ni de logique métier. Pour caster `"OUI"` en `BOOL` et dédoublonner
   avec `QUALIFY ROW_NUMBER()`, les données doivent d'abord être *dans* une table
   BigQuery pour qu'on puisse écrire une requête SQL (le `MERGE`) dessus.
2. **Une table par invocation, jamais partagée.** Plusieurs fichiers peuvent
   arriver et déclencher des invocations en parallèle. Si elles écrivaient dans
   la même table staging, elles se marcheraient dessus (écrasements, races). Le
   nom de la table est donc unique par invocation (suffixe UUID, voir `main.py`),
   ce qui isole chaque exécution.
3. **Coût quasi nul.** Le chargement GCS → table native est un *load job*,
   gratuit chez BigQuery (pas de coût de requête). La table ne vit que le temps
   du `MERGE` (quelques secondes) : elle est supprimée juste après dans le
   `finally` de `on_new_raw_file`.

L'expiration par défaut (1 jour) configurée sur le dataset `staging` (étape 1
ci-dessous) est un filet de sécurité : si la fonction plante entre le
chargement et la suppression, la table orpheline disparaît toute seule au lieu
de s'accumuler indéfiniment.

## Pourquoi le filtre de partition dans le `MERGE`

La clause `ON ... AND DATE(target.request_timestamp) = @batch_date` permet à
BigQuery d'éliminer toutes les partitions (journées) non concernées de la table
`silver` avant même de scanner. Sans ce filtre, chaque exécution (toutes les
~30s) scannerait tout l'historique de la table cible, avec un coût qui grandit
avec le volume de données déjà accumulé.

## Procédure de déploiement

### 1. Créer les datasets BigQuery

Sur [console.cloud.google.com/bigquery](https://console.cloud.google.com/bigquery) :

- Dataset **`silver`** — région `europe-west1` (même que le bucket)
- Dataset **`staging`** — région `europe-west1`, avec une **expiration de table
  par défaut de 1 jour** (Options avancées à la création)

### 2. Créer la table silver

Exécuter [`bigquery/schema_silver.sql`](../../bigquery/schema_silver.sql)
(remplacer `MY_PROJECT_ID` par le vrai projet) dans l'éditeur de requêtes.

### 3. Créer le service account de la fonction

**IAM et administration → Comptes de service → Créer** : `velib-silver-loader`
(pas de clé JSON — Cloud Functions s'exécute directement avec ce compte).

Permissions à accorder :
- Sur le bucket raw (onglet Permissions du bucket) : **Storage Object Viewer**
- Au niveau projet (IAM → Accorder l'accès) : **BigQuery Data Editor** +
  **BigQuery Job User**

### 4. Créer la Cloud Function

**Cloud Functions → Créer une fonction**
- Environnement : 2ᵉ génération — Région : `europe-west1`
- Déclencheur : Cloud Storage → `On (finalize/create)` → bucket raw
- Compte de service d'exécution : `velib-silver-loader`
- Runtime : Python 3.11+ — Point d'entrée : `on_new_raw_file`
- Variables d'environnement : `GCP_PROJECT_ID`, `BQ_SILVER_DATASET=silver`,
  `BQ_STAGING_DATASET=staging`
- Code source : `main.py` et `requirements.txt` de ce dossier

### 5. Vérifier

```sql
-- Dernières lignes
SELECT stationcode, request_timestamp, numbikesavailable, kafka_timestamp
FROM `PROJECT.silver.velib_disponibilite`
ORDER BY request_timestamp DESC LIMIT 20;

-- Doit renvoyer 0 ligne (pas de doublon)
SELECT stationcode, request_timestamp, COUNT(*) AS n
FROM `PROJECT.silver.velib_disponibilite`
GROUP BY 1, 2 HAVING n > 1;
```
