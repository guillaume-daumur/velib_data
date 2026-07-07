#!/usr/bin/env bash
# =============================================================================
# test_restart_kafka.sh
#
# Teste la résilience du pipeline face à une panne du broker Kafka.
# Dans le POC (1 seul broker), Kafka est un SPOF (Single Point Of Failure) :
# sa panne interrompt le pipeline complet.
#
# Ce test illustre :
#   - l'impact d'une panne Kafka sur les producers et PySpark,
#   - la récupération automatique grâce à restart: unless-stopped,
#   - la reprise du pipeline une fois Kafka rétabli.
#
# Usage :
#   bash scripts/test_restart_kafka.sh
#
# Prérequis :
#   docker compose up --build  (pipeline doit être en cours d'exécution)
# =============================================================================

set -euo pipefail

echo "══════════════════════════════════════════════════════"
echo " TEST : Redémarrage du broker Kafka"
echo " Objectif : observer l'impact d'une panne Kafka (SPOF en POC)"
echo " et vérifier la reprise du pipeline après rétablissement"
echo "══════════════════════════════════════════════════════"
echo ""

# ── Avertissement SPOF ────────────────────────────────────────────────────────
echo "⚠️  ATTENTION — POINT UNIQUE DE PANNE (POC)"
echo "   En POC, le broker Kafka est un Single Point Of Failure."
echo "   Sa panne interrompt TOUT le pipeline."
echo "   En architecture cluster (3 brokers), 1 broker peut tomber"
echo "   sans que le pipeline soit interrompu."
echo ""

# ── Étape 1 : État initial ─────────────────────────────────────────────────────
echo "▶ [1/7] État des containers avant la panne :"
docker compose ps
echo ""

# ── Étape 2 : Logs avant la panne ────────────────────────────────────────────
echo "▶ [2/7] Logs producers (publications normales) :"
docker compose logs --tail=10 producers
echo ""

# ── Étape 3 : Arrêt de Kafka ──────────────────────────────────────────────────
echo "▶ [3/7] Arrêt du broker Kafka (simulation de panne)..."
docker compose stop kafka
echo "   Broker Kafka arrêté."
echo ""

# ── Étape 4 : Observer les erreurs ────────────────────────────────────────────
echo "▶ [4/7] Attente 10s — observation des erreurs sur producers et pyspark :"
sleep 10
echo ""
echo "   Logs producers (doivent montrer des erreurs de connexion Kafka) :"
echo "─────────────────────────────────────────────────────"
docker compose logs --tail=15 producers 2>/dev/null || echo "   (container en erreur)"
echo "─────────────────────────────────────────────────────"
echo ""
echo "   Logs pyspark (doivent montrer des erreurs de connexion Kafka) :"
echo "─────────────────────────────────────────────────────"
docker compose logs --tail=15 pyspark 2>/dev/null || echo "   (container en erreur)"
echo "─────────────────────────────────────────────────────"
echo ""

echo "   → COMPORTEMENT ATTENDU PENDANT LA PANNE :"
echo "     - producers  : erreurs 'Connection refused' ou 'Kafka not ready'"
echo "                    Les threads retry en boucle (wait_for_kafka)."
echo "     - pyspark    : Spark Structured Streaming suspend le micro-batch en cours"
echo "                    et tente de se reconnecter à Kafka."
echo "     - Les deux services restent actifs (restart: unless-stopped ne les tue pas)"
echo "       mais ne traitent plus de données."
echo ""

# ── Étape 5 : Redémarrage Kafka ───────────────────────────────────────────────
echo "▶ [5/7] Redémarrage du broker Kafka..."
docker compose start kafka
echo "   Broker Kafka redémarré — attente du healthcheck (30-45s)..."
echo ""

# Attendre que kafka passe healthy
MAX_WAIT=60
WAITED=0
while [ $WAITED -lt $MAX_WAIT ]; do
    STATUS=$(docker inspect --format='{{.State.Health.Status}}' kafka 2>/dev/null || echo "unknown")
    if [ "$STATUS" = "healthy" ]; then
        echo "   ✅ Kafka est healthy après ${WAITED}s."
        break
    fi
    echo "   Attente... (${WAITED}s / ${MAX_WAIT}s) — statut actuel : $STATUS"
    sleep 5
    WAITED=$((WAITED + 5))
done
echo ""

# ── Étape 6 : Vérifier la reprise du pipeline ────────────────────────────────
echo "▶ [6/7] Vérification de la reprise (attente 15s) :"
sleep 15
echo ""
echo "   Logs producers après reprise Kafka :"
echo "─────────────────────────────────────────────────────"
docker compose logs --tail=20 producers
echo "─────────────────────────────────────────────────────"
echo ""
echo "   Logs pyspark après reprise Kafka :"
echo "─────────────────────────────────────────────────────"
docker compose logs --tail=20 pyspark
echo "─────────────────────────────────────────────────────"
echo ""

# ── Étape 7 : État final ──────────────────────────────────────────────────────
echo "▶ [7/7] État final des containers :"
docker compose ps
echo ""

echo "══════════════════════════════════════════════════════"
echo " RÉSULTAT ATTENDU"
echo "══════════════════════════════════════════════════════"
echo " ✅ Kafka redémarre et retrouve ses données (volume kafka-data persistant)."
echo " ✅ Les producers reprennent automatiquement les publications."
echo " ✅ PySpark reprend le streaming depuis les checkpoints."
echo ""
echo " DONNÉES PENDANT LA PANNE :"
echo " ⚠️  Les messages que les producers tentaient de publier pendant la panne"
echo "     Kafka sont PERDUS (pas de broker disponible pour les recevoir)."
echo "     Les données Vélib' de la période de panne sont absentes."
echo ""
echo " DIFFÉRENCE AVEC L'ARCHITECTURE CLUSTER :"
echo " En cluster (3 brokers) :"
echo "   → Si 1 broker tombe, les 2 autres continuent de recevoir les messages."
echo "   → Kafka élit automatiquement un nouveau leader pour les partitions affectées."
echo "   → Le pipeline continue SANS INTERRUPTION."
echo "   → Voir : docker compose -f docker-compose.cluster.yml up --build"
echo "══════════════════════════════════════════════════════"
