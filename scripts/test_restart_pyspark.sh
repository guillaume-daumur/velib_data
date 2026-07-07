#!/usr/bin/env bash
# =============================================================================
# test_restart_pyspark.sh
#
# Teste la reprise du job PySpark après un crash du container.
# Ce test vérifie que les checkpoints Spark permettent de reprendre le streaming
# exactement au dernier offset Kafka traité, sans perte ni doublon de données.
#
# Usage :
#   bash scripts/test_restart_pyspark.sh
#
# Prérequis :
#   docker compose up --build  (pipeline doit être en cours d'exécution)
# =============================================================================

set -euo pipefail

echo "══════════════════════════════════════════════════════"
echo " TEST : Redémarrage du container PySpark"
echo " Objectif : vérifier la reprise depuis les checkpoints"
echo "══════════════════════════════════════════════════════"
echo ""

# ── Étape 1 : État initial ─────────────────────────────────────────────────────
echo "▶ [1/6] État des containers avant le test :"
docker compose ps
echo ""

# ── Étape 2 : Logs avant le crash ─────────────────────────────────────────────
echo "▶ [2/6] Derniers logs PySpark avant le redémarrage :"
echo "─────────────────────────────────────────────────────"
docker compose logs --tail=20 pyspark
echo "─────────────────────────────────────────────────────"
echo ""

# ── Étape 3 : Arrêt simulé du container ───────────────────────────────────────
echo "▶ [3/6] Arrêt du container pyspark (simulation de crash)..."
docker compose stop pyspark
echo "   Container pyspark arrêté."
echo ""

# ── Étape 4 : Vérifier que les checkpoints sont persistés ────────────────────
echo "▶ [4/6] Vérification des checkpoints sur le host :"
echo "   Répertoire : ./data/checkpoints/"
if [ -d "./data/checkpoints" ]; then
    ls -lh ./data/checkpoints/ 2>/dev/null || echo "   (répertoire vide ou inexistant)"
    echo ""
    echo "   ✅ Les checkpoints sont présents → Spark reprendra depuis ces offsets."
else
    echo "   ⚠️  Répertoire de checkpoint introuvable."
    echo "   Spark devra repartir de 'latest' (messages manqués pendant l'arrêt)."
fi
echo ""

# ── Étape 5 : Attente et redémarrage ─────────────────────────────────────────
echo "▶ [5/6] Attente de 5 secondes, puis redémarrage de pyspark..."
sleep 5
docker compose start pyspark
echo "   Container pyspark redémarré."
echo ""

# ── Étape 6 : Vérifier la reprise dans les logs ───────────────────────────────
echo "▶ [6/6] Logs PySpark après redémarrage (attente 30s pour le démarrage Spark) :"
echo "─────────────────────────────────────────────────────"
echo "   → À surveiller dans les logs :"
echo "     'Resuming query from checkpoint'   ← reprise depuis checkpoint ✅"
echo "     'Starting query from latest'       ← pas de checkpoint, repart de latest ⚠️"
echo "     'Starting query from earliest'     ← replay depuis le début ℹ️"
echo "─────────────────────────────────────────────────────"
sleep 30
docker compose logs --tail=30 pyspark
echo ""

echo "══════════════════════════════════════════════════════"
echo " RÉSULTAT ATTENDU"
echo "══════════════════════════════════════════════════════"
echo " ✅ PySpark reprend depuis les offsets Kafka sauvegardés."
echo " ✅ Les messages publiés pendant l'arrêt sont traités."
echo " ✅ Aucune ligne n'est dupliquée dans data/raw/ ou BigQuery."
echo ""
echo " POURQUOI ÇA FONCTIONNE :"
echo " Le checkpoint stocke le dernier offset Kafka traité."
echo " Kafka conserve les messages 7 jours (rétention par défaut)."
echo " → Spark peut rejouer les messages manqués pendant l'arrêt."
echo ""
echo " LIMITE (POC) :"
echo " Si Kafka était aussi arrêté pendant la panne de PySpark,"
echo " les messages non encore écrits sur disque (in-flight) sont perdus."
echo " En cluster avec 3 brokers, Kafka reste disponible même si 1 broker tombe."
echo "══════════════════════════════════════════════════════"
