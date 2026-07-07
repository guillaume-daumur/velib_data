#!/usr/bin/env bash
# =============================================================================
# test_restart_producers.sh
#
# Teste la reprise des producers Python après un crash du container.
# Vérifie que les producers reprennent les appels API et republier dans Kafka
# sans intervention manuelle, grâce à restart: unless-stopped et wait_for_kafka().
#
# Usage :
#   bash scripts/test_restart_producers.sh
#
# Prérequis :
#   docker compose up --build  (pipeline doit être en cours d'exécution)
# =============================================================================

set -euo pipefail

echo "══════════════════════════════════════════════════════"
echo " TEST : Redémarrage du container Producers"
echo " Objectif : vérifier la reprise des appels API et des publications Kafka"
echo "══════════════════════════════════════════════════════"
echo ""

# ── Étape 1 : État initial ─────────────────────────────────────────────────────
echo "▶ [1/5] État des containers avant le test :"
docker compose ps
echo ""

# ── Étape 2 : Logs avant l'arrêt ─────────────────────────────────────────────
echo "▶ [2/5] Derniers logs producers (publications en cours) :"
echo "─────────────────────────────────────────────────────"
docker compose logs --tail=15 producers
echo "─────────────────────────────────────────────────────"
echo ""

# ── Étape 3 : Arrêt du container ──────────────────────────────────────────────
echo "▶ [3/5] Arrêt du container producers (simulation de crash)..."
docker compose stop producers
echo "   Container producers arrêté."
echo ""
echo "   → Pendant cet arrêt : Kafka reste actif, PySpark continue de tourner."
echo "   → Kafka ne reçoit plus de nouveaux messages (aucune publication)."
echo "   → PySpark traite les derniers messages encore dans les topics."
echo ""

sleep 5

# ── Étape 4 : Redémarrage ─────────────────────────────────────────────────────
echo "▶ [4/5] Redémarrage du container producers..."
docker compose start producers
echo "   Container producers redémarré."
echo ""
echo "   → Le producer exécute wait_for_kafka() avant de publier."
echo "   → Il créé (ou vérifie) les topics Kafka."
echo "   → Il démarre les deux threads : dispo + stations."
echo ""

# ── Étape 5 : Vérification de la reprise ─────────────────────────────────────
echo "▶ [5/5] Logs producers après redémarrage (attente 20s) :"
echo "─────────────────────────────────────────────────────"
echo "   → À surveiller :"
echo "     'Kafka prêt'                          ← connexion rétablie ✅"
echo "     'Fetching disponibilité data...'       ← appels API reprennent ✅"
echo "     'Published N records → topic ...'      ← publication Kafka ✅"
echo "─────────────────────────────────────────────────────"
sleep 20
docker compose logs --tail=30 producers
echo ""

echo "══════════════════════════════════════════════════════"
echo " RÉSULTAT ATTENDU"
echo "══════════════════════════════════════════════════════"
echo " ✅ Les producers se reconnectent à Kafka automatiquement."
echo " ✅ Les appels API Vélib' reprennent au prochain cycle."
echo " ✅ Les messages sont de nouveau publiés dans les topics Kafka."
echo ""
echo " NOTE :"
echo " Les snapshots API manqués pendant l'arrêt des producers sont perdus."
echo " La donnée Vélib' est temps réel : pas de mécanisme de rattrapage API."
echo " Ce comportement est acceptable pour un projet de monitoring en temps réel."
echo "══════════════════════════════════════════════════════"
