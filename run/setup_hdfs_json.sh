#!/bin/bash
# ==============================================================
# setup_hdfs_json.sh
# Qatar Ministry of Sports Demo - One-Time HDFS & Hive Setup (JSON Pipeline)
#
# Run ONCE as user 'cloudera' on cdp-master1:
#   bash /home/cloudera/mos_qatar_demo_poc/run/setup_hdfs_json.sh
# ==============================================================

set -uo pipefail

BASE_DIR="/home/cloudera/mos_qatar_demo_poc"
DDL_DIR="${BASE_DIR}/ddl"
LOG_DIR="${BASE_DIR}/logs"

HDFS_BASE="/data/mos_qatar_demo/json_files"
BEELINE_URL="jdbc:hive2://cdp-master1.cloudera.bbi:2181,cdp-master2.cloudera.bbi:2181,cdp-master3.cloudera.bbi:2181/default;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2"

mkdir -p "${LOG_DIR}"
TIMESTAMP="$(date '+%Y%m%d_%H%M%S')"
SETUP_LOG="${LOG_DIR}/setup_hdfs_json_${TIMESTAMP}.log"

log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" | tee -a "${SETUP_LOG}"
}

log "======================================================"
log "  MOS Qatar Demo - JSON HDFS & Hive Setup"
log "  User: $(whoami) | Host: $(hostname)"
log "======================================================"

# ── Create local landing and archive dirs for JSON ─────────────────────────
log ""
log "[1/5] Creating local directories for JSON pipeline..."
mkdir -p "${BASE_DIR}/landing_json"
mkdir -p "${BASE_DIR}/archive_json"
log "  ${BASE_DIR}/landing_json"
log "  ${BASE_DIR}/archive_json"

# ── Create HDFS directories ────────────────────────────────────────────────
log ""
log "[2/5] Creating HDFS directories..."
for HDFS_DIR in \
    "${HDFS_BASE}/raw" \
    "${HDFS_BASE}/gold" \
    "${HDFS_BASE}/metadata"; do
    hdfs dfs -mkdir -p "${HDFS_DIR}" 2>&1 | tee -a "${SETUP_LOG}"
    log "  Created: ${HDFS_DIR}"
done

# ── Set HDFS permissions ───────────────────────────────────────────────────
log ""
log "[3/5] Setting HDFS permissions..."
hdfs dfs -chmod -R 775 "${HDFS_BASE}"                  2>&1 | tee -a "${SETUP_LOG}"
hdfs dfs -chown -R cloudera:supergroup "${HDFS_BASE}"  2>&1 | tee -a "${SETUP_LOG}"
log "  Done."

# ── Verify HDFS structure ──────────────────────────────────────────────────
log ""
log "[4/5] HDFS listing:"
hdfs dfs -ls -R "${HDFS_BASE}" 2>&1 | tee -a "${SETUP_LOG}"

# ── Create Hive tables ─────────────────────────────────────────────────────
log ""
log "[5/5] Creating Hive JSON tables via beeline..."

log "  Creating sports_unstructured_raw_json..."
beeline -u "${BEELINE_URL}" \
    -n "" -p "" --silent=false \
    -f "${DDL_DIR}/create_raw_json_table.hql" \
    2>&1 | tee -a "${SETUP_LOG}"

log "  Creating sports_events_gold_json..."
beeline -u "${BEELINE_URL}" \
    -n "" -p "" --silent=false \
    -f "${DDL_DIR}/create_gold_json_table.hql" \
    2>&1 | tee -a "${SETUP_LOG}"

# ── Verify ─────────────────────────────────────────────────────────────────
log ""
log "  Verifying Hive tables..."
beeline -u "${BEELINE_URL}" \
    -n "" -p "" --silent=false \
    -e "USE mos_qatar_demo; SHOW TABLES;" \
    2>&1 | tee -a "${SETUP_LOG}"

log ""
log "======================================================"
log "  JSON HDFS & Hive Setup Completed Successfully!"
log "  Log: ${SETUP_LOG}"
log "======================================================"
