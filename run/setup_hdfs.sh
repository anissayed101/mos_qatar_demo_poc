#!/bin/bash
# ==============================================================
# setup_hdfs.sh
# Qatar Ministry of Sports Demo - One-Time HDFS & Hive Setup
#
# Creates all HDFS directories, sets permissions, and creates
# the Hive database and both tables via beeline.
#
# Run ONCE as user 'cloudera' on cdp-master1 (node 131):
#   bash /opt/mos_qatar_demo/run/setup_hdfs.sh
#
# Prerequisites:
#   - setup_local.sh already executed
#   - HDFS is running and accessible
#   - HiveServer2 is running and accessible via ZooKeeper
#   - DDL files exist under /opt/mos_qatar_demo/ddl/
# ==============================================================

set -uo pipefail

BASE_DIR="/home/cloudera/mos_qatar_demo_poc"
DDL_DIR="${BASE_DIR}/ddl"
LOG_DIR="${BASE_DIR}/logs"

HDFS_BASE="/data/mos_qatar_demo/text_files"

BEELINE_URL="jdbc:hive2://cdp-master1.cloudera.bbi:2181,cdp-master2.cloudera.bbi:2181,cdp-master3.cloudera.bbi:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2"
BEELINE_URL_2="jdbc:hive2://cdp-master1.cloudera.bbi:2181,cdp-master2.cloudera.bbi:2181,cdp-master3.cloudera.bbi:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2"

mkdir -p "${LOG_DIR}"
TIMESTAMP="$(date '+%Y%m%d_%H%M%S')"
SETUP_LOG="${LOG_DIR}/setup_hdfs_${TIMESTAMP}.log"

log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" | tee -a "${SETUP_LOG}"
}

log "======================================================"
log "  MOS Qatar Demo - HDFS & Hive Setup"
log "  User: $(whoami) | Host: $(hostname)"
log "======================================================"

# ── HDFS: Create directory structure ──────────────────────────────────────
log ""
log "[1/5] Creating HDFS directories..."

for HDFS_DIR in \
    "${HDFS_BASE}/raw" \
    "${HDFS_BASE}/gold" \
    "${HDFS_BASE}/silver" \
    "${HDFS_BASE}/metadata/raw_ingestion" \
    "${HDFS_BASE}/metadata/gold_processing"; do

    hdfs dfs -mkdir -p "${HDFS_DIR}" 2>&1 | tee -a "${SETUP_LOG}"
    log "  Created: ${HDFS_DIR}"
done

# ── HDFS: Set permissions ──────────────────────────────────────────────────
log ""
log "[2/5] Setting HDFS permissions..."
hdfs dfs -chmod -R 775 "${HDFS_BASE}"                  2>&1 | tee -a "${SETUP_LOG}"
hdfs dfs -chown -R cloudera:supergroup "${HDFS_BASE}"  2>&1 | tee -a "${SETUP_LOG}"
log "  Permissions set on ${HDFS_BASE}"

# ── HDFS: Verify structure ─────────────────────────────────────────────────
log ""
log "[3/5] HDFS directory listing:"
hdfs dfs -ls -R "${HDFS_BASE}" 2>&1 | tee -a "${SETUP_LOG}"

# ── Hive: Create database ──────────────────────────────────────────────────
log ""
log "[4/5] Creating Hive database (mos_qatar_demo)..."
beeline -u "${BEELINE_URL}" \
    -n "cloudera" -p "cloudera" \
    --silent=false \
    -f "${DDL_DIR}/create_database.hql" \
    2>&1 | tee -a "${SETUP_LOG}"
log "  Database DDL done."

# ── Hive: Create raw table ─────────────────────────────────────────────────
log ""
log "  Creating raw table (sports_unstructured_raw)..."
beeline -u "${BEELINE_URL}" \
    -n "cloudera" -p "cloudera" \
    --silent=false \
    -f "${DDL_DIR}/create_raw_table.hql" \
    2>&1 | tee -a "${SETUP_LOG}"
log "  Raw table DDL done."

# ── Hive: Create gold table ────────────────────────────────────────────────
log ""
log "  Creating gold table (sports_events_gold)..."
beeline -u "${BEELINE_URL}" \
    -n "cloudera" -p "cloudera" \
    --silent=false \
    -f "${DDL_DIR}/create_gold_table.hql" \
    2>&1 | tee -a "${SETUP_LOG}"
log "  Gold table DDL done."

# ── Final summary ──────────────────────────────────────────────────────────
log ""
log "[5/5] Verifying Hive objects..."
beeline -u "${BEELINE_URL_2}" \
    -n "cloudera" -p "cloudera" \
    --silent=false \
    -e "SHOW TABLES; GRANT SELECT ON TABLE mos_qatar_demo.sports_events_gold TO USER cloudera; GRANT SELECT ON TABLE mos_qatar_demo.sports_unstructured_raw TO USER cloudera;" \
    2>&1 | tee -a "${SETUP_LOG}"

log ""
log "======================================================"
log "  HDFS & Hive Setup Completed Successfully!"
log "  Log : ${SETUP_LOG}"
log "======================================================"
