#!/bin/bash
# ==============================================================
# run_transform.sh
# Qatar Ministry of Sports Demo - Shell Wrapper for Spark Gold Transform
#
# Submits Job 3 (03_transform_to_gold.py) via spark3-submit on YARN.
# Logs wrapper output separately from PySpark application log.
# Exit code propagated for cron/Oozie error detection.
#
# Usage:
#   bash /opt/mos_qatar_demo/scripts/run_transform.sh
#
# For Oozie migration: use a Spark action with spark3-submit parameters below.
# ==============================================================

set -uo pipefail

# ── Resolve paths ─────────────────────────────────────────────────────────────
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE_DIR="$(dirname "${SCRIPT_DIR}")"
CONFIG_FILE="${BASE_DIR}/config/mos_qatar_demo.ini"
PYTHON_SCRIPT="${SCRIPT_DIR}/03_transform_to_gold.py"
LOG_DIR="${BASE_DIR}/logs"

mkdir -p "${LOG_DIR}"

TIMESTAMP="$(date '+%Y%m%d_%H%M%S')"
WRAPPER_LOG="${LOG_DIR}/run_transform_${TIMESTAMP}.log"

# ── Logging helper ────────────────────────────────────────────────────────────
log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" | tee -a "${WRAPPER_LOG}"
}

log "======================================================"
log "  MOS Qatar Demo - run_transform.sh Started"
log "======================================================"
log "  Script  : ${PYTHON_SCRIPT}"
log "  Config  : ${CONFIG_FILE}"
log "  WrapLog : ${WRAPPER_LOG}"
log "  User    : $(whoami)"
log "  Host    : $(hostname)"
log "------------------------------------------------------"

# ── Pre-flight checks ─────────────────────────────────────────────────────────
if [ ! -f "${PYTHON_SCRIPT}" ]; then
    log "ERROR: PySpark script not found: ${PYTHON_SCRIPT}"
    exit 1
fi

if [ ! -f "${CONFIG_FILE}" ]; then
    log "ERROR: Config file not found: ${CONFIG_FILE}"
    exit 1
fi

# ── spark3-submit ─────────────────────────────────────────────────────────────
log "Submitting Spark job via spark3-submit..."
log "  master       : yarn"
log "  deploy-mode  : client"
log "  executors    : 2 x 1 core x 1g"
log "------------------------------------------------------"

spark3-submit \
    --master yarn \
    --deploy-mode client \
    --executor-memory 1g \
    --executor-cores 1 \
    --num-executors 2 \
    --name "MOS_Qatar_Gold_Transform" \
    --conf "spark.yarn.appMasterEnv.PYSPARK_PYTHON=/usr/bin/python3" \
    --conf "spark.executorEnv.PYSPARK_PYTHON=/usr/bin/python3" \
    --conf "spark.sql.hive.convertMetastoreParquet=false" \
    --conf "hive.exec.dynamic.partition=true" \
    --conf "hive.exec.dynamic.partition.mode=nonstrict" \
    "${PYTHON_SCRIPT}" \
    --config "${CONFIG_FILE}" \
    2>&1 | tee -a "${WRAPPER_LOG}"

EXIT_CODE="${PIPESTATUS[0]}"

log "------------------------------------------------------"
if [ "${EXIT_CODE}" -eq 0 ]; then
    log "  STATUS: SUCCESS (exit code 0)"
else
    log "  STATUS: FAILED  (exit code ${EXIT_CODE})"
fi
log "======================================================"

exit "${EXIT_CODE}"
