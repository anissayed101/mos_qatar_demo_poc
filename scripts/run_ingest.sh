#!/bin/bash
# ==============================================================
# run_ingest.sh
# Qatar Ministry of Sports Demo - Shell Wrapper for HDFS Raw Ingestion
#
# Invokes Job 2 (02_ingest_to_hdfs.py) via python3.
# Logs wrapper output separately from Python log.
# Exit code propagated for cron/Oozie error detection.
#
# Usage:
#   bash /opt/mos_qatar_demo/scripts/run_ingest.sh
#
# For Oozie migration: use a Shell action pointing to this script.
# ==============================================================

set -uo pipefail

# ── Resolve paths ─────────────────────────────────────────────────────────────
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE_DIR="$(dirname "${SCRIPT_DIR}")"
CONFIG_FILE="${BASE_DIR}/config/mos_qatar_demo.ini"
PYTHON_SCRIPT="${SCRIPT_DIR}/02_ingest_to_hdfs.py"
LOG_DIR="${BASE_DIR}/logs"

mkdir -p "${LOG_DIR}"

TIMESTAMP="$(date '+%Y%m%d_%H%M%S')"
WRAPPER_LOG="${LOG_DIR}/run_ingest_${TIMESTAMP}.log"

# ── Logging helper ────────────────────────────────────────────────────────────
log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" | tee -a "${WRAPPER_LOG}"
}

log "======================================================"
log "  MOS Qatar Demo - run_ingest.sh Started"
log "======================================================"
log "  Script  : ${PYTHON_SCRIPT}"
log "  Config  : ${CONFIG_FILE}"
log "  WrapLog : ${WRAPPER_LOG}"
log "  User    : $(whoami)"
log "  Host    : $(hostname)"
log "------------------------------------------------------"

# ── Pre-flight checks ─────────────────────────────────────────────────────────
if [ ! -f "${PYTHON_SCRIPT}" ]; then
    log "ERROR: Python script not found: ${PYTHON_SCRIPT}"
    exit 1
fi

if [ ! -f "${CONFIG_FILE}" ]; then
    log "ERROR: Config file not found: ${CONFIG_FILE}"
    exit 1
fi

# ── Run ingestion job ─────────────────────────────────────────────────────────
log "Launching: python3 ${PYTHON_SCRIPT}"

python3 "${PYTHON_SCRIPT}" --config "${CONFIG_FILE}" 2>&1 | tee -a "${WRAPPER_LOG}"
EXIT_CODE="${PIPESTATUS[0]}"

log "------------------------------------------------------"
if [ "${EXIT_CODE}" -eq 0 ]; then
    log "  STATUS: SUCCESS (exit code 0)"
else
    log "  STATUS: FAILED  (exit code ${EXIT_CODE})"
fi
log "======================================================"

exit "${EXIT_CODE}"
