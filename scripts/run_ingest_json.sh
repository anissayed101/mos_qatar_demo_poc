#!/bin/bash
# ==============================================================
# run_ingest_json.sh
# Qatar Ministry of Sports Demo - Shell Wrapper for JSON HDFS Ingestion
# ==============================================================

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE_DIR="$(dirname "${SCRIPT_DIR}")"
CONFIG_FILE="${BASE_DIR}/config/mos_qatar_json.ini"
PYTHON_SCRIPT="${SCRIPT_DIR}/02_ingest_to_hdfs_json.py"
LOG_DIR="${BASE_DIR}/logs"

mkdir -p "${LOG_DIR}"
TIMESTAMP="$(date '+%Y%m%d_%H%M%S')"
WRAPPER_LOG="${LOG_DIR}/run_ingest_json_${TIMESTAMP}.log"

log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" | tee -a "${WRAPPER_LOG}"
}

log "======================================================"
log "  MOS Qatar Demo - run_ingest_json.sh Started"
log "======================================================"
log "  Script  : ${PYTHON_SCRIPT}"
log "  Config  : ${CONFIG_FILE}"
log "  WrapLog : ${WRAPPER_LOG}"
log "  User    : $(whoami)"
log "  Host    : $(hostname)"
log "------------------------------------------------------"

[ ! -f "${PYTHON_SCRIPT}" ] && { log "ERROR: Script not found: ${PYTHON_SCRIPT}"; exit 1; }
[ ! -f "${CONFIG_FILE}"   ] && { log "ERROR: Config not found: ${CONFIG_FILE}";   exit 1; }

log "Launching: python3 ${PYTHON_SCRIPT}"

python3 "${PYTHON_SCRIPT}" --config "${CONFIG_FILE}" 2>&1 | tee -a "${WRAPPER_LOG}"
EXIT_CODE="${PIPESTATUS[0]}"

log "------------------------------------------------------"
[ "${EXIT_CODE}" -eq 0 ] \
    && log "  STATUS: SUCCESS (exit code 0)" \
    || log "  STATUS: FAILED  (exit code ${EXIT_CODE})"
log "======================================================"

exit "${EXIT_CODE}"
