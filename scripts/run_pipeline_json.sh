#!/bin/bash
# ==============================================================
# run_pipeline_json.sh
# Qatar Ministry of Sports Demo
# MASTER ORCHESTRATOR — JSON-Based Pipeline
#
# Runs all three JSON pipeline jobs end-to-end in sequence:
#   Step 1 : 01_generate_data_json.py    (Python - generates .json file)
#   Step 2 : run_ingest_json.sh          (Python - local -> HDFS raw)
#   Step 3 : run_transform_json.sh       (PySpark - HDFS raw -> Hive gold)
#
# Sleeps SLEEP_BETWEEN_STEPS seconds between each step.
# Aborts immediately if any step fails (non-zero exit code).
#
# Usage:
#   bash /home/cloudera/mos_qatar_demo_poc/scripts/run_pipeline_json.sh
#
# NOTE: This script expects the JSON pipeline scripts to exist:
#   - scripts/01_generate_data_json.py
#   - scripts/run_ingest_json.sh
#   - scripts/run_transform_json.sh
#   These will be created in the JSON pipeline implementation phase.
#
# For full demo (text + JSON): use run_pipeline_all.sh instead.
# ==============================================================

set -uo pipefail

# ── Config ────────────────────────────────────────────────────────────────────
SLEEP_BETWEEN_STEPS=5          # seconds to pause between steps

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE_DIR="$(dirname "${SCRIPT_DIR}")"
CONFIG_FILE="${BASE_DIR}/config/mos_qatar_json.ini"
LOG_DIR="${BASE_DIR}/logs"

mkdir -p "${LOG_DIR}"
TIMESTAMP="$(date '+%Y%m%d_%H%M%S')"
MASTER_LOG="${LOG_DIR}/pipeline_json_${TIMESTAMP}.log"

PIPELINE_START=$(date +%s)

# ── Helpers ───────────────────────────────────────────────────────────────────
log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" | tee -a "${MASTER_LOG}"
}

banner() {
    log "=============================================================="
    log "  $1"
    log "=============================================================="
}

step_banner() {
    log ""
    log "╔══════════════════════════════════════════════════════════════╗"
    log "║  $1"
    log "╚══════════════════════════════════════════════════════════════╝"
}

elapsed() {
    local START=$1
    local END=$(date +%s)
    local DIFF=$((END - START))
    echo "${DIFF}s"
}

print_summary() {
    log ""
    log "══════════════════════════════════════════════════════════════"
    log "  PIPELINE SUMMARY — JSON"
    log "──────────────────────────────────────────────────────────────"
    log "  Step 1 - Generate  : ${STEP1_STATUS} ${STEP1_TIME:+(${STEP1_TIME})}"
    log "  Step 2 - Ingest    : ${STEP2_STATUS} ${STEP2_TIME:+(${STEP2_TIME})}"
    log "  Step 3 - Transform : ${STEP3_STATUS} ${STEP3_TIME:+(${STEP3_TIME})}"
    log "══════════════════════════════════════════════════════════════"
}

# ── Pre-flight checks ─────────────────────────────────────────────────────────
if [ ! -f "${CONFIG_FILE}" ]; then
    echo "ERROR: JSON config file not found: ${CONFIG_FILE}"
    echo "       Please ensure mos_qatar_json.ini exists in config/"
    exit 1
fi

for SCRIPT in \
    "${SCRIPT_DIR}/01_generate_data_json.py" \
    "${SCRIPT_DIR}/run_ingest_json.sh" \
    "${SCRIPT_DIR}/run_transform_json.sh"; do
    if [ ! -f "${SCRIPT}" ]; then
        echo "ERROR: Required script not found: ${SCRIPT}"
        echo "       JSON pipeline scripts need to be created first."
        exit 1
    fi
done

# ── Pipeline Start ────────────────────────────────────────────────────────────
banner "MOS Qatar Demo — JSON PIPELINE STARTED"
log "  Master log : ${MASTER_LOG}"
log "  Config     : ${CONFIG_FILE}"
log "  User       : $(whoami)"
log "  Host       : $(hostname)"
log "  Sleep gap  : ${SLEEP_BETWEEN_STEPS}s between steps"

STEP1_STATUS="NOT RUN" ; STEP1_TIME=""
STEP2_STATUS="NOT RUN" ; STEP2_TIME=""
STEP3_STATUS="NOT RUN" ; STEP3_TIME=""

# ══════════════════════════════════════════════════════════════════════════════
# STEP 1 — Generate .json data file
# ══════════════════════════════════════════════════════════════════════════════
step_banner "STEP 1 of 3 — Generate JSON Data File"
STEP_START=$(date +%s)

python3 "${SCRIPT_DIR}/01_generate_data_json.py" --config "${CONFIG_FILE}" \
    2>&1 | tee -a "${MASTER_LOG}"
EXIT_CODE="${PIPESTATUS[0]}"

STEP1_TIME=$(elapsed $STEP_START)

if [ "${EXIT_CODE}" -ne 0 ]; then
    STEP1_STATUS="FAILED"
    log "  ✗ STEP 1 FAILED (exit code ${EXIT_CODE}) — Aborting pipeline."
    print_summary
    exit 1
fi

STEP1_STATUS="SUCCESS"
log "  ✓ STEP 1 COMPLETE (${STEP1_TIME})"
log "  Sleeping ${SLEEP_BETWEEN_STEPS}s before next step..."
sleep "${SLEEP_BETWEEN_STEPS}"

# ══════════════════════════════════════════════════════════════════════════════
# STEP 2 — Ingest .json files to HDFS raw
# ══════════════════════════════════════════════════════════════════════════════
step_banner "STEP 2 of 3 — Ingest JSON to HDFS Raw"
STEP_START=$(date +%s)

bash "${SCRIPT_DIR}/run_ingest_json.sh" 2>&1 | tee -a "${MASTER_LOG}"
EXIT_CODE="${PIPESTATUS[0]}"

STEP2_TIME=$(elapsed $STEP_START)

if [ "${EXIT_CODE}" -ne 0 ]; then
    STEP2_STATUS="FAILED"
    log "  ✗ STEP 2 FAILED (exit code ${EXIT_CODE}) — Aborting pipeline."
    print_summary
    exit 1
fi

STEP2_STATUS="SUCCESS"
log "  ✓ STEP 2 COMPLETE (${STEP2_TIME})"
log "  Sleeping ${SLEEP_BETWEEN_STEPS}s before next step..."
sleep "${SLEEP_BETWEEN_STEPS}"

# ══════════════════════════════════════════════════════════════════════════════
# STEP 3 — Transform HDFS raw JSON to Hive gold (PySpark)
# ══════════════════════════════════════════════════════════════════════════════
step_banner "STEP 3 of 3 — Transform JSON to Hive Gold (PySpark)"
STEP_START=$(date +%s)

bash "${SCRIPT_DIR}/run_transform_json.sh" 2>&1 | tee -a "${MASTER_LOG}"
EXIT_CODE="${PIPESTATUS[0]}"

STEP3_TIME=$(elapsed $STEP_START)

if [ "${EXIT_CODE}" -ne 0 ]; then
    STEP3_STATUS="FAILED"
    log "  ✗ STEP 3 FAILED (exit code ${EXIT_CODE})"
    print_summary
    exit 1
fi

STEP3_STATUS="SUCCESS"

# ── Final Summary ─────────────────────────────────────────────────────────────
TOTAL_TIME=$(elapsed $PIPELINE_START)
print_summary
log "  Total elapsed      : ${TOTAL_TIME}"
log "  Master log         : ${MASTER_LOG}"
log "  ✓ JSON PIPELINE COMPLETED SUCCESSFULLY"
log "══════════════════════════════════════════════════════════════"

exit 0
