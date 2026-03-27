#!/bin/bash
# ==============================================================
# run_pipeline_text.sh
# Qatar Ministry of Sports Demo
# MASTER ORCHESTRATOR — Text-Based Pipeline
#
# Runs all three text pipeline jobs end-to-end in sequence:
#   Step 1 : 01_generate_data.py    (Python - generates .txt file)
#   Step 2 : 02_ingest_to_hdfs.py   (Python - local -> HDFS raw)
#   Step 3 : 03_transform_to_gold.py (PySpark - HDFS raw -> Hive gold)
#
# Sleeps SLEEP_BETWEEN_STEPS seconds between each step.
# Aborts immediately if any step fails (non-zero exit code).
#
# Usage:
#   bash /home/cloudera/mos_qatar_demo_poc/scripts/run_pipeline_text.sh
#
# For full demo (text + JSON): use run_pipeline_all.sh instead.
# ==============================================================

set -uo pipefail

# ── Config ────────────────────────────────────────────────────────────────────
SLEEP_BETWEEN_STEPS=5          # seconds to pause between steps

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE_DIR="$(dirname "${SCRIPT_DIR}")"
CONFIG_FILE="${BASE_DIR}/config/mos_qatar_demo.ini"
LOG_DIR="${BASE_DIR}/logs"

mkdir -p "${LOG_DIR}"
TIMESTAMP="$(date '+%Y%m%d_%H%M%S')"
MASTER_LOG="${LOG_DIR}/pipeline_text_${TIMESTAMP}.log"

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

# ── Pre-flight checks ─────────────────────────────────────────────────────────
if [ ! -f "${CONFIG_FILE}" ]; then
    echo "ERROR: Config file not found: ${CONFIG_FILE}"
    exit 1
fi

for SCRIPT in \
    "${SCRIPT_DIR}/01_generate_data.py" \
    "${SCRIPT_DIR}/run_ingest.sh" \
    "${SCRIPT_DIR}/run_transform.sh"; do
    if [ ! -f "${SCRIPT}" ]; then
        echo "ERROR: Required script not found: ${SCRIPT}"
        exit 1
    fi
done

# ── Pipeline Start ────────────────────────────────────────────────────────────
banner "MOS Qatar Demo — TEXT PIPELINE STARTED"
log "  Master log : ${MASTER_LOG}"
log "  Config     : ${CONFIG_FILE}"
log "  User       : $(whoami)"
log "  Host       : $(hostname)"
log "  Sleep gap  : ${SLEEP_BETWEEN_STEPS}s between steps"

# ── Track per-step results ────────────────────────────────────────────────────
STEP1_STATUS="NOT RUN"
STEP2_STATUS="NOT RUN"
STEP3_STATUS="NOT RUN"
STEP1_TIME="-"
STEP2_TIME="-"
STEP3_TIME="-"

# ══════════════════════════════════════════════════════════════════════════════
# STEP 1 — Generate .txt data file
# ══════════════════════════════════════════════════════════════════════════════
step_banner "STEP 1 of 3 — Generate Text Data File"
STEP_START=$(date +%s)

python3 "${SCRIPT_DIR}/01_generate_data.py" --config "${CONFIG_FILE}" \
    2>&1 | tee -a "${MASTER_LOG}"
EXIT_CODE="${PIPESTATUS[0]}"

STEP1_TIME=$(elapsed $STEP_START)

if [ "${EXIT_CODE}" -ne 0 ]; then
    STEP1_STATUS="FAILED"
    log ""
    log "  ✗ STEP 1 FAILED (exit code ${EXIT_CODE}) — Aborting pipeline."
    log ""
    log "  PIPELINE ABORTED at Step 1."
    log "  Check log: ${MASTER_LOG}"
    # Print summary before exit
    log ""
    log "══════════════════════════════════════════════════════════════"
    log "  PIPELINE SUMMARY — TEXT"
    log "──────────────────────────────────────────────────────────────"
    log "  Step 1 - Generate  : ${STEP1_STATUS} (${STEP1_TIME})"
    log "  Step 2 - Ingest    : ${STEP2_STATUS}"
    log "  Step 3 - Transform : ${STEP3_STATUS}"
    log "══════════════════════════════════════════════════════════════"
    exit 1
fi

STEP1_STATUS="SUCCESS"
log "  ✓ STEP 1 COMPLETE (${STEP1_TIME})"
log "  Sleeping ${SLEEP_BETWEEN_STEPS}s before next step..."
sleep "${SLEEP_BETWEEN_STEPS}"

# ══════════════════════════════════════════════════════════════════════════════
# STEP 2 — Ingest .txt files to HDFS raw
# ══════════════════════════════════════════════════════════════════════════════
step_banner "STEP 2 of 3 — Ingest to HDFS Raw"
STEP_START=$(date +%s)

bash "${SCRIPT_DIR}/run_ingest.sh" 2>&1 | tee -a "${MASTER_LOG}"
EXIT_CODE="${PIPESTATUS[0]}"

STEP2_TIME=$(elapsed $STEP_START)

if [ "${EXIT_CODE}" -ne 0 ]; then
    STEP2_STATUS="FAILED"
    log ""
    log "  ✗ STEP 2 FAILED (exit code ${EXIT_CODE}) — Aborting pipeline."
    log ""
    log "══════════════════════════════════════════════════════════════"
    log "  PIPELINE SUMMARY — TEXT"
    log "──────────────────────────────────────────────────────────────"
    log "  Step 1 - Generate  : ${STEP1_STATUS} (${STEP1_TIME})"
    log "  Step 2 - Ingest    : ${STEP2_STATUS} (${STEP2_TIME})"
    log "  Step 3 - Transform : ${STEP3_STATUS}"
    log "══════════════════════════════════════════════════════════════"
    exit 1
fi

STEP2_STATUS="SUCCESS"
log "  ✓ STEP 2 COMPLETE (${STEP2_TIME})"
log "  Sleeping ${SLEEP_BETWEEN_STEPS}s before next step..."
sleep "${SLEEP_BETWEEN_STEPS}"

# ══════════════════════════════════════════════════════════════════════════════
# STEP 3 — Transform HDFS raw to Hive gold (PySpark)
# ══════════════════════════════════════════════════════════════════════════════
step_banner "STEP 3 of 3 — Transform to Hive Gold (PySpark)"
STEP_START=$(date +%s)

bash "${SCRIPT_DIR}/run_transform.sh" 2>&1 | tee -a "${MASTER_LOG}"
EXIT_CODE="${PIPESTATUS[0]}"

STEP3_TIME=$(elapsed $STEP_START)

if [ "${EXIT_CODE}" -ne 0 ]; then
    STEP3_STATUS="FAILED"
    log ""
    log "  ✗ STEP 3 FAILED (exit code ${EXIT_CODE})"
    log ""
    log "══════════════════════════════════════════════════════════════"
    log "  PIPELINE SUMMARY — TEXT"
    log "──────────────────────────────────────────────────────────────"
    log "  Step 1 - Generate  : ${STEP1_STATUS} (${STEP1_TIME})"
    log "  Step 2 - Ingest    : ${STEP2_STATUS} (${STEP2_TIME})"
    log "  Step 3 - Transform : ${STEP3_STATUS} (${STEP3_TIME})"
    log "══════════════════════════════════════════════════════════════"
    exit 1
fi

STEP3_STATUS="SUCCESS"

# ── Final Summary ─────────────────────────────────────────────────────────────
TOTAL_TIME=$(elapsed $PIPELINE_START)
log ""
log "══════════════════════════════════════════════════════════════"
log "  PIPELINE SUMMARY — TEXT"
log "──────────────────────────────────────────────────────────────"
log "  Step 1 - Generate  : ${STEP1_STATUS} (${STEP1_TIME})"
log "  Step 2 - Ingest    : ${STEP2_STATUS} (${STEP2_TIME})"
log "  Step 3 - Transform : ${STEP3_STATUS} (${STEP3_TIME})"
log "──────────────────────────────────────────────────────────────"
log "  Total elapsed      : ${TOTAL_TIME}"
log "  Master log         : ${MASTER_LOG}"
log "══════════════════════════════════════════════════════════════"
log "  ✓ TEXT PIPELINE COMPLETED SUCCESSFULLY"
log "══════════════════════════════════════════════════════════════"

exit 0
