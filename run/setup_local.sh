#!/bin/bash
# ==============================================================
# setup_local.sh
# Qatar Ministry of Sports Demo - One-Time Local Linux Setup
#
# Creates all required local directories under /opt/mos_qatar_demo,
# sets ownership to cloudera:cloudera, and initialises tracking logs.
#
# Run ONCE as root (or via sudo) on cdp-master1 (node 131):
#   sudo bash /opt/mos_qatar_demo/run/setup_local.sh
#
# After this, all runtime jobs run as user 'cloudera'.
# ==============================================================

set -euo pipefail

BASE_DIR="/opt/mos_qatar_demo"
OWNER="cloudera"
GROUP="cloudera"

echo "======================================================"
echo "  MOS Qatar Demo - Local Linux Setup"
echo "  Base : ${BASE_DIR}"
echo "  Owner: ${OWNER}:${GROUP}"
echo "======================================================"

# ── Create directory structure ─────────────────────────────────────────────
echo ""
echo "[1/4] Creating directory structure..."

for DIR in \
    "${BASE_DIR}/config" \
    "${BASE_DIR}/scripts" \
    "${BASE_DIR}/ddl" \
    "${BASE_DIR}/run" \
    "${BASE_DIR}/landing" \
    "${BASE_DIR}/archive" \
    "${BASE_DIR}/logs" \
    "${BASE_DIR}/metadata"; do

    mkdir -p "${DIR}"
    echo "  Created: ${DIR}"
done

# ── Ownership ──────────────────────────────────────────────────────────────
echo ""
echo "[2/4] Setting ownership to ${OWNER}:${GROUP}..."
chown -R "${OWNER}:${GROUP}" "${BASE_DIR}"
echo "  Done."

# ── Permissions ────────────────────────────────────────────────────────────
echo ""
echo "[3/4] Setting permissions..."
chmod -R 755 "${BASE_DIR}"
# Write access on runtime dirs for cloudera user
chmod -R 775 "${BASE_DIR}/landing"
chmod -R 775 "${BASE_DIR}/archive"
chmod -R 775 "${BASE_DIR}/logs"
chmod -R 775 "${BASE_DIR}/metadata"
echo "  Done."

# ── Make shell scripts executable ──────────────────────────────────────────
if ls "${BASE_DIR}/scripts/"*.sh >/dev/null 2>&1; then
    chmod +x "${BASE_DIR}/scripts/"*.sh
    echo "  Shell scripts in scripts/ made executable."
fi
if ls "${BASE_DIR}/run/"*.sh >/dev/null 2>&1; then
    chmod +x "${BASE_DIR}/run/"*.sh
    echo "  Shell scripts in run/ made executable."
fi

# ── Initialise tracking logs (empty) ──────────────────────────────────────
echo ""
echo "[4/4] Initialising tracking log files..."
touch "${BASE_DIR}/metadata/ingested_files.log"
touch "${BASE_DIR}/metadata/processed_files.log"
chown "${OWNER}:${GROUP}" "${BASE_DIR}/metadata/"*.log
echo "  ${BASE_DIR}/metadata/ingested_files.log"
echo "  ${BASE_DIR}/metadata/processed_files.log"

# ── Final check ────────────────────────────────────────────────────────────
echo ""
echo "======================================================"
echo "  Local setup completed successfully!"
echo ""
echo "  Directory listing:"
ls -la "${BASE_DIR}/"
echo "======================================================"
