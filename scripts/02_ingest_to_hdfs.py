#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
02_ingest_to_hdfs.py
====================
Qatar Ministry of Sports Demo - Local Landing to HDFS Raw Ingestion

Purpose:
    Scans the local landing directory for new .txt files, pushes them to HDFS
    raw path with an inline metadata header, archives the local file, and
    records the filename in a local tracking log to ensure idempotency.

    NOTE: This job uses direct hdfs CLI calls via subprocess. No SparkSession
    is needed for a simple local->HDFS copy. The shell wrapper (run_ingest.sh)
    invokes this via python3 directly. For future Oozie migration, use a
    Shell action pointing to run_ingest.sh.

Idempotency:
    - Reads /opt/mos_qatar_demo/metadata/ingested_files.log
    - Only processes files NOT already listed there
    - Appends successfully ingested filenames to the log
    - Skips and logs any file already ingested

Usage:
    python3 02_ingest_to_hdfs.py [--config /path/to/config.ini]

Author : BBI.ai Demo Team
Cluster: Cloudera CDP 7.1.9 / Python 3.6.8
"""

import os
import sys
import glob
import shutil
import logging
import argparse
import configparser
import subprocess
from datetime import datetime

# ─────────────────────────────────────────────────────────────────────────────
# ARGUMENT PARSING
# ─────────────────────────────────────────────────────────────────────────────

def parse_args():
    default_config = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        'config', 'mos_qatar_demo.ini'
    )
    parser = argparse.ArgumentParser(description='MOS Qatar HDFS Raw Ingestion')
    parser.add_argument('--config', default=default_config,
                        help='Path to config .ini file')
    return parser.parse_args()


# ─────────────────────────────────────────────────────────────────────────────
# LOGGING
# ─────────────────────────────────────────────────────────────────────────────

def setup_logging(log_dir):
    os.makedirs(log_dir, exist_ok=True)
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    log_file  = os.path.join(log_dir, 'ingest_to_hdfs_{}.log'.format(timestamp))
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s [%(levelname)s] %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler(sys.stdout),
        ]
    )
    return logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# IDEMPOTENCY / TRACKING
# ─────────────────────────────────────────────────────────────────────────────

def load_ingested_files(ingested_log):
    """
    Load the set of filenames already pushed to HDFS raw.
    Returns an empty set if the tracking log does not yet exist.
    """
    if not os.path.exists(ingested_log):
        return set()
    with open(ingested_log, 'r') as fh:
        return set(line.strip() for line in fh if line.strip())


def mark_ingested(ingested_log, filename):
    """Append a successfully ingested filename to the tracking log."""
    os.makedirs(os.path.dirname(ingested_log), exist_ok=True)
    with open(ingested_log, 'a') as fh:
        fh.write(filename + '\n')


# ─────────────────────────────────────────────────────────────────────────────
# HDFS OPERATIONS
# ─────────────────────────────────────────────────────────────────────────────

def hdfs_mkdir(logger, hdfs_path):
    """Create HDFS directory, idempotent. Logs warning but does not fail."""
    cmd    = ['hdfs', 'dfs', '-mkdir', '-p', hdfs_path]
    result = subprocess.run(cmd,
                            stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE)
    if result.returncode != 0:
        stderr = result.stderr.decode('utf-8').strip()
        logger.warning("hdfs mkdir warning for '{}': {}".format(hdfs_path, stderr))
    else:
        logger.info("HDFS directory confirmed: {}".format(hdfs_path))


def hdfs_put(logger, local_path, hdfs_dest_path):
    """
    Copy a local file to an HDFS path. Uses -f to overwrite if exists.
    Raises RuntimeError on failure.
    """
    cmd    = ['hdfs', 'dfs', '-put', '-f', local_path, hdfs_dest_path]
    result = subprocess.run(cmd,
                            stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE)
    if result.returncode != 0:
        stderr = result.stderr.decode('utf-8').strip()
        raise RuntimeError("hdfs put failed: {}".format(stderr))
    logger.info("  HDFS put OK: {} -> {}".format(
        os.path.basename(local_path), hdfs_dest_path))


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────

def main():
    args   = parse_args()

    # ── Load config ──────────────────────────────────────────────────────────
    config = configparser.ConfigParser()
    if not os.path.exists(args.config):
        print("ERROR: Config file not found: {}".format(args.config))
        sys.exit(1)
    config.read(args.config)

    landing_dir   = config.get('paths', 'local_landing')
    archive_dir   = config.get('paths', 'local_archive')
    log_dir       = config.get('paths', 'local_logs')
    metadata_dir  = config.get('paths', 'local_metadata')
    hdfs_raw      = config.get('hdfs',  'hdfs_raw')
    file_pattern  = config.get('files', 'file_pattern')
    archive_flag  = config.getboolean('files', 'archive_processed')
    ingested_log  = config.get('tracking', 'ingested_log')

    # ── Logging ──────────────────────────────────────────────────────────────
    logger = setup_logging(log_dir)
    logger.info("=" * 66)
    logger.info("  MOS Qatar Demo - HDFS Raw Ingestion Started")
    logger.info("=" * 66)
    logger.info("Landing dir    : {}".format(landing_dir))
    logger.info("Archive dir    : {}".format(archive_dir))
    logger.info("HDFS raw path  : {}".format(hdfs_raw))
    logger.info("Tracking log   : {}".format(ingested_log))
    logger.info("Archive flag   : {}".format(archive_flag))
    logger.info("-" * 66)

    # ── Ensure local dirs ────────────────────────────────────────────────────
    os.makedirs(landing_dir,  exist_ok=True)
    os.makedirs(archive_dir,  exist_ok=True)
    os.makedirs(metadata_dir, exist_ok=True)

    # ── Ensure HDFS raw dir exists ───────────────────────────────────────────
    hdfs_mkdir(logger, hdfs_raw)

    # ── Load already-ingested filenames ──────────────────────────────────────
    ingested_files = load_ingested_files(ingested_log)
    logger.info("Previously ingested files : {}".format(len(ingested_files)))

    # ── Discover local .txt files ────────────────────────────────────────────
    search_pattern = os.path.join(landing_dir, file_pattern)
    local_files    = sorted(glob.glob(search_pattern))
    logger.info("Files found in landing    : {}".format(len(local_files)))

    if not local_files:
        logger.info("No files to ingest. Exiting cleanly.")
        logger.info("=" * 66)
        logger.info("  HDFS Raw Ingestion Completed (nothing to do)")
        logger.info("=" * 66)
        return

    # ── Process files ─────────────────────────────────────────────────────────
    processed  = 0
    skipped    = 0
    failed     = 0
    ingest_ts  = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    for local_file in local_files:
        filename = os.path.basename(local_file)

        # ── Idempotency: skip already ingested files ──────────────────────
        if filename in ingested_files:
            logger.info("SKIP   [already ingested]: {}".format(filename))
            skipped += 1
            continue

        logger.info("INGEST : {}".format(filename))
        temp_path = os.path.join('/tmp', 'mos_ingest_{}'.format(filename))

        try:
            # ── Prepend inline metadata header to the file content ─────────
            # This header allows the downstream PySpark job (Job 3) to
            # extract source_file and ingest_ts without needing extra metadata
            # files or HDFS reads.
            with open(local_file, 'r') as src:
                original_content = src.read()

            with open(temp_path, 'w') as tmp:
                tmp.write(
                    "##INGEST_METADATA## source_file={} ingest_ts={}\n".format(
                        filename, ingest_ts)
                )
                tmp.write(original_content)

            # ── Push to HDFS raw ───────────────────────────────────────────
            hdfs_dest = "{}/{}".format(hdfs_raw, filename)
            hdfs_put(logger, temp_path, hdfs_dest)

            # ── Remove temp file ───────────────────────────────────────────
            os.remove(temp_path)

            # ── Record in tracking log ─────────────────────────────────────
            mark_ingested(ingested_log, filename)

            # ── Archive local source file ──────────────────────────────────
            if archive_flag:
                archive_path = os.path.join(archive_dir, filename)
                shutil.move(local_file, archive_path)
                logger.info("  Archived  : {} -> {}".format(filename, archive_dir))

            processed += 1
            logger.info("  SUCCESS   : {}".format(filename))

        except Exception as exc:
            logger.error("  FAILED    : {} | {}".format(filename, str(exc)))
            # Remove temp file if it was partially written
            if os.path.exists(temp_path):
                try:
                    os.remove(temp_path)
                except OSError:
                    pass
            failed += 1

    # ── Summary ───────────────────────────────────────────────────────────────
    logger.info("=" * 66)
    logger.info("  Ingestion Summary")
    logger.info("-" * 66)
    logger.info("  Processed  : {:>4d}".format(processed))
    logger.info("  Skipped    : {:>4d}".format(skipped))
    logger.info("  Failed     : {:>4d}".format(failed))
    logger.info("=" * 66)

    if failed > 0:
        logger.error("One or more files failed. Exiting with code 1.")
        sys.exit(1)

    logger.info("  HDFS Raw Ingestion Completed Successfully")
    logger.info("=" * 66)


if __name__ == '__main__':
    main()
