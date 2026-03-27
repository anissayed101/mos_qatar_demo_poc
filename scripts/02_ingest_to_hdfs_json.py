#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
02_ingest_to_hdfs_json.py
=========================
Qatar Ministry of Sports Demo - Local JSON Landing to HDFS Raw Ingestion

Purpose:
    Scans local_landing for new .json files, validates they are valid JSON,
    pushes them to HDFS raw path, archives the local file, and records the
    filename in ingested_files_json.log to ensure idempotency.

    Unlike the text pipeline, this script does NOT prepend a metadata header
    inside the file (that would break JSON validity). Instead, metadata is
    recorded in the tracking log as:
        filename|ingest_ts
    The downstream PySpark job reads ingest_ts from there.

Idempotency:
    - Reads metadata/ingested_files_json.log
    - Only processes files NOT already listed there
    - Appends successfully ingested filenames to the log

Usage:
    python3 02_ingest_to_hdfs_json.py [--config /path/to/mos_qatar_json.ini]

Author : BBI.ai Demo Team
Cluster: Cloudera CDP 7.1.9 / Python 3.6.8
"""

import os
import sys
import glob
import json
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
        'config', 'mos_qatar_json.ini'
    )
    parser = argparse.ArgumentParser(description='MOS Qatar JSON HDFS Raw Ingestion')
    parser.add_argument('--config', default=default_config,
                        help='Path to config .ini file')
    return parser.parse_args()


# ─────────────────────────────────────────────────────────────────────────────
# LOGGING
# ─────────────────────────────────────────────────────────────────────────────

def setup_logging(log_dir):
    os.makedirs(log_dir, exist_ok=True)
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    log_file  = os.path.join(log_dir, 'ingest_to_hdfs_json_{}.log'.format(timestamp))
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
# IDEMPOTENCY
# ─────────────────────────────────────────────────────────────────────────────

def load_ingested_files(ingested_log):
    """
    Returns a dict of { filename: ingest_ts } already ingested.
    Log format per line:  filename|ingest_ts
    """
    if not os.path.exists(ingested_log):
        return {}
    result = {}
    with open(ingested_log, 'r') as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            parts = line.split('|', 1)
            if len(parts) == 2:
                result[parts[0]] = parts[1]
            else:
                result[parts[0]] = 'unknown'
    return result


def mark_ingested(ingested_log, filename, ingest_ts):
    """Append filename|ingest_ts to the tracking log."""
    os.makedirs(os.path.dirname(ingested_log), exist_ok=True)
    with open(ingested_log, 'a') as fh:
        fh.write('{}|{}\n'.format(filename, ingest_ts))


# ─────────────────────────────────────────────────────────────────────────────
# HDFS
# ─────────────────────────────────────────────────────────────────────────────

def hdfs_mkdir(logger, hdfs_path):
    cmd    = ['hdfs', 'dfs', '-mkdir', '-p', hdfs_path]
    result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if result.returncode != 0:
        logger.warning("hdfs mkdir warning '{}': {}".format(
            hdfs_path, result.stderr.decode('utf-8').strip()))
    else:
        logger.info("HDFS directory confirmed: {}".format(hdfs_path))


def hdfs_put(logger, local_path, hdfs_dest_path):
    cmd    = ['hdfs', 'dfs', '-put', '-f', local_path, hdfs_dest_path]
    result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if result.returncode != 0:
        raise RuntimeError("hdfs put failed: {}".format(
            result.stderr.decode('utf-8').strip()))
    logger.info("  HDFS put OK: {} -> {}".format(
        os.path.basename(local_path), hdfs_dest_path))


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────

def main():
    args   = parse_args()

    config = configparser.ConfigParser()
    if not os.path.exists(args.config):
        print("ERROR: Config file not found: {}".format(args.config))
        sys.exit(1)
    config.read(args.config)

    landing_dir  = config.get('paths', 'local_landing')
    archive_dir  = config.get('paths', 'local_archive')
    log_dir      = config.get('paths', 'local_logs')
    metadata_dir = config.get('paths', 'local_metadata')
    hdfs_raw     = config.get('hdfs',  'hdfs_raw')
    file_pattern = config.get('files', 'file_pattern')
    archive_flag = config.getboolean('files', 'archive_processed')
    ingested_log = config.get('tracking', 'ingested_log')

    logger = setup_logging(log_dir)
    logger.info("=" * 66)
    logger.info("  MOS Qatar Demo - JSON HDFS Raw Ingestion Started")
    logger.info("=" * 66)
    logger.info("Landing dir    : {}".format(landing_dir))
    logger.info("Archive dir    : {}".format(archive_dir))
    logger.info("HDFS raw path  : {}".format(hdfs_raw))
    logger.info("Tracking log   : {}".format(ingested_log))
    logger.info("Archive flag   : {}".format(archive_flag))
    logger.info("-" * 66)

    os.makedirs(landing_dir,  exist_ok=True)
    os.makedirs(archive_dir,  exist_ok=True)
    os.makedirs(metadata_dir, exist_ok=True)

    hdfs_mkdir(logger, hdfs_raw)

    ingested_files = load_ingested_files(ingested_log)
    logger.info("Previously ingested files : {}".format(len(ingested_files)))

    search_pattern = os.path.join(landing_dir, file_pattern)
    local_files    = sorted(glob.glob(search_pattern))
    logger.info("Files found in landing    : {}".format(len(local_files)))

    if not local_files:
        logger.info("No JSON files to ingest. Exiting cleanly.")
        logger.info("=" * 66)
        return

    processed = 0
    skipped   = 0
    failed    = 0
    ingest_ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    for local_file in local_files:
        filename = os.path.basename(local_file)

        if filename in ingested_files:
            logger.info("SKIP   [already ingested]: {}".format(filename))
            skipped += 1
            continue

        logger.info("INGEST : {}".format(filename))

        try:
            # ── Validate JSON before pushing to HDFS ─────────────────────
            with open(local_file, 'r') as fh:
                content = fh.read()

            try:
                records = json.loads(content)
                if not isinstance(records, list):
                    raise ValueError("Expected a JSON array at root level")
                logger.info("  JSON valid : {} records".format(len(records)))
            except (ValueError, json.JSONDecodeError) as je:
                raise RuntimeError("Invalid JSON in {}: {}".format(filename, str(je)))

            # ── Push to HDFS raw ──────────────────────────────────────────
            hdfs_dest = "{}/{}".format(hdfs_raw, filename)
            hdfs_put(logger, local_file, hdfs_dest)

            # ── Record in tracking log (filename|ingest_ts) ───────────────
            mark_ingested(ingested_log, filename, ingest_ts)

            # ── Archive local file ────────────────────────────────────────
            if archive_flag:
                archive_path = os.path.join(archive_dir, filename)
                shutil.move(local_file, archive_path)
                logger.info("  Archived  : {} -> {}".format(filename, archive_dir))

            processed += 1
            logger.info("  SUCCESS   : {}".format(filename))

        except Exception as exc:
            logger.error("  FAILED    : {} | {}".format(filename, str(exc)))
            failed += 1

    logger.info("=" * 66)
    logger.info("  JSON Ingestion Summary")
    logger.info("-" * 66)
    logger.info("  Processed  : {:>4d}".format(processed))
    logger.info("  Skipped    : {:>4d}".format(skipped))
    logger.info("  Failed     : {:>4d}".format(failed))
    logger.info("=" * 66)

    if failed > 0:
        logger.error("One or more files failed. Exiting with code 1.")
        sys.exit(1)

    logger.info("  JSON HDFS Raw Ingestion Completed Successfully")
    logger.info("=" * 66)


if __name__ == '__main__':
    main()
