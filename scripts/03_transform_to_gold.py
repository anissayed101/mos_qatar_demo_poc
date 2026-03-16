#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
03_transform_to_gold.py
=======================
Qatar Ministry of Sports Demo - HDFS Raw to Gold Transformation (PySpark)

Purpose:
    1. Lists .txt files in HDFS raw path.
    2. Skips files already tracked in the local processed_files.log.
    3. For each new file: reads raw text, parses pipe-delimited labeled records
       into 14 structured fields, builds a Spark DataFrame.
    4. Writes the DataFrame as Parquet to HDFS gold path (partitioned by event_date).
    5. Appends rows into the Hive gold table (mos_qatar_demo.sports_events_gold).
    6. Records the processed filename in local processed_files.log.

Idempotency:
    - Reads /opt/mos_qatar_demo/metadata/processed_files.log
    - Only processes HDFS raw filenames NOT already listed there
    - Appends successfully processed filenames to the log after each file

Record format parsed:
    Event ID: EVT1001 | Event Name: Gulf Youth Cup Round 1 | Sport: Football |
    Venue: Lusail Stadium | City: Doha | Country: Qatar |
    Event Date: 2026-03-20 | Team A: Qatar U21 | Team B: Jordan U21 |
    Status: Scheduled | Attendance: 42000

Output fields:
    event_id, event_name, sport_type, venue, city, country,
    event_date (partition), team_a, team_b, status, attendance,
    source_file, ingest_ts, processing_ts

Usage:
    spark3-submit \\
        --master yarn \\
        --deploy-mode client \\
        --executor-memory 1g \\
        --executor-cores 1 \\
        --num-executors 2 \\
        --conf "spark.yarn.appMasterEnv.PYSPARK_PYTHON=/usr/bin/python3" \\
        --conf "spark.executorEnv.PYSPARK_PYTHON=/usr/bin/python3" \\
        /opt/mos_qatar_demo/scripts/03_transform_to_gold.py \\
        --config /opt/mos_qatar_demo/config/mos_qatar_demo.ini

For Oozie migration: use a Spark action with the spark3-submit parameters above.

Author : BBI.ai Demo Team
Cluster: Cloudera CDP 7.1.9 / PySpark 3.3.2 / Python 3.6.8
"""

import os
import sys
import logging
import argparse
import configparser
import subprocess
import traceback
from datetime import datetime

from pyspark.sql import SparkSession, Row
from pyspark.sql.types import (
    StructType, StructField,
    StringType, LongType
)


# ─────────────────────────────────────────────────────────────────────────────
# ARGUMENT PARSING
# ─────────────────────────────────────────────────────────────────────────────

def parse_args():
    default_config = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        'config', 'mos_qatar_demo.ini'
    )
    parser = argparse.ArgumentParser(description='MOS Qatar Gold Transformation')
    parser.add_argument('--config', default=default_config,
                        help='Path to config .ini file')
    return parser.parse_args()


# ─────────────────────────────────────────────────────────────────────────────
# LOGGING  (driver-side only — Spark executor logs go to YARN)
# ─────────────────────────────────────────────────────────────────────────────

def setup_logging(log_dir):
    os.makedirs(log_dir, exist_ok=True)
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    log_file  = os.path.join(log_dir, 'transform_to_gold_{}.log'.format(timestamp))
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

def load_processed_files(processed_log):
    """Return set of HDFS raw filenames already transformed to gold."""
    if not os.path.exists(processed_log):
        return set()
    with open(processed_log, 'r') as fh:
        return set(line.strip() for line in fh if line.strip())


def mark_processed(processed_log, filename):
    """Append a successfully processed filename to the tracking log."""
    os.makedirs(os.path.dirname(processed_log), exist_ok=True)
    with open(processed_log, 'a') as fh:
        fh.write(filename + '\n')


# ─────────────────────────────────────────────────────────────────────────────
# HDFS FILE LISTING
# ─────────────────────────────────────────────────────────────────────────────

def list_hdfs_files(logger, hdfs_dir, suffix='.txt'):
    """
    List files in an HDFS directory using the hdfs CLI.
    Returns a sorted list of absolute HDFS paths that end with 'suffix'.

    hdfs dfs -ls output format:
        -rw-r--r--   3 cloudera supergroup  1234 2026-03-16 12:00 /path/to/file.txt
    Fields: perms replication owner group size date time path  (index 0-7)
    """
    cmd    = ['hdfs', 'dfs', '-ls', hdfs_dir]
    result = subprocess.run(cmd,
                            stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE)

    if result.returncode != 0:
        stderr = result.stderr.decode('utf-8').strip()
        logger.error("hdfs ls failed for '{}': {}".format(hdfs_dir, stderr))
        return []

    stdout = result.stdout.decode('utf-8')
    files  = []

    for line in stdout.strip().split('\n'):
        parts = line.split()
        # File lines start with '-' (not 'd' for directory)
        if len(parts) >= 8 and parts[0].startswith('-'):
            hdfs_path = parts[7]
            if hdfs_path.endswith(suffix):
                files.append(hdfs_path)

    logger.info("HDFS files found in '{}': {}".format(hdfs_dir, len(files)))
    return sorted(files)


# ─────────────────────────────────────────────────────────────────────────────
# RECORD PARSING
# ─────────────────────────────────────────────────────────────────────────────

def parse_record_line(line):
    """
    Parse a single pipe-delimited labeled event record line into a dict.

    Input:
        Event ID: EVT1001 | Event Name: Gulf Youth Cup Round 1 | Sport: Football |
        Venue: Lusail Stadium | City: Doha | Country: Qatar |
        Event Date: 2026-03-20 | Team A: Qatar U21 | Team B: Jordan U21 |
        Status: Scheduled | Attendance: 42000

    Returns:
        dict with keys: event_id, event_name, sport_type, venue, city, country,
                        event_date, team_a, team_b, status, attendance
        OR None if the line is a metadata header, blank, or unparseable.
    """
    line = line.strip()

    # ── Skip blank lines and metadata headers ─────────────────────────────
    if not line:
        return None
    if line.startswith("##INGEST_METADATA##"):
        return None
    if 'Event ID:' not in line:
        return None

    # ── Parse key: value pairs separated by ' | ' ─────────────────────────
    pairs = {}
    for segment in line.split(' | '):
        if ': ' in segment:
            key, val = segment.split(': ', 1)
            pairs[key.strip()] = val.strip()

    # ── Require at minimum an Event ID field ──────────────────────────────
    if 'Event ID' not in pairs:
        return None

    # ── Safely cast attendance to integer ─────────────────────────────────
    attendance_raw = pairs.get('Attendance', '0').replace(',', '').strip()
    try:
        attendance = int(attendance_raw)
    except ValueError:
        attendance = 0

    # ── Default event_date fallback (avoids null partition) ───────────────
    event_date = pairs.get('Event Date', '1900-01-01').strip()
    if not event_date or len(event_date) != 10:
        event_date = '1900-01-01'

    return {
        'event_id':   pairs.get('Event ID',    ''),
        'event_name': pairs.get('Event Name',  ''),
        'sport_type': pairs.get('Sport',       ''),
        'venue':      pairs.get('Venue',       ''),
        'city':       pairs.get('City',        ''),
        'country':    pairs.get('Country',     ''),
        'event_date': event_date,
        'team_a':     pairs.get('Team A',      ''),
        'team_b':     pairs.get('Team B',      ''),
        'status':     pairs.get('Status',      ''),
        'attendance': attendance,
    }


def extract_ingest_ts(lines):
    """
    Extract ingest_ts value from the ##INGEST_METADATA## header line.
    Header format: ##INGEST_METADATA## source_file=xxx.txt ingest_ts=YYYY-MM-DD HH:MM:SS
    Returns 'unknown' if not found.
    """
    for line in lines:
        if line.strip().startswith("##INGEST_METADATA##"):
            try:
                return line.split('ingest_ts=', 1)[1].strip()
            except IndexError:
                pass
    return 'unknown'


# ─────────────────────────────────────────────────────────────────────────────
# GOLD SCHEMA
# Column order here MUST match the Hive table column order exactly.
# Partition column (event_date) is LAST — required for insertInto().
# ─────────────────────────────────────────────────────────────────────────────

GOLD_SCHEMA = StructType([
    StructField("event_id",      StringType(), True),
    StructField("event_name",    StringType(), True),
    StructField("sport_type",    StringType(), True),
    StructField("venue",         StringType(), True),
    StructField("city",          StringType(), True),
    StructField("country",       StringType(), True),
    StructField("team_a",        StringType(), True),
    StructField("team_b",        StringType(), True),
    StructField("status",        StringType(), True),
    StructField("attendance",    LongType(),   True),
    StructField("source_file",   StringType(), True),
    StructField("ingest_ts",     StringType(), True),
    StructField("processing_ts", StringType(), True),
    StructField("event_date",    StringType(), True),   # <-- partition column, must be last
])


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────

def main():
    args = parse_args()

    # ── Load config ──────────────────────────────────────────────────────────
    config = configparser.ConfigParser()
    if not os.path.exists(args.config):
        print("ERROR: Config file not found: {}".format(args.config))
        sys.exit(1)
    config.read(args.config)

    log_dir        = config.get('paths',    'local_logs')
    metadata_dir   = config.get('paths',    'local_metadata')
    hdfs_raw       = config.get('hdfs',     'hdfs_raw')
    namenode_uri   = config.get('hdfs',     'namenode_uri')
    metastore_uris = config.get('hive',     'metastore_uris')
    database       = config.get('hive',     'database')
    gold_table     = config.get('hive',     'gold_table')
    app_name       = config.get('spark',    'app_name_transform')
    processed_log  = config.get('tracking', 'processed_log')

    # ── Logging (driver) ─────────────────────────────────────────────────────
    logger = setup_logging(log_dir)
    logger.info("=" * 66)
    logger.info("  MOS Qatar Demo - Raw to Gold Transformation Started")
    logger.info("=" * 66)
    logger.info("HDFS raw path    : {}".format(hdfs_raw))
    logger.info("Hive target      : {}.{}".format(database, gold_table))
    logger.info("Namenode URI     : {}".format(namenode_uri))
    logger.info("Processed log    : {}".format(processed_log))
    logger.info("-" * 66)

    # ── Ensure local metadata dir ─────────────────────────────────────────────
    os.makedirs(metadata_dir, exist_ok=True)

    # ── Load already-processed filenames ──────────────────────────────────────
    processed_files = load_processed_files(processed_log)
    logger.info("Previously processed HDFS files: {}".format(len(processed_files)))

    # ── List HDFS raw files ────────────────────────────────────────────────────
    hdfs_files = list_hdfs_files(logger, hdfs_raw, suffix='.txt')

    if not hdfs_files:
        logger.info("No .txt files found in HDFS raw. Exiting cleanly.")
        logger.info("=" * 66)
        return

    # ── Separate new from already-processed files ──────────────────────────────
    new_files = []
    for hdfs_path in hdfs_files:
        fname = os.path.basename(hdfs_path)
        if fname in processed_files:
            logger.info("SKIP   [already processed]: {}".format(fname))
        else:
            logger.info("QUEUED [to be processed]  : {}".format(fname))
            new_files.append(hdfs_path)

    if not new_files:
        logger.info("All HDFS raw files already processed. Nothing to do.")
        logger.info("=" * 66)
        return

    logger.info("-" * 66)
    logger.info("Files to process now : {}".format(len(new_files)))

    # ── Initialise SparkSession ────────────────────────────────────────────────
    logger.info("Initialising SparkSession with Hive support...")
    spark = (
        SparkSession.builder
        .appName(app_name)
        .config("hive.metastore.uris",               metastore_uris)
        .config("hive.exec.dynamic.partition",        "true")
        .config("hive.exec.dynamic.partition.mode",   "nonstrict")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        # Use Hive's own Parquet writer for full external-table compatibility
        .config("spark.sql.hive.convertMetastoreParquet", "false")
        .enableHiveSupport()
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    logger.info("SparkSession ready. App ID: {}".format(
        spark.sparkContext.applicationId))

    processing_ts   = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    total_records   = 0
    files_processed = 0
    files_failed    = 0

    # ── Process each new file ─────────────────────────────────────────────────
    for hdfs_file_path in new_files:
        filename = os.path.basename(hdfs_file_path)
        logger.info("-" * 66)
        logger.info("Processing : {}".format(filename))

        try:
            # ── Build full HDFS URI for Spark read ───────────────────────
            # namenode_uri  = hdfs://cdp-master1.cloudera.bbi:8020
            # hdfs_file_path = /data/mos_qatar_demo/.../filename.txt
            full_uri = "{}{}".format(namenode_uri, hdfs_file_path)
            logger.info("  Reading  : {}".format(full_uri))

            # ── Read raw lines from HDFS ─────────────────────────────────
            # .collect() brings lines to the driver; acceptable for small
            # text files (a few KB each in this demo).
            raw_lines = spark.sparkContext.textFile(full_uri).collect()
            logger.info("  Lines    : {}".format(len(raw_lines)))

            # ── Extract ingest timestamp from metadata header ─────────────
            ingest_ts = extract_ingest_ts(raw_lines)
            logger.info("  Ingest TS: {}".format(ingest_ts))

            # ── Parse records into structured tuples ──────────────────────
            # Tuple column order must match GOLD_SCHEMA exactly.
            rows         = []
            parse_errors = 0

            for line in raw_lines:
                parsed = parse_record_line(line)
                if parsed is None:
                    # Log only non-blank, non-metadata unparseable lines
                    stripped = line.strip()
                    if stripped and not stripped.startswith("##INGEST_METADATA##"):
                        logger.warning("  Unparseable line: {}".format(stripped[:80]))
                        parse_errors += 1
                    continue

                # Build tuple matching GOLD_SCHEMA column order
                rows.append((
                    parsed['event_id'],
                    parsed['event_name'],
                    parsed['sport_type'],
                    parsed['venue'],
                    parsed['city'],
                    parsed['country'],
                    parsed['team_a'],
                    parsed['team_b'],
                    parsed['status'],
                    parsed['attendance'],    # LongType
                    filename,                # source_file
                    ingest_ts,               # ingest_ts
                    processing_ts,           # processing_ts
                    parsed['event_date'],    # event_date  <-- partition col (last)
                ))

            logger.info("  Parsed OK: {}".format(len(rows)))
            if parse_errors > 0:
                logger.warning("  Parse errors: {}".format(parse_errors))

            if not rows:
                logger.warning("  No valid records found. Skipping this file.")
                files_failed += 1
                continue

            # ── Create Spark DataFrame ────────────────────────────────────
            df = spark.createDataFrame(rows, schema=GOLD_SCHEMA)
            df.cache()
            record_count = df.count()
            logger.info("  DataFrame rows   : {}".format(record_count))

            # ── Show a sample in the log (first 3 rows) ───────────────────
            logger.info("  Sample (3 rows)  :")
            for row in df.limit(3).collect():
                logger.info("    {}".format(row))

            # ── Insert into Hive gold table ───────────────────────────────
            # insertInto writes to the table's LOCATION as Parquet (because
            # the table is STORED AS PARQUET in the DDL).
            # Dynamic partitioning by event_date is enabled above.
            hive_target = "{}.{}".format(database, gold_table)
            logger.info("  Inserting into   : {}".format(hive_target))

            df.write.mode("append").insertInto(hive_target)

            logger.info("  Insert complete.")

            df.unpersist()

            # ── Mark file as processed in local tracking log ──────────────
            mark_processed(processed_log, filename)

            total_records   += record_count
            files_processed += 1
            logger.info("  SUCCESS: {} ({} records written)".format(
                filename, record_count))

        except Exception as exc:
            logger.error("  FAILED: {} | {}".format(filename, str(exc)))
            logger.error(traceback.format_exc())
            files_failed += 1

    # ── Refresh Hive partition metadata ───────────────────────────────────────
    if files_processed > 0:
        try:
            repair_sql = "MSCK REPAIR TABLE {}.{}".format(database, gold_table)
            logger.info("Running: {}".format(repair_sql))
            spark.sql(repair_sql)
            logger.info("Hive partition repair complete.")
        except Exception as exc:
            logger.warning("MSCK REPAIR TABLE warning (non-fatal): {}".format(str(exc)))

    # ── Stop Spark ────────────────────────────────────────────────────────────
    spark.stop()

    # ── Final summary ─────────────────────────────────────────────────────────
    logger.info("=" * 66)
    logger.info("  Transformation Summary")
    logger.info("-" * 66)
    logger.info("  Files processed  : {:>4d}".format(files_processed))
    logger.info("  Files failed     : {:>4d}".format(files_failed))
    logger.info("  Total records    : {:>6d}".format(total_records))
    logger.info("=" * 66)

    if files_failed > 0:
        logger.error("One or more files failed. Exiting with code 1.")
        sys.exit(1)

    logger.info("  Raw to Gold Transformation Completed Successfully")
    logger.info("=" * 66)


if __name__ == '__main__':
    main()
