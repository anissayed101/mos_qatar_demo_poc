#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
03_transform_to_gold_json.py
============================
Qatar Ministry of Sports Demo - HDFS JSON Raw to Gold Transformation (PySpark)

Purpose:
    1. Lists .json files in HDFS raw JSON path.
    2. Skips files already tracked in processed_files_json.log.
    3. For each new file: reads JSON array, parses all records into 17
       structured fields (including nested metadata fields).
    4. Writes Parquet to HDFS gold JSON path (partitioned by event_date).
    5. Inserts/appends into Hive gold JSON table.
    6. Records processed filename in processed_files_json.log.

Output fields (from JSON record + nested metadata):
    event_id, event_name, sport_type, venue, city, country,
    event_date (partition), team_a, team_b, status, attendance,
    meta_source_system, meta_priority, meta_broadcast,
    meta_category, meta_referee, meta_notes,
    source_file, ingest_ts, processing_ts

Ingest_ts is read from ingested_files_json.log (filename|ingest_ts format).

Usage:
    spark3-submit \\
        --master yarn --deploy-mode client \\
        --executor-memory 1g --executor-cores 1 --num-executors 2 \\
        --conf "spark.yarn.appMasterEnv.PYSPARK_PYTHON=/usr/bin/python3" \\
        --conf "spark.executorEnv.PYSPARK_PYTHON=/usr/bin/python3" \\
        /home/cloudera/mos_qatar_demo_poc/scripts/03_transform_to_gold_json.py \\
        --config /home/cloudera/mos_qatar_demo_poc/config/mos_qatar_json.ini

Author : BBI.ai Demo Team
Cluster: Cloudera CDP 7.1.9 / PySpark 3.3.2 / Python 3.6.8
"""

import os
import sys
import json
import logging
import argparse
import configparser
import traceback
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField,
    StringType, LongType, BooleanType
)


# ─────────────────────────────────────────────────────────────────────────────
# ARGUMENT PARSING
# ─────────────────────────────────────────────────────────────────────────────

def parse_args():
    default_config = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        'config', 'mos_qatar_json.ini'
    )
    parser = argparse.ArgumentParser(description='MOS Qatar JSON Gold Transformation')
    parser.add_argument('--config', default=default_config,
                        help='Path to config .ini file')
    return parser.parse_args()


# ─────────────────────────────────────────────────────────────────────────────
# LOGGING
# ─────────────────────────────────────────────────────────────────────────────

def setup_logging(log_dir):
    os.makedirs(log_dir, exist_ok=True)
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    log_file  = os.path.join(log_dir, 'transform_to_gold_json_{}.log'.format(timestamp))
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

def load_processed_files(processed_log):
    if not os.path.exists(processed_log):
        return set()
    with open(processed_log, 'r') as fh:
        return set(line.strip() for line in fh if line.strip())


def mark_processed(processed_log, filename):
    os.makedirs(os.path.dirname(processed_log), exist_ok=True)
    with open(processed_log, 'a') as fh:
        fh.write(filename + '\n')


def load_ingest_ts_map(ingested_log):
    """
    Read the JSON ingestion tracking log to get ingest_ts per filename.
    Log format: filename|ingest_ts
    Returns dict { filename: ingest_ts }
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
    return result


# ─────────────────────────────────────────────────────────────────────────────
# HDFS FILE LISTING via Hadoop FileSystem Java API
# ─────────────────────────────────────────────────────────────────────────────

def list_hdfs_files(spark, logger, hdfs_dir, suffix='.json'):
    """
    List files in HDFS directory using PySpark's Java gateway.
    Returns sorted list of absolute HDFS paths ending with suffix.
    """
    try:
        sc         = spark.sparkContext
        URI        = sc._jvm.java.net.URI
        Path       = sc._jvm.org.apache.hadoop.fs.Path
        FileSystem = sc._jvm.org.apache.hadoop.fs.FileSystem

        fs       = FileSystem.get(URI(hdfs_dir), sc._jsc.hadoopConfiguration())
        statuses = fs.listStatus(Path(hdfs_dir))

        files = []
        for status in statuses:
            path_str  = status.getPath().toString()
            uri_obj   = URI(path_str)
            hdfs_path = uri_obj.getPath()
            if hdfs_path.endswith(suffix):
                files.append(hdfs_path)

        logger.info("HDFS files found in '{}': {}".format(hdfs_dir, len(files)))
        return sorted(files)

    except Exception as exc:
        logger.error("Hadoop FS listing failed for '{}': {}".format(hdfs_dir, str(exc)))
        return []


# ─────────────────────────────────────────────────────────────────────────────
# GOLD SCHEMA
# Column order MUST match Hive table exactly.
# Partition column (event_date) is LAST.
# ─────────────────────────────────────────────────────────────────────────────

GOLD_SCHEMA = StructType([
    StructField("event_id",           StringType(),  True),
    StructField("event_name",         StringType(),  True),
    StructField("sport_type",         StringType(),  True),
    StructField("venue",              StringType(),  True),
    StructField("city",               StringType(),  True),
    StructField("country",            StringType(),  True),
    StructField("team_a",             StringType(),  True),
    StructField("team_b",             StringType(),  True),
    StructField("status",             StringType(),  True),
    StructField("attendance",         LongType(),    True),
    StructField("meta_source_system", StringType(),  True),
    StructField("meta_priority",      StringType(),  True),
    StructField("meta_broadcast",     StringType(),  True),   # stored as string for Hive compat
    StructField("meta_category",      StringType(),  True),
    StructField("meta_referee",       StringType(),  True),
    StructField("meta_notes",         StringType(),  True),
    StructField("source_file",        StringType(),  True),
    StructField("ingest_ts",          StringType(),  True),
    StructField("processing_ts",      StringType(),  True),
    StructField("event_date",         StringType(),  True),   # partition col — LAST
])


# ─────────────────────────────────────────────────────────────────────────────
# JSON RECORD PARSING
# ─────────────────────────────────────────────────────────────────────────────

def parse_json_record(rec, filename, ingest_ts, processing_ts):
    """
    Parse a single JSON event dict into a tuple matching GOLD_SCHEMA.
    Handles missing keys gracefully with defaults.
    Returns tuple or None if record is completely invalid.
    """
    if not isinstance(rec, dict):
        return None

    meta = rec.get('metadata', {})
    if not isinstance(meta, dict):
        meta = {}

    # Safe attendance cast
    try:
        attendance = int(rec.get('attendance', 0))
    except (ValueError, TypeError):
        attendance = 0

    # Safe event_date validation
    event_date = str(rec.get('event_date', '1900-01-01')).strip()
    if len(event_date) != 10:
        event_date = '1900-01-01'

    # broadcast is bool in JSON — store as string for Hive STRING column
    broadcast_raw = meta.get('broadcast', False)
    broadcast_str = 'true' if broadcast_raw is True else 'false'

    return (
        str(rec.get('event_id',   '')),
        str(rec.get('event_name', '')),
        str(rec.get('sport',      '')),       # JSON key is 'sport', column is sport_type
        str(rec.get('venue',      '')),
        str(rec.get('city',       '')),
        str(rec.get('country',    '')),
        str(rec.get('team_a',     '')),
        str(rec.get('team_b',     '')),
        str(rec.get('status',     '')),
        attendance,
        str(meta.get('source_system', '')),
        str(meta.get('priority',      '')),
        broadcast_str,
        str(meta.get('category',      '')),
        str(meta.get('referee',       '')),
        str(meta.get('notes',         '')),
        filename,
        ingest_ts,
        processing_ts,
        event_date,                            # partition col — LAST
    )


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────

def main():
    args = parse_args()

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
    ingested_log   = config.get('tracking', 'ingested_log')

    logger = setup_logging(log_dir)
    logger.info("=" * 66)
    logger.info("  MOS Qatar Demo - JSON Raw to Gold Transformation Started")
    logger.info("=" * 66)
    logger.info("HDFS raw path    : {}".format(hdfs_raw))
    logger.info("Hive target      : {}.{}".format(database, gold_table))
    logger.info("Processed log    : {}".format(processed_log))
    logger.info("-" * 66)

    os.makedirs(metadata_dir, exist_ok=True)

    # ── Initialise SparkSession before any HDFS operation ─────────────────────
    logger.info("Initialising SparkSession...")
    spark = (
        SparkSession.builder
        .appName(app_name)
        .config("hive.metastore.uris",               metastore_uris)
        .config("hive.exec.dynamic.partition",        "true")
        .config("hive.exec.dynamic.partition.mode",   "nonstrict")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.sql.hive.convertMetastoreParquet", "false")
        .enableHiveSupport()
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    logger.info("SparkSession ready. App ID: {}".format(
        spark.sparkContext.applicationId))

    processed_files = load_processed_files(processed_log)
    logger.info("Previously processed files : {}".format(len(processed_files)))

    ingest_ts_map = load_ingest_ts_map(ingested_log)
    logger.info("Ingest TS entries loaded   : {}".format(len(ingest_ts_map)))

    # ── List HDFS raw JSON files ───────────────────────────────────────────────
    hdfs_files = list_hdfs_files(spark, logger, hdfs_raw, suffix='.json')

    if not hdfs_files:
        logger.info("No .json files found in HDFS raw. Exiting cleanly.")
        spark.stop()
        return

    new_files = []
    for hdfs_path in hdfs_files:
        fname = os.path.basename(hdfs_path)
        if fname in processed_files:
            logger.info("SKIP   [already processed]: {}".format(fname))
        else:
            logger.info("QUEUED [to be processed]  : {}".format(fname))
            new_files.append(hdfs_path)

    if not new_files:
        logger.info("All JSON raw files already processed. Nothing to do.")
        spark.stop()
        return

    logger.info("-" * 66)
    logger.info("Files to process now : {}".format(len(new_files)))

    processing_ts   = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    total_records   = 0
    files_processed = 0
    files_failed    = 0

    for hdfs_file_path in new_files:
        filename = os.path.basename(hdfs_file_path)
        logger.info("-" * 66)
        logger.info("Processing : {}".format(filename))

        try:
            full_uri  = "{}{}".format(namenode_uri, hdfs_file_path)
            ingest_ts = ingest_ts_map.get(filename, 'unknown')
            logger.info("  Reading  : {}".format(full_uri))
            logger.info("  Ingest TS: {}".format(ingest_ts))

            # ── Read JSON file as single string, parse on driver ──────────
            # For small demo files this is fine. The entire file is one
            # JSON array so we read it as a text file and parse with json.
            raw_lines   = spark.sparkContext.textFile(full_uri).collect()
            raw_content = '\n'.join(raw_lines)

            try:
                records_json = json.loads(raw_content)
                if not isinstance(records_json, list):
                    raise ValueError("Root element must be a JSON array")
            except (ValueError, Exception) as je:
                raise RuntimeError("JSON parse error in {}: {}".format(filename, str(je)))

            logger.info("  JSON records : {}".format(len(records_json)))

            # ── Parse each record into a tuple matching GOLD_SCHEMA ───────
            rows         = []
            parse_errors = 0

            for rec in records_json:
                row = parse_json_record(rec, filename, ingest_ts, processing_ts)
                if row is None:
                    parse_errors += 1
                    logger.warning("  Skipped invalid record: {}".format(str(rec)[:80]))
                    continue
                rows.append(row)

            logger.info("  Parsed OK    : {}".format(len(rows)))
            if parse_errors > 0:
                logger.warning("  Parse errors : {}".format(parse_errors))

            if not rows:
                logger.warning("  No valid records. Skipping file.")
                files_failed += 1
                continue

            # ── Create DataFrame and insert into Hive ─────────────────────
            df = spark.createDataFrame(rows, schema=GOLD_SCHEMA)
            df.cache()
            record_count = df.count()
            logger.info("  DataFrame rows : {}".format(record_count))

            logger.info("  Sample (2 rows):")
            for row in df.limit(2).collect():
                logger.info("    {}".format(row))

            hive_target = "{}.{}".format(database, gold_table)
            logger.info("  Inserting into : {}".format(hive_target))
            df.write.mode("append").insertInto(hive_target)
            logger.info("  Insert complete.")

            df.unpersist()
            mark_processed(processed_log, filename)

            total_records   += record_count
            files_processed += 1
            logger.info("  SUCCESS: {} ({} records)".format(filename, record_count))

        except Exception as exc:
            logger.error("  FAILED: {} | {}".format(filename, str(exc)))
            logger.error(traceback.format_exc())
            files_failed += 1

    # ── Refresh Hive partitions ────────────────────────────────────────────────
    if files_processed > 0:
        try:
            repair_sql = "MSCK REPAIR TABLE {}.{}".format(database, gold_table)
            logger.info("Running: {}".format(repair_sql))
            spark.sql(repair_sql)
            logger.info("Hive partition repair complete.")
        except Exception as exc:
            logger.warning("MSCK REPAIR warning (non-fatal): {}".format(str(exc)))

    spark.stop()

    logger.info("=" * 66)
    logger.info("  JSON Transformation Summary")
    logger.info("-" * 66)
    logger.info("  Files processed  : {:>4d}".format(files_processed))
    logger.info("  Files failed     : {:>4d}".format(files_failed))
    logger.info("  Total records    : {:>6d}".format(total_records))
    logger.info("=" * 66)

    if files_failed > 0:
        logger.error("One or more files failed. Exiting with code 1.")
        sys.exit(1)

    logger.info("  JSON Raw to Gold Transformation Completed Successfully")
    logger.info("=" * 66)


if __name__ == '__main__':
    main()
