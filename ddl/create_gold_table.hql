-- ==============================================================
-- create_gold_table.hql
-- Qatar Ministry of Sports Demo
-- Hive External Gold Table: sports_events_gold
--
-- Purpose:
--   Stores structured parsed sports event records written by Job 3 as
--   Parquet files partitioned by event_date.
--
-- Column order MUST match GOLD_SCHEMA in 03_transform_to_gold.py exactly.
-- Partition column (event_date) is always last in the non-partition column list.
--
-- Notes:
--   - EXTERNAL: dropping this table will NOT delete HDFS parquet data.
--   - STORED AS PARQUET with SNAPPY compression for efficient columnar reads.
--   - Partitioned by event_date for efficient date-range queries in demos.
-- ==============================================================

USE mos_qatar_demo;

DROP TABLE IF EXISTS sports_events_gold;

CREATE EXTERNAL TABLE sports_events_gold (
    event_id        STRING  COMMENT 'Unique event identifier (e.g. EVT3827)',
    event_name      STRING  COMMENT 'Full name of the sports event',
    sport_type      STRING  COMMENT 'Type of sport (Football, Athletics, etc.)',
    venue           STRING  COMMENT 'Name of the event venue',
    city            STRING  COMMENT 'Host city',
    country         STRING  COMMENT 'Host country',
    team_a          STRING  COMMENT 'First participating team or squad',
    team_b          STRING  COMMENT 'Second participating team or squad',
    status          STRING  COMMENT 'Event status: Scheduled / Confirmed / Completed / Postponed / Cancelled',
    attendance      BIGINT  COMMENT 'Expected or actual attendance figure',
    source_file     STRING  COMMENT 'Source .txt filename this record originated from',
    ingest_ts       STRING  COMMENT 'Timestamp when source file was ingested to HDFS raw (from Job 2)',
    processing_ts   STRING  COMMENT 'Timestamp when this record was parsed and written to gold (Job 3)'
)
COMMENT 'Gold structured sports events table - Qatar Ministry of Sports Demo (BBI.ai)'
PARTITIONED BY (
    event_date      STRING  COMMENT 'Event date partition key (YYYY-MM-DD format)'
)
STORED AS PARQUET
LOCATION 'hdfs://cdp-master1.cloudera.bbi:8020/data/mos_qatar_demo/text_files/gold'
TBLPROPERTIES (
    'parquet.compression' = 'SNAPPY'
);

-- Validate
DESCRIBE FORMATTED sports_events_gold;
SHOW PARTITIONS sports_events_gold;
