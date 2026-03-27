-- ==============================================================
-- create_gold_json_table.hql
-- Qatar Ministry of Sports Demo
-- Hive External Gold Table: sports_events_gold_json
--
-- Stores structured parsed JSON sports event records written
-- by PySpark Job 3 (JSON) as Parquet, partitioned by event_date.
-- Includes nested metadata fields flattened into columns.
--
-- Column order MUST match GOLD_SCHEMA in 03_transform_to_gold_json.py.
-- Partition column (event_date) is always last.
-- ==============================================================

USE mos_qatar_demo;

DROP TABLE IF EXISTS sports_events_gold_json;

CREATE EXTERNAL TABLE sports_events_gold_json (
    event_id            STRING  COMMENT 'Unique event identifier (e.g. EVT3827)',
    event_name          STRING  COMMENT 'Full name of the sports event',
    sport_type          STRING  COMMENT 'Type of sport (Football, Athletics, etc.)',
    venue               STRING  COMMENT 'Name of the event venue',
    city                STRING  COMMENT 'Host city',
    country             STRING  COMMENT 'Host country',
    team_a              STRING  COMMENT 'First participating team',
    team_b              STRING  COMMENT 'Second participating team',
    status              STRING  COMMENT 'Event status',
    attendance          BIGINT  COMMENT 'Expected or actual attendance',
    meta_source_system  STRING  COMMENT 'Source system from JSON metadata block',
    meta_priority       STRING  COMMENT 'Event priority: High / Medium / Low',
    meta_broadcast      STRING  COMMENT 'Whether event is broadcast: true / false',
    meta_category       STRING  COMMENT 'Event category: International / Domestic / etc.',
    meta_referee        STRING  COMMENT 'Assigned referee name',
    meta_notes          STRING  COMMENT 'Free-text notes from source system',
    source_file         STRING  COMMENT 'Source .json filename',
    ingest_ts           STRING  COMMENT 'Timestamp when file was ingested to HDFS raw',
    processing_ts       STRING  COMMENT 'Timestamp when record was written to gold'
)
COMMENT 'Gold structured JSON sports events - Qatar Ministry of Sports Demo (BBI.ai)'
PARTITIONED BY (
    event_date          STRING  COMMENT 'Event date partition key (YYYY-MM-DD)'
)
STORED AS PARQUET
LOCATION 'hdfs://cdp-master1.cloudera.bbi:8020/data/mos_qatar_demo/json_files/gold'
TBLPROPERTIES (
    'parquet.compression' = 'SNAPPY'
);

DESCRIBE FORMATTED sports_events_gold_json;
SHOW PARTITIONS sports_events_gold_json;
