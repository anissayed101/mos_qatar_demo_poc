-- ==============================================================
-- create_raw_json_table.hql
-- Qatar Ministry of Sports Demo
-- Hive External Raw Table: sports_unstructured_raw_json
--
-- Points to HDFS JSON raw directory. Each row = one raw JSON file
-- read as a single text line. Used for ad-hoc inspection only.
-- Actual parsing is done by PySpark Job 3 (JSON).
-- ==============================================================

USE mos_qatar_demo;

DROP TABLE IF EXISTS sports_unstructured_raw_json;

CREATE EXTERNAL TABLE sports_unstructured_raw_json (
    raw_line    STRING  COMMENT 'Raw JSON file content line'
)
COMMENT 'Raw JSON sports event data ingested from local landing directory'
STORED AS TEXTFILE
LOCATION 'hdfs://cdp-master1.cloudera.bbi:8020/data/mos_qatar_demo/json_files/raw'
TBLPROPERTIES (
    'skip.header.line.count' = '0'
);

MSCK REPAIR TABLE sports_unstructured_raw_json;

DESCRIBE FORMATTED sports_unstructured_raw_json;
SELECT raw_line FROM sports_unstructured_raw_json LIMIT 3;
