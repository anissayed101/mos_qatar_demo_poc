-- ==============================================================
-- create_raw_table.hql
-- Qatar Ministry of Sports Demo
-- Hive External Raw Table: sports_unstructured_raw
--
-- Purpose:
--   Points to the HDFS raw directory where Job 2 deposits .txt files.
--   Each row = one line from the source files (raw_line column).
--   Allows ad-hoc Hive inspection of raw ingested data before transform.
--
-- Notes:
--   - EXTERNAL: Hive does not delete HDFS data if table is dropped.
--   - TEXTFILE: matches the plain text format pushed by Job 2.
--   - skip.header.line.count=0: Do not skip metadata ## lines; they are
--     visible here and filtered by Job 3 during transformation.
-- ==============================================================

USE mos_qatar_demo;

DROP TABLE IF EXISTS sports_unstructured_raw;

CREATE EXTERNAL TABLE sports_unstructured_raw (
    raw_line    STRING    COMMENT 'Raw text line from ingested .txt sports event file'
)
COMMENT 'Raw unstructured sports event data ingested from local Linux landing directory'
STORED AS TEXTFILE
LOCATION 'hdfs://cdp-master1.cloudera.bbi:8020/data/mos_qatar_demo/text_files/raw'
TBLPROPERTIES (
    'skip.header.line.count' = '0'
);

-- Refresh partition/file metadata
MSCK REPAIR TABLE sports_unstructured_raw;

-- Validate
DESCRIBE FORMATTED sports_unstructured_raw;
SELECT raw_line FROM sports_unstructured_raw LIMIT 5;
