-- ==============================================================
-- create_database.hql
-- Qatar Ministry of Sports Demo - Create Hive Database
-- ==============================================================

CREATE DATABASE IF NOT EXISTS mos_qatar_demo
  COMMENT 'Qatar Ministry of Sports - Unstructured Data Pipeline Demo (BBI.ai)'
  LOCATION 'hdfs://cdp-master1.cloudera.bbi:8020/user/hive/warehouse/mos_qatar_demo.db';

SHOW DATABASES LIKE 'mos_qatar_demo';
