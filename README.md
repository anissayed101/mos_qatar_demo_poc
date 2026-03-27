# MOS Qatar Demo — Unstructured & JSON Data Pipeline
## Qatar Ministry of Sports | Cloudera CDP 7.1.9 | BBI.ai Demo Team

---

## Overview

This repository contains a complete end-to-end Cloudera CDP demo pipeline built for the **Qatar Ministry of Sports** client engagement. It demonstrates two parallel data ingestion and transformation use cases running on a 3-node CDP 7.1.9 cluster:

| Pipeline | Input Format | Description |
|----------|-------------|-------------|
| **Text Pipeline** | `.txt` pipe-delimited records | Unstructured sports event text → Hive gold table |
| **JSON Pipeline** | `.json` array records | Semi-structured JSON sports events → Hive gold table |

Both pipelines follow the same logical flow: **Generate → Ingest to HDFS Raw → Transform to Hive Gold**

---

## Architecture

### Text Pipeline
```
[cdp-master1: landing/]
        |
        |  JOB 1: 01_generate_data.py
        |  Generates: mos_sports_events_YYYYMMDD_HHMMSS.txt
        v
[HDFS: /data/mos_qatar_demo/text_files/raw/]
        |
        |  JOB 2: run_ingest.sh -> 02_ingest_to_hdfs.py
        |  Adds ##INGEST_METADATA## header, archives local file
        v
[HDFS: /data/mos_qatar_demo/text_files/gold/event_date=YYYY-MM-DD/]
        |
        |  JOB 3: run_transform.sh -> 03_transform_to_gold.py
        |  PySpark: parses 14 structured fields, writes Parquet
        v
[Hive: mos_qatar_demo.sports_events_gold]
```

### JSON Pipeline
```
[cdp-master1: landing_json/]
        |
        |  JOB 1: 01_generate_data_json.py
        |  Generates: mos_sports_events_YYYYMMDD_HHMMSS.json
        v
[HDFS: /data/mos_qatar_demo/json_files/raw/]
        |
        |  JOB 2: run_ingest_json.sh -> 02_ingest_to_hdfs_json.py
        |  Validates JSON, pushes to HDFS, archives local file
        v
[HDFS: /data/mos_qatar_demo/json_files/gold/event_date=YYYY-MM-DD/]
        |
        |  JOB 3: run_transform_json.sh -> 03_transform_to_gold_json.py
        |  PySpark: parses 20 fields incl. nested metadata, writes Parquet
        v
[Hive: mos_qatar_demo.sports_events_gold_json]
```

---

## Cluster Environment

| Component | Value |
|-----------|-------|
| Cluster | Cloudera CDP 7.1.9 |
| Spark | 3.3.2 via `spark3-submit` |
| Python | 3.6.8 |
| Node 131 (driver) | cdp-master1.cloudera.bbi (192.168.113.131) |
| Node 132 | cdp-master2.cloudera.bbi (192.168.113.132) |
| Node 133 | cdp-master3.cloudera.bbi (192.168.113.133) |
| HDFS Namenode | hdfs://cdp-master1.cloudera.bbi:8020 |
| YARN ResourceManager | cdp-master3.cloudera.bbi:8032 |
| Hive Metastore | thrift://cdp-master2.cloudera.bbi:9083,thrift://cdp-master3.cloudera.bbi:9083 |
| HiveServer2 | ZooKeeper discovery (2181 on all nodes) |
| Run user | `cloudera` |
| Local base path | `/home/cloudera/mos_qatar_demo_poc` (cdp-master1 only) |

---

## Repository Structure

```
mos_qatar_demo_poc/
├── config/
│   ├── mos_qatar_demo.ini           # Text pipeline master config
│   └── mos_qatar_json.ini           # JSON pipeline master config
│
├── scripts/
│   ├── 01_generate_data.py          # Text Job 1: generates .txt files
│   ├── 02_ingest_to_hdfs.py         # Text Job 2: local .txt -> HDFS raw
│   ├── 03_transform_to_gold.py      # Text Job 3: PySpark raw -> Hive gold
│   ├── run_ingest.sh                # Wrapper for Text Job 2
│   ├── run_transform.sh             # Wrapper for Text Job 3
│   ├── run_pipeline_text.sh         # MASTER: runs full text pipeline
│   │
│   ├── 01_generate_data_json.py     # JSON Job 1: generates .json files
│   ├── 02_ingest_to_hdfs_json.py    # JSON Job 2: local .json -> HDFS raw
│   ├── 03_transform_to_gold_json.py # JSON Job 3: PySpark raw -> Hive gold
│   ├── run_ingest_json.sh           # Wrapper for JSON Job 2
│   ├── run_transform_json.sh        # Wrapper for JSON Job 3
│   └── run_pipeline_json.sh         # MASTER: runs full JSON pipeline
│
├── ddl/
│   ├── create_database.hql          # Creates mos_qatar_demo Hive DB
│   ├── create_raw_table.hql         # sports_unstructured_raw (text)
│   ├── create_gold_table.hql        # sports_events_gold (text)
│   ├── create_raw_json_table.hql    # sports_unstructured_raw_json
│   └── create_gold_json_table.hql   # sports_events_gold_json
│
├── run/
│   ├── setup_local.sh               # ONE-TIME: create local dirs
│   ├── setup_hdfs.sh                # ONE-TIME: HDFS + Hive setup (text)
│   └── setup_hdfs_json.sh           # ONE-TIME: HDFS + Hive setup (JSON)
│
├── landing/                         # Text .txt drop zone (cdp-master1)
├── landing_json/                    # JSON .json drop zone (cdp-master1)
├── archive/                         # Archived text files post-ingest
├── archive_json/                    # Archived JSON files post-ingest
├── logs/                            # All pipeline log files
├── metadata/
│   ├── ingested_files.log           # Text files pushed to HDFS raw
│   ├── processed_files.log          # Text files transformed to gold
│   ├── ingested_files_json.log      # JSON files pushed to HDFS raw
│   └── processed_files_json.log     # JSON files transformed to gold
└── RUNBOOK.md                       # Detailed operations guide
```

---

## Hive Tables

| Table | Pipeline | Format | Partition |
|-------|----------|--------|-----------|
| `sports_unstructured_raw` | Text | TEXTFILE | None |
| `sports_events_gold` | Text | PARQUET/SNAPPY | `event_date` |
| `sports_unstructured_raw_json` | JSON | TEXTFILE | None |
| `sports_events_gold_json` | JSON | PARQUET/SNAPPY | `event_date` |

All tables are in the `mos_qatar_demo` Hive database.

---

## ONE-TIME SETUP

### Step 0.1 — Local Directory Setup (run as root, once)
```bash
sudo bash /home/cloudera/mos_qatar_demo_poc/run/setup_local.sh
```

### Step 0.2 — HDFS & Hive Setup for Text Pipeline (run as cloudera, once)
```bash
bash /home/cloudera/mos_qatar_demo_poc/run/setup_hdfs.sh
```

### Step 0.3 — HDFS & Hive Setup for JSON Pipeline (run as cloudera, once)
```bash
bash /home/cloudera/mos_qatar_demo_poc/run/setup_hdfs_json.sh
```

Verify HDFS:
```bash
hdfs dfs -ls /data/mos_qatar_demo/
```

Verify Hive tables:
```bash
beeline -u "jdbc:hive2://cdp-master1.cloudera.bbi:2181,cdp-master2.cloudera.bbi:2181,cdp-master3.cloudera.bbi:2181/default;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2" \
  -e "USE mos_qatar_demo; SHOW TABLES;"
```

---

## RUNNING THE PIPELINES

### Option A — Master Script (Recommended for Demo)

Runs all 3 jobs end-to-end with a 5-second sleep between each step.

**Text pipeline only:**
```bash
bash /home/cloudera/mos_qatar_demo_poc/scripts/run_pipeline_text.sh
```

**JSON pipeline only:**
```bash
bash /home/cloudera/mos_qatar_demo_poc/scripts/run_pipeline_json.sh
```

---

### Option B — Run Jobs Individually

#### TEXT PIPELINE

**Job 1 — Generate .txt data file:**
```bash
python3 /home/cloudera/mos_qatar_demo_poc/scripts/01_generate_data.py
```

**Job 2 — Ingest .txt to HDFS raw:**
```bash
bash /home/cloudera/mos_qatar_demo_poc/scripts/run_ingest.sh
```

**Job 3 — Transform text to Hive gold (PySpark):**
```bash
bash /home/cloudera/mos_qatar_demo_poc/scripts/run_transform.sh
```

---

#### JSON PIPELINE

**Job 1 — Generate .json data file:**
```bash
python3 /home/cloudera/mos_qatar_demo_poc/scripts/01_generate_data_json.py
```

**Job 2 — Ingest .json to HDFS raw:**
```bash
bash /home/cloudera/mos_qatar_demo_poc/scripts/run_ingest_json.sh
```

**Job 3 — Transform JSON to Hive gold (PySpark):**
```bash
bash /home/cloudera/mos_qatar_demo_poc/scripts/run_transform_json.sh
```

---

## VERIFICATION COMMANDS

### Text Pipeline Verification
```bash
# Check generated files in landing
ls -lh /home/cloudera/mos_qatar_demo_poc/landing/

# Check HDFS raw
hdfs dfs -ls /data/mos_qatar_demo/text_files/raw/

# Check HDFS gold partitions
hdfs dfs -ls -R /data/mos_qatar_demo/text_files/gold/

# Check tracking logs
cat /home/cloudera/mos_qatar_demo_poc/metadata/ingested_files.log
cat /home/cloudera/mos_qatar_demo_poc/metadata/processed_files.log
```

### JSON Pipeline Verification
```bash
# Check generated files in landing_json
ls -lh /home/cloudera/mos_qatar_demo_poc/landing_json/

# Check HDFS raw
hdfs dfs -ls /data/mos_qatar_demo/json_files/raw/

# Check HDFS gold partitions
hdfs dfs -ls -R /data/mos_qatar_demo/json_files/gold/

# Check tracking logs
cat /home/cloudera/mos_qatar_demo_poc/metadata/ingested_files_json.log
cat /home/cloudera/mos_qatar_demo_poc/metadata/processed_files_json.log
```

---

## HIVE DEMO QUERIES

Open beeline:
```bash
beeline -u "jdbc:hive2://cdp-master1.cloudera.bbi:2181,cdp-master2.cloudera.bbi:2181,cdp-master3.cloudera.bbi:2181/default;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2"
```

### Text Pipeline Queries

```sql
USE mos_qatar_demo;

-- Total record count
SELECT COUNT(*) AS total_events FROM sports_events_gold;

-- Preview records
SELECT event_id, event_name, sport_type, venue, event_date, status, attendance
FROM sports_events_gold LIMIT 10;

-- Events by sport type
SELECT sport_type, COUNT(*) AS total
FROM sports_events_gold
GROUP BY sport_type ORDER BY total DESC;

-- Most booked venues
SELECT venue, city, COUNT(*) AS bookings
FROM sports_events_gold
GROUP BY venue, city ORDER BY bookings DESC;

-- Events by status
SELECT status, COUNT(*) AS count
FROM sports_events_gold
GROUP BY status ORDER BY count DESC;

-- Total attendance by city
SELECT city, SUM(attendance) AS total_attendance, COUNT(*) AS events
FROM sports_events_gold
GROUP BY city ORDER BY total_attendance DESC;

-- Upcoming football events
SELECT event_date, event_name, venue, team_a, team_b, status, attendance
FROM sports_events_gold
WHERE sport_type = 'Football' AND event_date >= CURRENT_DATE
ORDER BY event_date LIMIT 20;

-- Events at Lusail Stadium
SELECT event_id, event_name, event_date, team_a, team_b, status, attendance
FROM sports_events_gold
WHERE venue = 'Lusail Stadium'
ORDER BY event_date;

-- Source file lineage
SELECT source_file, COUNT(*) AS records,
       MIN(event_date) AS earliest_event,
       MAX(event_date) AS latest_event,
       ingest_ts
FROM sports_events_gold
GROUP BY source_file, ingest_ts ORDER BY source_file;

-- Show partitions
SHOW PARTITIONS sports_events_gold;
```

### JSON Pipeline Queries

```sql
USE mos_qatar_demo;

-- Total record count
SELECT COUNT(*) AS total_events FROM sports_events_gold_json;

-- Preview records with metadata fields
SELECT event_id, event_name, sport_type, venue, event_date,
       status, attendance, meta_priority, meta_broadcast, meta_category
FROM sports_events_gold_json LIMIT 10;

-- Events by priority (from nested metadata)
SELECT meta_priority, COUNT(*) AS total
FROM sports_events_gold_json
GROUP BY meta_priority ORDER BY total DESC;

-- Broadcast vs non-broadcast events
SELECT meta_broadcast, COUNT(*) AS total
FROM sports_events_gold_json
GROUP BY meta_broadcast ORDER BY total DESC;

-- Events by category
SELECT meta_category, COUNT(*) AS total, SUM(attendance) AS total_attendance
FROM sports_events_gold_json
GROUP BY meta_category ORDER BY total DESC;

-- Events by source system (metadata field)
SELECT meta_source_system, COUNT(*) AS events
FROM sports_events_gold_json
GROUP BY meta_source_system ORDER BY events DESC;

-- High priority events at top venues
SELECT event_id, event_name, venue, event_date, team_a, team_b,
       meta_priority, meta_referee, meta_notes
FROM sports_events_gold_json
WHERE meta_priority = 'High'
ORDER BY event_date LIMIT 20;

-- Referee workload
SELECT meta_referee, COUNT(*) AS assigned_events
FROM sports_events_gold_json
GROUP BY meta_referee ORDER BY assigned_events DESC;

-- Compare both pipelines record counts
SELECT 'Text Pipeline' AS pipeline, COUNT(*) AS records FROM sports_events_gold
UNION ALL
SELECT 'JSON Pipeline' AS pipeline, COUNT(*) AS records FROM sports_events_gold_json;

-- Show partitions
SHOW PARTITIONS sports_events_gold_json;
```

---

## CRON SCHEDULE

```bash
crontab -e
```

```
# ── TEXT PIPELINE ──────────────────────────────────────────────────────────
# Job 1: Generate new .txt file every 1 minute
* * * * * /usr/bin/python3 /home/cloudera/mos_qatar_demo_poc/scripts/01_generate_data.py >> /home/cloudera/mos_qatar_demo_poc/logs/cron_generate.log 2>&1

# Job 2: Ingest text to HDFS every 5 minutes
*/5 * * * * /bin/bash /home/cloudera/mos_qatar_demo_poc/scripts/run_ingest.sh >> /home/cloudera/mos_qatar_demo_poc/logs/cron_ingest.log 2>&1

# Job 3: Transform text to gold every 10 minutes
*/10 * * * * /bin/bash /home/cloudera/mos_qatar_demo_poc/scripts/run_transform.sh >> /home/cloudera/mos_qatar_demo_poc/logs/cron_transform.log 2>&1

# ── JSON PIPELINE ──────────────────────────────────────────────────────────
# Job 1: Generate new .json file every 1 minute
* * * * * /usr/bin/python3 /home/cloudera/mos_qatar_demo_poc/scripts/01_generate_data_json.py >> /home/cloudera/mos_qatar_demo_poc/logs/cron_generate_json.log 2>&1

# Job 2: Ingest JSON to HDFS every 5 minutes
*/5 * * * * /bin/bash /home/cloudera/mos_qatar_demo_poc/scripts/run_ingest_json.sh >> /home/cloudera/mos_qatar_demo_poc/logs/cron_ingest_json.log 2>&1

# Job 3: Transform JSON to gold every 10 minutes
*/10 * * * * /bin/bash /home/cloudera/mos_qatar_demo_poc/scripts/run_transform_json.sh >> /home/cloudera/mos_qatar_demo_poc/logs/cron_transform_json.log 2>&1
```

---

## IDEMPOTENCY

All jobs are fully idempotent — safe to rerun at any time.

| Job | Tracking File | Behaviour on Rerun |
|-----|--------------|-------------------|
| Text Job 2 | `metadata/ingested_files.log` | Skips already-ingested filenames |
| Text Job 3 | `metadata/processed_files.log` | Skips already-processed filenames |
| JSON Job 2 | `metadata/ingested_files_json.log` | Skips already-ingested filenames |
| JSON Job 3 | `metadata/processed_files_json.log` | Skips already-processed filenames |

---

## LOG FILES REFERENCE

| Log File Pattern | Description |
|-----------------|-------------|
| `logs/generate_data_YYYYMMDD_HHMMSS.log` | Text Job 1 run log |
| `logs/ingest_to_hdfs_YYYYMMDD_HHMMSS.log` | Text Job 2 Python log |
| `logs/run_ingest_YYYYMMDD_HHMMSS.log` | Text Job 2 shell wrapper log |
| `logs/transform_to_gold_YYYYMMDD_HHMMSS.log` | Text Job 3 PySpark driver log |
| `logs/run_transform_YYYYMMDD_HHMMSS.log` | Text Job 3 shell wrapper log |
| `logs/pipeline_text_YYYYMMDD_HHMMSS.log` | Text master pipeline log |
| `logs/generate_data_json_YYYYMMDD_HHMMSS.log` | JSON Job 1 run log |
| `logs/ingest_to_hdfs_json_YYYYMMDD_HHMMSS.log` | JSON Job 2 Python log |
| `logs/run_ingest_json_YYYYMMDD_HHMMSS.log` | JSON Job 2 shell wrapper log |
| `logs/transform_to_gold_json_YYYYMMDD_HHMMSS.log` | JSON Job 3 PySpark driver log |
| `logs/run_transform_json_YYYYMMDD_HHMMSS.log` | JSON Job 3 shell wrapper log |
| `logs/pipeline_json_YYYYMMDD_HHMMSS.log` | JSON master pipeline log |

---

## RESET DEMO FROM SCRATCH

### Reset Text Pipeline
```bash
hdfs dfs -rm -r /data/mos_qatar_demo/text_files/raw/*
hdfs dfs -rm -r /data/mos_qatar_demo/text_files/gold/*
> /home/cloudera/mos_qatar_demo_poc/metadata/ingested_files.log
> /home/cloudera/mos_qatar_demo_poc/metadata/processed_files.log
rm -f /home/cloudera/mos_qatar_demo_poc/landing/*.txt
rm -f /home/cloudera/mos_qatar_demo_poc/archive/*.txt
bash /home/cloudera/mos_qatar_demo_poc/run/setup_hdfs.sh
```

### Reset JSON Pipeline
```bash
hdfs dfs -rm -r /data/mos_qatar_demo/json_files/raw/*
hdfs dfs -rm -r /data/mos_qatar_demo/json_files/gold/*
> /home/cloudera/mos_qatar_demo_poc/metadata/ingested_files_json.log
> /home/cloudera/mos_qatar_demo_poc/metadata/processed_files_json.log
rm -f /home/cloudera/mos_qatar_demo_poc/landing_json/*.json
rm -f /home/cloudera/mos_qatar_demo_poc/archive_json/*.json
bash /home/cloudera/mos_qatar_demo_poc/run/setup_hdfs_json.sh
```

---

## TROUBLESHOOTING

**`hdfs: command not found`**
```bash
export PATH=$PATH:/opt/cloudera/parcels/CDH/bin
```

**`beeline: command not found`**
```bash
export PATH=$PATH:/opt/cloudera/parcels/CDH/bin
```

**Beeline ZooKeeper timeout**
```bash
echo ruok | nc cdp-master1.cloudera.bbi 2181
echo ruok | nc cdp-master2.cloudera.bbi 2181
echo ruok | nc cdp-master3.cloudera.bbi 2181
# Should respond: imok
```

**Spark job fails: Hive metastore not available**
```bash
ps aux | grep HiveMetaStore
```

**No data in Hive gold table after transform**
```sql
MSCK REPAIR TABLE mos_qatar_demo.sports_events_gold;
MSCK REPAIR TABLE mos_qatar_demo.sports_events_gold_json;
```

**YARN job queued / not starting**
```bash
yarn application -list
# YARN RM UI: http://cdp-master3.cloudera.bbi:8088
```

**Column mismatch on insertInto**
- Verify `GOLD_SCHEMA` column order in the PySpark script matches the Hive DDL exactly
- Partition column (`event_date`) must always be LAST in both

---

## FUTURE OOZIE MIGRATION

| Job | Oozie Action Type | Script |
|-----|------------------|--------|
| Text Job 1 | Shell action | `01_generate_data.py` |
| Text Job 2 | Shell action | `run_ingest.sh` |
| Text Job 3 | Spark action | `run_transform.sh` params |
| JSON Job 1 | Shell action | `01_generate_data_json.py` |
| JSON Job 2 | Shell action | `run_ingest_json.sh` |
| JSON Job 3 | Spark action | `run_transform_json.sh` params |

All paths are config-driven via `.ini` files — no code changes needed for Oozie migration.

---

*Prepared by BBI.ai for Qatar Ministry of Sports client demo engagement.*
*Cluster: Cloudera CDP 7.1.9 | Date: March 2026*
