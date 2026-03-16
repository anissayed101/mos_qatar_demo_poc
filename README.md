# MOS Qatar Demo — Runbook & Operations Guide
## Qatar Ministry of Sports | Unstructured Data Pipeline
### Cloudera CDP 7.1.9 | BBI.ai Demo Team

---

## 1. Architecture Overview

```
[cdp-master1: /home/cloudera/mos_qatar_demo_poc/landing/]
        |
        |  JOB 1: 01_generate_data.py
        |  (cron every 1 min, python3)
        |  Creates: mos_sports_events_YYYYMMDD_HHMMSS.txt
        v
[cdp-master1: /home/cloudera/mos_qatar_demo_poc/landing/mos_sports_events_YYYYMMDD_HHMMSS.txt]
        |
        |  JOB 2: run_ingest.sh -> 02_ingest_to_hdfs.py
        |  (python3, hdfs dfs -put)
        |  Adds ##INGEST_METADATA## header to each file
        |  Archives local file to /home/cloudera/mos_qatar_demo_poc/archive/
        v
[HDFS: /data/mos_qatar_demo/text_files/raw/mos_sports_events_YYYYMMDD_HHMMSS.txt]
        |
        |  JOB 3: run_transform.sh -> 03_transform_to_gold.py
        |  (spark3-submit --master yarn)
        |  Parses pipe-delimited records -> 14 structured fields
        |  Writes Parquet + inserts into Hive
        v
[HDFS: /data/mos_qatar_demo/text_files/gold/event_date=YYYY-MM-DD/part-xxxx.parquet]
        |
        |  Hive External Table (STORED AS PARQUET, partitioned by event_date)
        v
[Hive: mos_qatar_demo.sports_events_gold]
```

---

## 2. Cluster Environment

| Component         | Value                                          |
|-------------------|------------------------------------------------|
| Cluster           | Cloudera CDP 7.1.9                             |
| Spark             | 3.3.2 via spark3-submit                        |
| Python            | 3.6.8                                          |
| Node 131 (driver) | cdp-master1.cloudera.bbi (192.168.113.131)     |
| Node 132          | cdp-master2.cloudera.bbi (192.168.113.132)     |
| Node 133          | cdp-master3.cloudera.bbi (192.168.113.133)     |
| HDFS Namenode     | hdfs://cdp-master1.cloudera.bbi:8020           |
| YARN RM           | cdp-master3.cloudera.bbi:8032                  |
| Hive Metastore    | thrift://cdp-master2.cloudera.bbi:9083,thrift://cdp-master3.cloudera.bbi:9083 |
| HiveServer2       | ZooKeeper discovery (ports 2181 on all nodes)  |
| Run user          | cloudera                                       |
| Local base path   | /home/cloudera/mos_qatar_demo_poc (on cdp-master1 only)      |

---

## 3. File Inventory

```
/home/cloudera/mos_qatar_demo_poc
├── config/
│   └── mos_qatar_demo.ini          # Master config - all jobs read this
├── scripts/
│   ├── 01_generate_data.py         # Job 1: synthetic .txt data generator
│   ├── 02_ingest_to_hdfs.py        # Job 2: local -> HDFS raw ingestion
│   ├── 03_transform_to_gold.py     # Job 3: HDFS raw -> Hive gold (PySpark)
│   ├── run_ingest.sh               # Shell wrapper for Job 2
│   └── run_transform.sh            # Shell wrapper for Job 3
├── ddl/
│   ├── create_database.hql         # Hive database DDL
│   ├── create_raw_table.hql        # sports_unstructured_raw DDL
│   └── create_gold_table.hql       # sports_events_gold DDL
├── run/
│   ├── setup_local.sh              # ONE-TIME: create local dirs, set permissions
│   └── setup_hdfs.sh               # ONE-TIME: create HDFS paths + Hive tables
├── landing/                        # Drop zone for generated .txt files
├── archive/                        # Local files moved here after HDFS push
├── logs/                           # All timestamped log files land here
└── metadata/
    ├── ingested_files.log          # Filenames already pushed to HDFS raw (Job 2)
    └── processed_files.log         # Filenames already transformed to gold (Job 3)
```

---

## 4. STEP-BY-STEP EXECUTION

### STEP 0 — One-Time Setup (run ONCE only)

```bash
# 0.1 Run local setup as root (creates dirs, sets ownership)
sudo bash /home/cloudera/mos_qatar_demo_poc/run/setup_local.sh

# 0.2 Run HDFS and Hive setup as cloudera
su - cloudera
bash /home/cloudera/mos_qatar_demo_poc/run/setup_hdfs.sh
```

Verify HDFS was created:
```bash
hdfs dfs -ls /data/mos_qatar_demo/text_files/
```

Verify Hive tables:
```bash
beeline -u "jdbc:hive2://cdp-master1.cloudera.bbi:2181,cdp-master2.cloudera.bbi:2181,cdp-master3.cloudera.bbi:2181/default;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2" \
  -e "USE mos_qatar_demo; SHOW TABLES;"
```

---

### STEP 1 — Generate Test Data (Job 1)

**Single manual run:**
```bash
su - cloudera
python3 /home/cloudera/mos_qatar_demo_poc/scripts/01_generate_data.py
```

**Check output:**
```bash
ls -lh /home/cloudera/mos_qatar_demo_poc/landing/
cat /home/cloudera/mos_qatar_demo_poc/landing/mos_sports_events_*.txt
```

**Set up cron (every 1 minute):**
```bash
crontab -e
# Add:
* * * * * /usr/bin/python3 /home/cloudera/mos_qatar_demo_poc/scripts/01_generate_data.py >> /home/cloudera/mos_qatar_demo_poc/logs/cron_generate.log 2>&1
```

---

### STEP 2 — Ingest to HDFS Raw (Job 2)

```bash
su - cloudera
bash /home/cloudera/mos_qatar_demo_poc/scripts/run_ingest.sh
```

**Verify HDFS raw:**
```bash
hdfs dfs -ls /data/mos_qatar_demo/text_files/raw/
hdfs dfs -cat /data/mos_qatar_demo/text_files/raw/mos_sports_events_*.txt | head -10
```

**Check tracking log:**
```bash
cat /home/cloudera/mos_qatar_demo_poc/metadata/ingested_files.log
```

---

### STEP 3 — Transform to Gold (Job 3)

```bash
su - cloudera
bash /home/cloudera/mos_qatar_demo_poc/scripts/run_transform.sh
```

**Verify HDFS gold:**
```bash
hdfs dfs -ls -R /data/mos_qatar_demo/text_files/gold/
```

**Check tracking log:**
```bash
cat /home/cloudera/mos_qatar_demo_poc/metadata/processed_files.log
```

---

### STEP 4 — Query in Hive (Demo Queries)

Open beeline:
```bash
beeline -u "jdbc:hive2://cdp-master1.cloudera.bbi:2181,cdp-master2.cloudera.bbi:2181,cdp-master3.cloudera.bbi:2181/default;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2"
```

```sql
-- Switch to database
USE mos_qatar_demo;

-- Total record count
SELECT COUNT(*) AS total_events FROM sports_events_gold;

-- Preview first 10 records
SELECT event_id, event_name, sport_type, venue, event_date, status, attendance
FROM sports_events_gold
LIMIT 10;

-- Events by sport type
SELECT sport_type, COUNT(*) AS total
FROM sports_events_gold
GROUP BY sport_type
ORDER BY total DESC;

-- Events by venue (most booked venues)
SELECT venue, city, COUNT(*) AS bookings
FROM sports_events_gold
GROUP BY venue, city
ORDER BY bookings DESC;

-- Events by status
SELECT status, COUNT(*) AS count
FROM sports_events_gold
GROUP BY status
ORDER BY count DESC;

-- Total expected attendance by city
SELECT city, SUM(attendance) AS total_attendance, COUNT(*) AS events
FROM sports_events_gold
GROUP BY city
ORDER BY total_attendance DESC;

-- Upcoming football events (next 30 days)
SELECT event_date, event_name, venue, team_a, team_b, status, attendance
FROM sports_events_gold
WHERE sport_type = 'Football'
  AND event_date >= CURRENT_DATE
ORDER BY event_date
LIMIT 20;

-- Events at Lusail Stadium
SELECT event_id, event_name, event_date, team_a, team_b, status, attendance
FROM sports_events_gold
WHERE venue = 'Lusail Stadium'
ORDER BY event_date;

-- Source file lineage (how many records came from each file)
SELECT source_file, COUNT(*) AS records,
       MIN(event_date) AS earliest_event,
       MAX(event_date) AS latest_event,
       ingest_ts
FROM sports_events_gold
GROUP BY source_file, ingest_ts
ORDER BY source_file;

-- Show Hive partitions
SHOW PARTITIONS sports_events_gold;

-- Check raw table (quick scan)
SELECT raw_line FROM sports_unstructured_raw LIMIT 5;
```

---

## 5. CRON SCHEDULE (Full Pipeline)

```bash
crontab -e
# ── Job 1: Generate new data file every 1 minute ──────────────────────────
* * * * * /usr/bin/python3 /home/cloudera/mos_qatar_demo_poc/scripts/01_generate_data.py >> /home/cloudera/mos_qatar_demo_poc/logs/cron_generate.log 2>&1

# ── Job 2: Ingest to HDFS every 5 minutes ─────────────────────────────────
*/5 * * * * /bin/bash /home/cloudera/mos_qatar_demo_poc/scripts/run_ingest.sh >> /home/cloudera/mos_qatar_demo_poc/logs/cron_ingest.log 2>&1

# ── Job 3: Transform to gold every 10 minutes ─────────────────────────────
*/10 * * * * /bin/bash /home/cloudera/mos_qatar_demo_poc/scripts/run_transform.sh >> /home/cloudera/mos_qatar_demo_poc/logs/cron_transform.log 2>&1
```

---

## 6. IDEMPOTENCY RULES

| Job   | Tracking File               | Key             | Behaviour on rerun              |
|-------|-----------------------------|-----------------|----------------------------------|
| Job 2 | metadata/ingested_files.log | filename        | Skips files already in the log  |
| Job 3 | metadata/processed_files.log| filename        | Skips files already in the log  |

Both jobs log clearly: `SKIP [already ingested]` or `SKIP [already processed]`.

---

## 7. LOG FILES REFERENCE

| Log File Pattern                                 | Description                        |
|--------------------------------------------------|------------------------------------|
| `logs/generate_data_YYYYMMDD_HHMMSS.log`         | Job 1 run log                      |
| `logs/ingest_to_hdfs_YYYYMMDD_HHMMSS.log`        | Job 2 Python log                   |
| `logs/run_ingest_YYYYMMDD_HHMMSS.log`            | Job 2 shell wrapper log            |
| `logs/transform_to_gold_YYYYMMDD_HHMMSS.log`     | Job 3 PySpark driver log           |
| `logs/run_transform_YYYYMMDD_HHMMSS.log`         | Job 3 shell wrapper log            |
| `logs/setup_hdfs_YYYYMMDD_HHMMSS.log`            | One-time HDFS/Hive setup log       |
| `logs/cron_generate.log`                         | Cron output for Job 1              |
| `logs/cron_ingest.log`                           | Cron output for Job 2              |
| `logs/cron_transform.log`                        | Cron output for Job 3              |

---

## 8. RESET DEMO FROM SCRATCH

```bash
# ── Clear HDFS data ────────────────────────────────────────────────────────
hdfs dfs -rm -r /data/mos_qatar_demo/text_files/raw/*
hdfs dfs -rm -r /data/mos_qatar_demo/text_files/gold/*

# ── Clear tracking logs ────────────────────────────────────────────────────
> //home/cloudera/mos_qatar_demo_poc/metadata/ingested_files.log
> /home/cloudera/mos_qatar_demo_poc/metadata/processed_files.log

# ── Clear local landing and archive ───────────────────────────────────────
rm -f /home/cloudera/mos_qatar_demo_poc/landing/*.txt
rm -f /home/cloudera/mos_qatar_demo_poc/archive/*.txt

# ── Re-run Hive DDL (drops and recreates tables) ──────────────────────────
bash /home/cloudera/mos_qatar_demo_poc/run/setup_hdfs.sh
```

---

## 9. TROUBLESHOOTING

**`hdfs: command not found`**
```bash
export PATH=$PATH:/opt/cloudera/parcels/CDH/bin
```

**`beeline: command not found`**
```bash
export PATH=$PATH:/opt/cloudera/parcels/CDH/bin
```

**Beeline cannot connect (ZooKeeper timeout)**
```bash
echo ruok | nc cdp-master1.cloudera.bbi 2181
echo ruok | nc cdp-master2.cloudera.bbi 2181
echo ruok | nc cdp-master3.cloudera.bbi 2181
# Should respond: imok
```

**Spark job fails: Hive metastore not available**
```bash
# Check metastore on master2 and master3
ps aux | grep HiveMetaStore
# Or via CM API / CM console
```

**insertInto fails: column mismatch**
- Verify GOLD_SCHEMA column order in 03_transform_to_gold.py matches create_gold_table.hql exactly
- Partition column (event_date) must be LAST in both places

**No data in Hive gold table after transform**
```sql
-- Run in beeline:
MSCK REPAIR TABLE mos_qatar_demo.sports_events_gold;
```

**Spark YARN job queued / not starting**
```bash
yarn application -list
# Check YARN RM UI: http://cdp-master3.cloudera.bbi:8088
```

---

## 10. FUTURE OOZIE MIGRATION NOTES

| Current Job     | Oozie Action Type | Notes                                    |
|-----------------|-------------------|------------------------------------------|
| Job 1 (generate)| Shell action      | Point to: `/usr/bin/python3 01_generate_data.py` |
| Job 2 (ingest)  | Shell action      | Point to: `run_ingest.sh`                |
| Job 3 (transform)| Spark action     | Use spark3-submit params from run_transform.sh |

All paths come from `mos_qatar_demo.ini` — no code changes needed for Oozie.
Pass `--config` arg via Oozie job properties if config path differs.

---

*Prepared by BBI.ai for Qatar Ministry of Sports client demo engagement.*
*Cluster: Cloudera CDP 7.1.9 | Date: March 2026*
