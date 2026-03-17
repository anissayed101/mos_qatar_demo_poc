#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
01_generate_data.py
===================
Qatar Ministry of Sports Demo - Synthetic Sports Data Generator

Purpose:
    Generates a single uniquely-named .txt file per run containing
    3 to 10 synthetic sports event records in pipe-delimited label format.
    Designed to be scheduled via cron every 1 minute.

Record format (one per line):
    Event ID: EVT1001 | Event Name: Gulf Youth Cup Round 1 | Sport: Football |
    Venue: Lusail Stadium | City: Doha | Country: Qatar |
    Event Date: 2026-03-20 | Team A: Qatar U21 | Team B: Jordan U21 |
    Status: Scheduled | Attendance: 42000

Usage:
    python3 01_generate_data.py [--config /path/to/config.ini]

Cron scheduling (every 1 minute):
    * * * * * /usr/bin/python3 /opt/mos_qatar_demo/scripts/01_generate_data.py

Author : BBI.ai Demo Team
Cluster: Cloudera CDP 7.1.9 / Python 3.6.8
"""

import os
import sys
import random
import logging
import argparse
import configparser
from datetime import datetime, timedelta

# ─────────────────────────────────────────────────────────────────────────────
# ARGUMENT PARSING
# ─────────────────────────────────────────────────────────────────────────────

def parse_args():
    default_config = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        'config', 'mos_qatar_demo.ini'
    )
    parser = argparse.ArgumentParser(description='MOS Qatar Sports Data Generator')
    parser.add_argument('--config', default=default_config,
                        help='Path to config .ini file (default: ../config/mos_qatar_demo.ini)')
    return parser.parse_args()


# ─────────────────────────────────────────────────────────────────────────────
# DATA POOLS
# ─────────────────────────────────────────────────────────────────────────────

EVENT_NAMES = [
    "Gulf Youth Cup Round 1",
    "Gulf Youth Cup Round 2",
    "Gulf Youth Cup Quarter Final",
    "Gulf Youth Cup Semi Final",
    "Gulf Youth Cup Final",
    "Qatar Premier League Match Day 5",
    "Qatar Premier League Match Day 8",
    "Qatar Premier League Match Day 12",
    "Arab Nations Championship Group Stage",
    "Arab Nations Championship Semi Final",
    "Ministry Invitational Friendly Tournament",
    "Qatar Schools Football League",
    "GCC Club Championship Quarter Final",
    "GCC Club Championship Semi Final",
    "GCC Club Championship Final",
    "Qatar Cup Semi Final",
    "Qatar Cup Final",
    "Asian Qualifiers Warm-up Match",
    "Doha Sports Festival Opening Match",
    "National Team Preparation Match",
    "Qatar Under-20 Development League",
    "Gulf Cup Qualifier Round 1",
    "Ramadan Football Festival",
    "Ministry Youth Sports Day",
    "National Day Football Celebration",
    "Qatar Olympic Training Camp Evaluation",
    "FIFA World Cup Qatar Legacy Match",
    "MOS Annual Charity Football Gala",
]

SPORTS = [
    "Football", "Football", "Football", "Football", "Football",
    "Athletics", "Swimming", "Basketball", "Handball", "Volleyball",
]

VENUES = [
    "Lusail Stadium",
    "Education City Stadium",
    "Al Bayt Stadium",
    "Khalifa International Stadium",
    "Ahmad Bin Ali Stadium",
    "Al Janoub Stadium",
    "Al Thumama Stadium",
    "Aspire Zone Arena",
    "Qatar Sports Club Arena",
    "Al Wakrah Sports Complex",
]

VENUE_CITY_MAP = {
    "Lusail Stadium":               "Lusail",
    "Education City Stadium":       "Al Rayyan",
    "Al Bayt Stadium":              "Al Khor",
    "Khalifa International Stadium":"Doha",
    "Ahmad Bin Ali Stadium":        "Al Rayyan",
    "Al Janoub Stadium":            "Al Wakrah",
    "Al Thumama Stadium":           "Doha",
    "Aspire Zone Arena":            "Doha",
    "Qatar Sports Club Arena":      "Doha",
    "Al Wakrah Sports Complex":     "Al Wakrah",
}

TEAMS = [
    "Qatar National Team", "Qatar U21 Squad", "Qatar U17 Squad",
    "Jordan National Team", "Jordan U21",
    "Saudi Arabia National", "UAE National Team",
    "Kuwait FC", "Bahrain Youth Team", "Oman FC",
    "Iraq National Team", "Palestine Youth",
    "Al Sadd SC", "Al Duhail SC", "Al Rayyan SC",
    "Al Arabi SC", "Al Gharafa SC", "Al Khor SC",
]

STATUSES = [
    "Scheduled", "Confirmed", "Postponed",
    "Cancelled", "Completed", "In Progress",
]

MIN_RECORDS = 3
MAX_RECORDS = 10


# ─────────────────────────────────────────────────────────────────────────────
# LOGGING
# ─────────────────────────────────────────────────────────────────────────────

def setup_logging(log_dir):
    """Configure rotating timestamped log file plus stdout."""
    os.makedirs(log_dir, exist_ok=True)
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    log_file  = os.path.join(log_dir, 'generate_data_{}.log'.format(timestamp))
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
# RECORD GENERATION
# ─────────────────────────────────────────────────────────────────────────────

def random_event_date():
    """Return a random date string between -5 and +45 days from today."""
    delta = random.randint(-5, 45)
    return (datetime.now() + timedelta(days=delta)).strftime('%Y-%m-%d')


def generate_record():
    """
    Generate a single sports event record as a pipe-delimited labeled string.
    Example output:
        Event ID: EVT3827 | Event Name: Gulf Youth Cup Round 1 | Sport: Football |
        Venue: Lusail Stadium | City: Lusail | Country: Qatar |
        Event Date: 2026-03-20 | Team A: Qatar U21 | Team B: Jordan U21 |
        Status: Scheduled | Attendance: 42000
    """
    event_id   = "EVT{:04d}".format(random.randint(1000, 9999))
    event_name = random.choice(EVENT_NAMES)
    sport      = random.choice(SPORTS)
    venue      = random.choice(VENUES)
    city       = VENUE_CITY_MAP[venue]
    country    = "Qatar"
    event_date = random_event_date()
    team_a, team_b = random.sample(TEAMS, 2)
    status     = random.choice(STATUSES)
    attendance = random.randint(5000, 88000)

    return (
        "Event ID: {event_id} | Event Name: {event_name} | Sport: {sport} | "
        "Venue: {venue} | City: {city} | Country: {country} | "
        "Event Date: {event_date} | Team A: {team_a} | Team B: {team_b} | "
        "Status: {status} | Attendance: {attendance}"
    ).format(
        event_id=event_id,
        event_name=event_name,
        sport=sport,
        venue=venue,
        city=city,
        country=country,
        event_date=event_date,
        team_a=team_a,
        team_b=team_b,
        status=status,
        attendance=attendance,
    )


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────

def main():
    args   = parse_args()

    # ── Load config ──────────────────────────────────────────────────────────
    config = configparser.ConfigParser()
    if not os.path.exists(args.config):
        print("ERROR: Config file not found: {}".format(args.config))
        sys.exit(1)
    config.read(args.config)

    landing_dir = config.get('paths', 'local_landing')
    log_dir     = config.get('paths', 'local_logs')

    # ── Logging ──────────────────────────────────────────────────────────────
    logger = setup_logging(log_dir)
    logger.info("=" * 66)
    logger.info("  MOS Qatar Demo - Sports Data Generator")
    logger.info("=" * 66)
    logger.info("Config file   : {}".format(args.config))
    logger.info("Landing dir   : {}".format(landing_dir))

    # ── Ensure landing directory exists ──────────────────────────────────────
    os.makedirs(landing_dir, exist_ok=True)

    # ── Compose unique output filename ───────────────────────────────────────
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    filename  = "mos_sports_events_{}.txt".format(timestamp)
    filepath  = os.path.join(landing_dir, filename)

    # ── Determine number of records ──────────────────────────────────────────
    num_records = random.randint(MIN_RECORDS, MAX_RECORDS)
    logger.info("Output file   : {}".format(filename))
    logger.info("Records count : {}".format(num_records))
    logger.info("-" * 66)

    # ── Generate records ─────────────────────────────────────────────────────
    records = []
    for i in range(1, num_records + 1):
        rec = generate_record()
        records.append(rec)
        # Log truncated preview (first 90 chars)
        preview = rec[:90] + ("..." if len(rec) > 90 else "")
        logger.info("  Record {:02d}: {}".format(i, preview))

    # ── Write to file ─────────────────────────────────────────────────────────
    with open(filepath, 'w') as fh:
        for rec in records:
            fh.write(rec + '\n')

    file_size = os.path.getsize(filepath)

    logger.info("-" * 66)
    logger.info("File written successfully.")
    logger.info("  Full path : {}".format(filepath))
    logger.info("  Size      : {} bytes".format(file_size))
    logger.info("  Records   : {}".format(num_records))
    logger.info("=" * 66)
    logger.info("  Generator Completed Successfully")
    logger.info("=" * 66)


if __name__ == '__main__':
    main()
