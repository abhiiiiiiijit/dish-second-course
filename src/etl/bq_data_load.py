#!/usr/bin/env python3
import argparse
import json
import logging
import os
import re
from datetime import datetime, timedelta
from google.cloud import bigquery
from typing import List
import getpass 
import time
from src.etl.ga_sessions_schema import GA_SESSIONS_SCHEMA
from src.etl.daily_visits_schema import DAILY_VISIT_SCHEMA

# -----------------------------
# Logging Configuration
# -----------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)


# -----------------------------
# Extract partition date from directory name
# -----------------------------
def extract_partition_date(path: str) -> str | None:
    """
    Extracts date from folder names:
       - date=2016-08-01
       - date=20160801
    Returns formatted date YYYY-MM-DD
    """
    match = re.search(r"date=(\d{4}-\d{2}-\d{2})", path)
    if match:
        return match.group(1)

    match = re.search(r"date=(\d{4})(\d{2})(\d{2})", path)
    if match:
        return f"{match.group(1)}-{match.group(2)}-{match.group(3)}"

    return None


# -----------------------------
# Read JSON File
# -----------------------------
def load_json_file(file_path: str) -> List[dict]:
    try:
        with open(file_path, "r") as f:
            data = json.load(f)

        if not isinstance(data, list):
            data = [data]

        logging.info(f"Loaded {len(data)} records from {file_path}")
        return data

    except Exception as e:
        logging.error(f"Error reading {file_path}: {e}")
        return []




# -----------------------------
# Updated load_to_bigquery()
# -----------------------------
def load_to_bigquery(
    bq_client: bigquery.Client,
    table_id: str,
    rows: List[dict],
    source_file: str
):
    if not rows:
        logging.warning("⚠ No data to insert, skipping.")
        return

    # Detect schema based on table name
    if "ga_sessions" in table_id.lower():
        schema = GA_SESSIONS_SCHEMA
    else:
        schema = DAILY_VISIT_SCHEMA

    for row in rows:
        row["source_file"] = source_file
        if 'date' in row.keys():
                    date_obj = datetime.strptime(row['date'], "%Y%m%d")
                    row['date'] = date_obj.strftime("%Y-%m-%d")

    logging.info(f"📤 Loading {len(rows)} rows into {table_id}")

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        ignore_unknown_values=True,
        schema=schema
    )

    load_job = bq_client.load_table_from_json(
        rows,
        destination=table_id,
        job_config=job_config,
    )

    load_job.result()

    if load_job.errors:
        logging.error(f"❌ Load job errors:\n{load_job.errors}")
    else:
        logging.info(f"✅ Successfully loaded {load_job.output_rows} rows into {table_id}")


# -----------------------------
# Walk directory and process files
# -----------------------------
def process_directory_by_date_range(
    base_path: str,
    table_id: str,
    project_id: str,
    start_date: str,
    end_date: str,
    date_format: str = "%Y-%m-%d",
):
    """
    Processes directories between start_date and end_date (inclusive).
    Handles folder naming formats:
      - date=YYYY-MM-DD
      - date=YYYYMMDD
    """

    bq_client = bigquery.Client(project=project_id)

    current_date = datetime.strptime(start_date, date_format).date()
    end_date = datetime.strptime(end_date, date_format).date()

    while current_date <= end_date:
        # Both supported formats
        dir_formats = [
            f"date={current_date.strftime('%Y-%m-%d')}",
            f"date={current_date.strftime('%Y%m%d')}"
        ]

        found_dir = False

        for d in dir_formats:
            date_dir = os.path.join(base_path, d)

            if os.path.exists(date_dir):
                found_dir = True
                logging.info(f"📂 Processing directory: {date_dir}")

                for file_name in os.listdir(date_dir):
                    if not file_name.endswith(".json"):
                        continue

                    file_path = os.path.join(date_dir, file_name)
                    rows = load_json_file(file_path)

                    load_to_bigquery(
                        bq_client=bq_client,
                        table_id=table_id,
                        rows=rows,
                        source_file=file_path,
                    )

        if not found_dir:
            logging.info(f"⏭️ No directories found for date {current_date}")

        current_date += timedelta(days=1)

# def process_directory(base_path: str, table_id: str, project_id: str):
#     bq_client = bigquery.Client(project=project_id)

#     for root, _, files in os.walk(base_path):
#         partition_date = extract_partition_date(root)
#         if not partition_date:
#             continue

#         logging.info(f"📂 Detected partition date: {partition_date} in folder {root}")
#         # logging.info(f"📂 Detected partition date: in folder {root}")
#         for file_name in files:
#             if not file_name.endswith(".json"):
#                 continue

#             file_path = os.path.join(root, file_name)
#             rows = load_json_file(file_path)


#             load_to_bigquery(
#                 bq_client=bq_client,
#                 table_id=table_id,
#                 rows=rows,
#                 source_file=file_path,
#             )


# -----------------------------
# CLI ENTRYPOINT
# -----------------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Load JSON files into BigQuery")
    parser.add_argument("--path", required=True, help="Base directory containing JSON partitioned folders")
    parser.add_argument("--table", required=True, help="Target BigQuery table in format dataset.table")
    parser.add_argument("--project", required=True, help="GCP Project ID")

    args = parser.parse_args()


       
    start_time = datetime.now()
    process_directory_by_date_range(args.path, args.table, args.project, start_date="2016-08-01", end_date="2016-08-06")
    end_time = datetime.now()

    logging.info(f"⏱ Completed in: {end_time - start_time}")
