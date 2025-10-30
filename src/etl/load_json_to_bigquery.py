#!/usr/bin/env python3
import argparse
import json
import logging
import os
import re
from datetime import datetime
from google.cloud import bigquery
from typing import List
import getpass 
import time


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
# Load data into BigQuery
# -----------------------------
def load_to_bigquery(
    bq_client: bigquery.Client,
    table_id: str,          # Format: "project.dataset.table"
    rows: List[dict],
    source_file: str
):
    if not rows:
        logging.warning("⚠ No data to insert, skipping.")
        return

    # ✅ Add metadata fields
    loaded_ts = time.time()           # <-- epoch timestamp
    loaded_by = getpass.getuser()

    for row in rows:

        row["source_file"] = source_file


    print(f"Loading data into  table: {table_id}")


    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        ignore_unknown_values=True,
                schema=[
            bigquery.SchemaField("total_visits", "INT64"),
            bigquery.SchemaField("visit_date", "DATE"),
            bigquery.SchemaField("source_file", "STRING") 
    
        ]
    )

    # ✅ destination = "project.dataset.table$YYYYMMDD"
    load_job = bq_client.load_table_from_json(
        rows,
        destination=table_id,
        job_config=job_config
    )

    load_job.result()  # Wait for load to complete

    if load_job.errors:
        print("❌ Job finished with errors:")
        for error in load_job.errors:
            print(f" - {error['message']}")
    else:
        print(f"✅ Loaded {load_job.output_rows} rows into  {table_id}.")

    logging.info(f"✅ Loaded {len(rows)} rows → {table_id}")



# def load_to_bigquery(
#     bq_client: bigquery.Client,
#     table_id: str,          # Format: "project.dataset.table"
#     rows: List[dict],
#     source_file: str
# ):
#     if not rows:
#         logging.warning("⚠ No data to insert, skipping.")
#         return

#     # ✅ Add metadata fields
#     loaded_ts = time.time()           # <-- epoch timestamp
#     loaded_by = getpass.getuser()

#     for row in rows:
#                 # ✅ Explicit conversion of visit_date to DATE (YYYY-MM-DD)
#         # if "visit_date" in row:
#         #     try:
#         #         parsed_date = datetime.strptime(row["visit_date"], "%Y-%m-%d").date()
#         #         row["visit_date"] = parsed_date.isoformat()   # "YYYY-MM-DD"
#         #     except Exception:
#         #         raise ValueError(f"Invalid visit_date format: {row['visit_date']}")
#         # row["total_visits"] = int(row["total_visits"])  # Ensure total_visits is INT
#         row["source_file"] = source_file
#         # row["load_timestamp"] = loaded_ts
#         # row["loaded_by"] = loaded_by

#     # ✅ Determine partition from visit_date in first row
#     visit_date = rows[0]["visit_date"]          # Expected format: "YYYY-MM-DD"
#     partition_id = visit_date.replace("-", "")  # --> "YYYYMMDD"
#     partitioned_table = f"{table_id}${partition_id}"
#     print(f"Loading data into partitioned table: {partitioned_table}")


#     job_config = bigquery.LoadJobConfig(
#         write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
#         source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
#         ignore_unknown_values=True,
#                 schema=[
#             bigquery.SchemaField("total_visits", "INT64"),
#             bigquery.SchemaField("visit_date", "DATE"),
#             # bigquery.SchemaField("load_timestamp", "TIMESTAMP"),
#             # bigquery.SchemaField("loaded_by", "STRING"),
#             bigquery.SchemaField("source_file", "STRING") # <-- REQUIRED FOR PARTITIONED TABLE LOAD
    
#         ],# Explicitly confirm time partitioning for debugging purposes
#         time_partitioning=bigquery.TimePartitioning(
#         type_=bigquery.TimePartitioningType.DAY,
#         field="visit_date",

#     )
#     )

#     # ✅ destination = "project.dataset.table$YYYYMMDD"
#     load_job = bq_client.load_table_from_json(
#         rows,
#         destination=partitioned_table,
#         job_config=job_config
#     )

#     load_job.result()  # Wait for load to complete

#     if load_job.errors:
#         print("❌ Job finished with errors:")
#         for error in load_job.errors:
#             print(f" - {error['message']}")
#     else:
#         print(f"✅ Loaded {load_job.output_rows} rows into partition {partitioned_table}.")

#     logging.info(f"✅ Loaded {len(rows)} rows → {partitioned_table}")

# -----------------------------
# Walk directory and process files
# -----------------------------
def process_directory(base_path: str, table_id: str, project_id: str):
    bq_client = bigquery.Client(project=project_id)

    for root, _, files in os.walk(base_path):
        partition_date = extract_partition_date(root)
        if not partition_date:
            continue

        logging.info(f"📂 Detected partition date: {partition_date} in folder {root}")
        # logging.info(f"📂 Detected partition date: in folder {root}")
        for file_name in files:
            if not file_name.endswith(".json"):
                continue

            file_path = os.path.join(root, file_name)
            rows = load_json_file(file_path)

            # Override visit_date if missing
            # for row in rows:
            #     if "visit_date" not in row:
            #         row["visit_date"] = partition_date

            load_to_bigquery(
                bq_client=bq_client,
                table_id=table_id,
                rows=rows,
                source_file=file_path,
            )


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
    process_directory(args.path, args.table, args.project)
    end_time = datetime.now()

    logging.info(f"⏱ Completed in: {end_time - start_time}")
