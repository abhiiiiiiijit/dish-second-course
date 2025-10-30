from google.cloud import bigquery
DAILY_VISIT_SCHEMA = [
    bigquery.SchemaField("total_visits", "INT64"),
    bigquery.SchemaField("visit_date", "DATE"),
    bigquery.SchemaField("source_file", "STRING")
]
