import logging
from pathlib import Path
from google.cloud import bigquery

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

def load_sql_file(path: str) -> str:
    """Load SQL text from file."""
    sql_path = Path(path)
    if not sql_path.exists():
        raise FileNotFoundError(f"SQL file not found: {path}")
    return sql_path.read_text()

def run_bigquery_query(sql_path: str, project_id: str):
    """Runs SQL against BigQuery."""
    client = bigquery.Client(project=project_id)

    logging.info(f"Reading SQL from: {sql_path}")
    query = load_sql_file(sql_path)

    logging.info("Executing query...")
    job = client.query(query)
    result = job.result()  # blocks until complete

    logging.info("Query executed successfully.")
    return result

if __name__ == "__main__":
    PROJECT_ID = "dish-second-course"
    DV_SQL_FILE = "src/sql/daily_visits.sql"
    GA_SQL_FILE = "src/sql/ga_sessions.sql"

    run_bigquery_query(DV_SQL_FILE, PROJECT_ID) #Can parameterize the SQL file path
    run_bigquery_query(GA_SQL_FILE, PROJECT_ID)

