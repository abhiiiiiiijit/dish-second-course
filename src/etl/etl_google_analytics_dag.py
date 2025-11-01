from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import os
import sys
# Get project root (two levels up from dags/)
# PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '../'))
# sys.path.append(PROJECT_ROOT)

from data_extractor import (
    fetch_daily_visits_by_date,
    fetch_ga_sessions_by_date
)
import api_client
from bq_data_load import process_directory_by_date_range

# DAG configuration
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

# Define the DAG
with DAG(
    dag_id="etl_google_analytics_dag",
    default_args=default_args,
    description="ETL DAG for Google Analytics data pipeline",
    schedule_interval="0 6,18 * * 3",  # Runs at 6:00 AM and 6:00 PM on Wednesdays
    start_date=datetime(2016, 8, 10),
    catchup=False,
    tags=["google_analytics", "etl"],
) as dag:

    def compute_date_range(execution_date):
        """
        Compute dynamic start_date and end_date for task functions.
        start_date = dag_run_date - 7 days
        end_date = dag_run_date - 1 day
        """
        end_date = execution_date - timedelta(days=1)
        start_date = execution_date - timedelta(days=7)
        return start_date, end_date

    def wrapper_daily_visits_extraction(**context):
        start_date, end_date = compute_date_range(context["execution_date"])
        fetch_daily_visits_by_date(start_date=start_date, end_date=end_date, limit=50)

    def wrapper_daily_visits_ingestion(**context):
        start_date, end_date = compute_date_range(context["execution_date"])
        start_date = start_date.strftime("%Y-%m-%d")
        end_date = end_date.strftime("%Y-%m-%d")
        process_directory_by_date_range(start_date=start_date, end_date=end_date, project_id="dish-second-course", table_id="dish-second-course.analytics.daily_visits", base_path="data/daily_visits/")

    def wrapper_ga_sessions_extraction(**context):
        start_date, end_date = compute_date_range(context["execution_date"])
        fetch_ga_sessions_by_date(start_date=start_date, end_date=end_date,limit=500)

    def wrapper_ga_sessions_ingestion(**context):
        start_date, end_date = compute_date_range(context["execution_date"])
        start_date = start_date.strftime("%Y-%m-%d")
        end_date = end_date.strftime("%Y-%m-%d")
        process_directory_by_date_range(start_date=start_date, end_date=end_date, project_id="dish-second-course", table_id="dish-second-course.analytics.ga_sessions", base_path="data/ga_sessions/")

    # Define the tasks
    daily_visits_extraction = PythonOperator(
        task_id="daily_visits_data_extration",
        python_callable=wrapper_daily_visits_extraction,
        provide_context=True,
        execution_timeout=timedelta(minutes=3),
    )

    daily_visits_ingestion = PythonOperator(
        task_id="daily_visits_bq_ingestion",
        python_callable=wrapper_daily_visits_ingestion,
        provide_context=True,
        execution_timeout=timedelta(minutes=3),
    )

    ga_sessions_extraction = PythonOperator(
        task_id="ga_sessions_data_extration",
        python_callable=wrapper_ga_sessions_extraction,
        provide_context=True,
        execution_timeout=timedelta(minutes=3),
    )

    ga_sessions_ingestion = PythonOperator(
        task_id="ga_sessions_bq_ingestion",
        python_callable=wrapper_ga_sessions_ingestion,
        provide_context=True,
        execution_timeout=timedelta(minutes=3),
    )

    # Task flow
    daily_visits_extraction >> daily_visits_ingestion >> ga_sessions_extraction >> ga_sessions_ingestion
