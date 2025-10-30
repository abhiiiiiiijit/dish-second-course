from data_extractor import fetch_daily_visits_by_date, fetch_ga_sessions_by_date
from bq_data_load import process_directory
from datetime import datetime

if __name__ == "__main__":
    # Define date range for extraction
    start_date = datetime(2016, 8, 1)
    end_date = datetime(2016, 8, 1)  # Exclusive

    # Fetch and save Daily Visits data
    fetch_daily_visits_by_date(start_date, end_date, limit=50)

    # Fetch and save GA Sessions data
    fetch_ga_sessions_by_date(start_date, end_date, limit=50)

    # # Load Daily Visits data into BigQuery
    process_directory(
        base_path="data/daily_visits/",
        project_id="dish-second-course",
        table_id="dish-second-course.analytics.daily_visits"
    )

    # Load GA Sessions data into BigQuery
    process_directory(
        base_path="data/ga_sessions/",
        project_id="dish-second-course",
        table_id="dish-second-course.analytics.ga_sessions"
    )