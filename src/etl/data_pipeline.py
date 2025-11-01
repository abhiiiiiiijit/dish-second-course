from data_extractor import fetch_daily_visits_by_date, fetch_ga_sessions_by_date#src.etl.
from bq_data_load import process_directory_by_date_range #src.etl.
from datetime import datetime
from datetime import datetime
import argparse
import logging


if __name__ == "__main__":


    # -----------------------------------------------------------
    # Initialize logger
    # -----------------------------------------------------------
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s"
    )

    # -----------------------------------------------------------
    # Command line argument parsing
    # -----------------------------------------------------------
    parser = argparse.ArgumentParser(description="ETL Script for Daily Visits & GA Sessions")

    parser.add_argument(
        "--start-date",
        type=str,
        default="2016-08-01",
        help="Start date for data extraction (YYYY-MM-DD). Default: 2016-08-01"
    )

    parser.add_argument(
        "--end-date",
        type=str,
        default="2016-08-02",  # exclusive end date
        help="End date for data extraction (YYYY-MM-DD). Default: 2016-08-02"
    )

    parser.add_argument(
        "--limit",
        type=int,
        default=50,
        help="Record limit per API call. Default: 50"
    )

    # parser.add_argument(
    #     "--base-path",
    #     type=str,
    #     default="data/ga_sessions/",
    #     help="Base directory for local storage. Default: data/ga_sessions/"
    # )

    # parser.add_argument(
    #     "--project-id",
    #     type=str,
    #     default="dish-second-course",
    #     help="BigQuery Project ID. Default: dish-second-course"
    # )

    # parser.add_argument(
    #     "--table-id",
    #     type=str,
    #     default="dish-second-course.analytics.ga_sessions",
    #     help="BigQuery Table ID. Default: dish-second-course.analytics.ga_sessions"
    # )

    args = parser.parse_args()

    # -----------------------------------------------------------
    # Convert dates
    # -----------------------------------------------------------
    start_date = datetime.strptime(args.start_date, "%Y-%m-%d")
    end_date = datetime.strptime(args.end_date, "%Y-%m-%d")

    logging.info("Starting ETL process...")
    logging.info(f"Start date: {start_date}")
    logging.info(f"End date (exclusive): {end_date}")
    logging.info(f"Limit: {args.limit}")
    # logging.info(f"Base path: {args.base_path}")
    # logging.info(f"Project ID: {args.project_id}")
    # logging.info(f"Table ID: {args.table_id}")

    # -----------------------------------------------------------
    # Fetch Daily Visits
    # -----------------------------------------------------------
    try:
        fetch_daily_visits_by_date(start_date, end_date, limit=args.limit)
        logging.info("Daily Visits fetched successfully.")
    except Exception as e:
        logging.error(f"Error fetching Daily Visits: {e}", exc_info=True)

    # -----------------------------------------------------------
    # Fetch GA Sessions
    # -----------------------------------------------------------
    try:
        fetch_ga_sessions_by_date(start_date, end_date, limit=args.limit)
        logging.info("GA Sessions fetched successfully.")
    except Exception as e:
        logging.error(f"Error fetching GA Sessions: {e}", exc_info=True)

    # -----------------------------------------------------------
    # Load into BigQuery
    # -----------------------------------------------------------
    start_date = start_date.strftime("%Y-%m-%d")
    end_date = end_date.strftime("%Y-%m-%d")
    try:
        process_directory_by_date_range(
            base_path='data/daily_visits/',
            project_id='dish-second-course',
            table_id='dish-second-course.analytics.daily_visits',
            start_date=start_date,
            end_date=end_date,
        )
        logging.info("Daily Visits Data successfully loaded into BigQuery.")
    except Exception as e:
        logging.error(f"Error loading daily visit data into BigQuery: {e}", exc_info=True)

    try:
        process_directory_by_date_range(
            base_path='data/ga_sessions/',
            project_id='dish-second-course',
            table_id='dish-second-course.analytics.ga_sessions',
            start_date=start_date,
            end_date=end_date,
        )
        logging.info("GA Session Data successfully loaded into BigQuery.")
    except Exception as e:
        logging.error(f"Error loading ga session data into BigQuery: {e}", exc_info=True)

    logging.info("ETL process completed.")
