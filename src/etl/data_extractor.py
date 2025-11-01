import os
import json
import time
import uuid
import logging
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional
from collections import defaultdict

from api_client import get_daily_visits, get_ga_sessions

# === Logging Configuration ===
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)

# === Constants ===
DATA_DIR = "data"
MAX_RETRIES = 5
INITIAL_BACKOFF = 2  # seconds
MAX_BACKOFF = 60     # seconds
DATE_FORMAT = "%Y-%m-%d"

# Date range for both APIs
START_DATE = datetime(2016, 8, 1)
END_DATE = datetime(2016, 8, 7)


# === Utility Functions ===
def ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def handle_rate_limit(retry_count: int) -> None:
    backoff_time = min(INITIAL_BACKOFF * (2 ** retry_count), MAX_BACKOFF)
    logger.warning("Rate limit reached. Retrying in %s seconds...", backoff_time)
    time.sleep(backoff_time)


def save_json_partitioned(
    data: List[Dict[str, Any]],
    folder: str,
    date_value: str,
) -> None:
    """
    Save records into a date-partitioned directory with unique file names.
    Example: data/daily_visits/date=2025-10-28/part-xxxx.json
    """
    if not data:
        logger.warning("No data to save for %s (%s).", folder, date_value)
        return

    base_path = os.path.join(DATA_DIR, folder, f"date={date_value}")
    ensure_dir(base_path)

    file_name = f"part-{uuid.uuid4().hex[:8]}.json"
    file_path = os.path.join(base_path, file_name)

    try:
        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        logger.info("Saved %d records to %s", len(data), file_path)
    except Exception as e:
        logger.exception("Failed to save data for %s: %s", date_value, e)


def fetch_all_paginated_data(
    fetch_function,
    data_key: str = "records",
    limit: int = 100,
    **kwargs
) -> List[Dict[str, Any]]:
    """
    Generic pagination handler for APIs supporting 'page' and 'limit'.
    Additional filters (like 'date') can be passed via kwargs.
    """
    all_data: List[Dict[str, Any]] = []
    page = 1
    retry_count = 0

    while True:
        try:
            logger.info("Fetching page %d with params: %s", page, kwargs)
            data = fetch_function(page=page, limit=limit, **kwargs)

            if data is None:
                logger.warning("No data returned for page %d.", page)
                break

            records = data.get(data_key, [])
            if not records:
                logger.info("No more records found (page %d).", page)
                break

            all_data.extend(records)
            logger.info("Fetched %d records (total so far: %d).", len(records), len(all_data))

            if len(records) < limit:
                logger.info("Last page reached for params: %s", kwargs)
                break

            page += 1
            retry_count = 0

        except Exception as e:
            if "429" in str(e):
                handle_rate_limit(retry_count)
                retry_count += 1
                if retry_count > MAX_RETRIES:
                    logger.error("Max retries exceeded for rate limiting.")
                    break
            else:
                logger.exception("Unexpected error while fetching page %d: %s", page, e)
                break

    return all_data


# === Specific Extractors ===
def fetch_daily_visits_by_date(
    start_date: datetime,
    end_date: datetime,
    limit: int = 100
) -> None:
    """
    Fetch Daily Visits day-by-day between the date range and save each day's data.
    """
    logger.info("Starting Daily Visits extraction from %s to %s",
                start_date.date(), end_date.date())

    current_date = start_date
    while current_date <= end_date:
        date_str = current_date.strftime(DATE_FORMAT)
        logger.info("Fetching Daily Visits for date %s...", date_str)

        try:
            day_data = fetch_all_paginated_data(
                get_daily_visits,
                data_key="records",
                limit=limit,
                start_date=date_str,
                end_date=date_str
            )

            if day_data:
                save_json_partitioned(day_data, folder="daily_visits", date_value=date_str)
            else:
                logger.warning("No Daily Visit data found for %s.", date_str)

        except Exception as e:
            logger.exception("Error fetching Daily Visits for %s: %s", date_str, e)

        current_date += timedelta(days=1)

    logger.info("✅ Completed Daily Visits extraction for all dates.")


def fetch_ga_sessions_by_date(
    start_date: datetime,
    end_date: datetime,
    limit: int = 100
) -> None:
    """
    Fetch GA Sessions day-by-day between the date range and save each day's data.
    """
    logger.info("Starting GA Sessions extraction from %s to %s",
                start_date.date(), end_date.date())

    current_date = start_date
    while current_date <= end_date:
        date_str = current_date.strftime("%Y%m%d")
        logger.info("Fetching GA Sessions for date %s...", date_str)

        try:
            day_data = fetch_all_paginated_data(
                get_ga_sessions,
                data_key="records",
                limit=limit,
                date=date_str
            )

            if day_data:
                save_json_partitioned(day_data, folder="ga_sessions", date_value=date_str)
            else:
                logger.warning("No GA Session data found for %s.", date_str)

        except Exception as e:
            logger.exception("Error fetching GA Sessions for %s: %s", date_str, e)

        current_date += timedelta(days=1)

    logger.info("✅ Completed GA Sessions extraction for all dates.")


# === Main Runner ===
if __name__ == "__main__":
    try:
        # fetch_daily_visits_by_date(START_DATE, END_DATE, limit=50)
        fetch_ga_sessions_by_date(START_DATE, END_DATE, limit=50)
        logger.info("🎯 All data successfully extracted and saved to '%s/'", DATA_DIR)

    except KeyboardInterrupt:
        logger.warning("Data extraction interrupted by user.")
    except Exception as e:
        logger.exception("Fatal error during extraction: %s", e)
