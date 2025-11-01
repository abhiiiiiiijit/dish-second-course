import requests
import json
import logging
from typing import Optional, Dict, Any

# === Configuration ===
BASE_URL = "https://dish-second-course-gateway-2tximoqc.nw.gateway.dev"
API_KEY = "AIzaSyDMMWBOHgMG1u7P9jX9neaUQHY2vwlBTbM"
TIMEOUT = 10  # seconds

# === Logging Setup ===
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)

# === Session Setup ===
session = requests.Session()
session.headers.update({
    "X-API-Key": API_KEY,
    "Accept": "application/json"
})


def make_api_request(endpoint: str, params: Optional[Dict[str, Any]] = None) -> Optional[Dict[str, Any]]:
    """Helper function to make a GET request with error handling."""
    url = f"{BASE_URL}/{endpoint}"
    try:
        response = session.get(url, params=params, timeout=TIMEOUT)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.Timeout:
        logger.error("Request to %s timed out.", url)
    except requests.exceptions.ConnectionError:
        logger.error("Connection error occurred while connecting to %s", url)
    except requests.exceptions.HTTPError as http_err:
        logger.error("HTTP error %s: %s", response.status_code, response.text)
    except ValueError:
        logger.error("Invalid JSON response from %s", url)
    except Exception as e:
        logger.exception("Unexpected error: %s", e)
    return None


def get_daily_visits(
    page: int = 1,
    limit: int = 10,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None
) -> Optional[Dict[str, Any]]:
    """Fetch daily visit data with optional pagination and date filters."""
    params = {"page": page, "limit": limit}
    if start_date:
        params["start_date"] = start_date
    if end_date:
        params["end_date"] = end_date

    data = make_api_request("daily-visits", params)
    if data:
        logger.info("Fetched %d daily visits.", len(data.get("records", [])))
    return data


def get_ga_sessions(
    page: int = 1,
    limit: int = 5,
    date: Optional[str] = None,
    country: Optional[str] = None,
    device_category: Optional[str] = None
) -> Optional[Dict[str, Any]]:
    """Fetch GA session data with optional filters."""
    params = {"page": page, "limit": limit}
    if date:
        params["date"] = date
    if country:
        params["country"] = country
    if device_category:
        params["device_category"] = device_category

    data = make_api_request("ga-sessions-data", params)
    if data:
        logger.info("Fetched GA sessions with pagination info: %s", json.dumps(data.get("pagination", {}), indent=2))
    return data


if __name__ == "__main__":
    logger.info("Fetching GA sessions example...")
    ga_data = get_ga_sessions()
    if ga_data:
        logger.info("GA Sessions (sample): %s", json.dumps(ga_data, indent=2))
    else:
        logger.warning("No GA session data returned.")
