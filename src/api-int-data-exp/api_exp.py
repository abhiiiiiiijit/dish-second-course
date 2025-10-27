#!/usr/bin/env python3
"""
api_exploration.py
Correct usage of X-API-Key header with the dish-second-course API.
"""

import requests
import json
from typing import Optional, Dict, Any, List

BASE_URL = "https://dish-second-course-gateway-2tximoqc.nw.gateway.dev"
API_KEY = "AIzaSyDMMWBOHgMG1u7P9jX9neaUQHY2vwlBTbM"  # provided by you

# Create a session and attach the API key header so every request includes it
session = requests.Session()
session.headers.update({
    "X-API-Key": API_KEY,
    "Accept": "application/json"
})

def get_daily_visits( start_date=None, end_date=None):
    """Fetch daily visit data with optional date range filters."""
    params = {}
    if start_date:
        params["start_date"] = start_date
    if end_date:
        params["end_date"] = end_date

    url = f"{BASE_URL}/daily-visits"   # ✅ FIXED endpoint (added hyphen)
    response = session.get(url, params=params)

    if response.status_code == 200:
        data = response.json()
        print("\n=== Daily Visits Sample ===")
        print(json.dumps(data, indent=2))
        return data
    else:
        print("[daily-visits] HTTP", response.status_code, ":", response.text)
        return None
# --- 1. Daily Visits (Flat Structure) ---
# def get_daily_visits(page=1, limit=10, start_date=None, end_date=None):
#     """Fetch daily visit data with optional date range filters."""
#     params = {"page": page, "limit": limit}
#     if start_date:
#         params["start_date"] = start_date
#     if end_date:
#         params["end_date"] = end_date

#     url = f"{BASE_URL}/daily-visits"   # ✅ FIXED endpoint (added hyphen)
#     response = session.get(url, params=params)

#     if response.status_code == 200:
#         data = response.json()
#         print("\n=== Daily Visits Sample ===")
#         print(json.dumps(data, indent=2))
#         return data
#     else:
#         print("[daily-visits] HTTP", response.status_code, ":", response.text)
#         return None

# def get_ga_sessions(date=None, country=None, device_category=None):
#     """Fetch GA session data with optional filters."""
#     params = {}
#     if date:
#         params["date"] = date
#     if country:
#         params["country"] = country
#     if device_category:
#         params["device_category"] = device_category

#     url = f"{BASE_URL}/ga-sessions-data"   # ✅ FIXED endpoint (added hyphens)
#     response = session.get(url, params=params)

#     if response.status_code == 200:
#         data = response.json()
#         print("\n=== GA Sessions Sample ===")
#         print(json.dumps(data['pagination'], indent=2))
#         return data
#     else:
#         print("[ga-sessions-data] HTTP", response.status_code, ":", response.text)
#         return None
# --- 2. GA Sessions (Nested Structure) ---
def get_ga_sessions(page=1, limit=5, date=None, country=None, device_category=None):
    """Fetch GA session data with optional filters."""
    params = {"page": page, "limit": limit}
    if date:
        params["date"] = date
    if country:
        params["country"] = country
    if device_category:
        params["device_category"] = device_category

    url = f"{BASE_URL}/ga-sessions-data"   # ✅ FIXED endpoint (added hyphens)
    response = session.get(url, params=params)

    if response.status_code == 200:
        data = response.json()
        print("\n=== GA Sessions Sample ===")
        print(json.dumps(data, indent=2))
        return data
    else:
        print("[ga-sessions-data] HTTP", response.status_code, ":", response.text)
        return None



# def pretty_print_sample(title: str, data: Dict[str, Any], show_n: int = 2):
#     print(f"\n--- {title} ---")
#     if not data:
#         print("(no data)")
#         return
#     records = data.get("records", data)  # fallback to entire object
#     if isinstance(records, list):
#         sample = records[:show_n]
#         print(json.dumps(sample, indent=2))
#     else:
#         print(json.dumps(data, indent=2))


if __name__ == "__main__":
    # Example 1: daily visits for a small date window
    # print("Fetching daily visits (2016-08-01 to 2016-08-05)...")
    # dv = get_daily_visits()
    # pretty_print_sample("Daily Visits (sample)", dv)

    # Example 2: GA sessions for a specific date, country, device
    print("\nFetching GA sessions (date=20160801, country=United States, device_category=desktop)...")
    ga = get_ga_sessions()
    # pretty_print_sample("GA Sessions (sample)", ga)

    # Example 3: show how to page through daily visits (WARNING: might be many pages)
    # small example: only fetch limited pages by using limit=50 and stopping early if you want.
    # all_daily = fetch_all_daily_visits(start_date="2016-08-01", end_date="2016-08-10", limit=50)
    # print(f"\nFetched {len(all_daily)} daily records in total (sample):")
    # print(json.dumps(all_daily[:3], indent=2))
