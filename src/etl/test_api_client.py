import json
import pytest
from unittest.mock import MagicMock, patch
import requests
from etl import api_client



@pytest.fixture
def mock_session_get():
    """Mock requests.Session.get for all tests."""
    with patch.object(api_client.session, "get") as mock:
        yield mock


def test_make_api_request_success(mock_session_get):
    """Should return JSON data when request succeeds."""
    mock_response = MagicMock()
    mock_response.json.return_value = {"status": "ok"}
    mock_response.raise_for_status.return_value = None
    mock_session_get.return_value = mock_response

    resp = api_client.make_api_request("daily-visits", {"page": 1})

    mock_session_get.assert_called_once_with(
        f"{api_client.BASE_URL}/daily-visits",
        params={"page": 1},
        timeout=api_client.TIMEOUT,
    )

    assert resp == {"status": "ok"}


def test_make_api_request_timeout(mock_session_get, caplog):
    """Should return None and log timeout error."""
    mock_session_get.side_effect = requests.exceptions.Timeout

    resp = api_client.make_api_request("ga-sessions-data")

    assert resp is None
    assert "timed out" in caplog.text


def test_make_api_request_http_error(mock_session_get, caplog):
    """Should return None and log HTTP error details."""
    mock_response = MagicMock()
    mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError("Bad request")
    mock_response.status_code = 400
    mock_response.text = "Invalid input"
    mock_session_get.return_value = mock_response

    resp = api_client.make_api_request("daily-visits")

    assert resp is None
    assert "HTTP error" in caplog.text


def test_make_api_request_invalid_json(mock_session_get, caplog):
    """Should handle invalid JSON gracefully."""
    mock_response = MagicMock()
    mock_response.raise_for_status.return_value = None
    mock_response.json.side_effect = ValueError("Invalid JSON")
    mock_session_get.return_value = mock_response

    resp = api_client.make_api_request("ga-sessions-data")

    assert resp is None
    assert "Invalid JSON response" in caplog.text


def test_get_daily_visits_calls_api(monkeypatch):
    """Ensure get_daily_visits builds correct params and delegates to make_api_request."""
    mock_make_api = MagicMock(return_value={"records": []})
    monkeypatch.setattr(api_client, "make_api_request", mock_make_api)

    api_client.get_daily_visits(page=2, limit=20, start_date="2024-02-01", end_date="2024-02-10")

    mock_make_api.assert_called_once_with(
        "daily-visits",
        {"page": 2, "limit": 20, "start_date": "2024-02-01", "end_date": "2024-02-10"},
    )


def test_get_ga_sessions_calls_api(monkeypatch):
    """Ensure get_ga_sessions builds correct params and delegates to make_api_request."""
    mock_make_api = MagicMock(return_value={"pagination": {"page": 1}})
    monkeypatch.setattr(api_client, "make_api_request", mock_make_api)

    api_client.get_ga_sessions(page=3, limit=15, date="2024-02-01", country="US", device_category="mobile")

    mock_make_api.assert_called_once_with(
        "ga-sessions-data",
        {"page": 3, "limit": 15, "date": "2024-02-01", "country": "US", "device_category": "mobile"},
    )
