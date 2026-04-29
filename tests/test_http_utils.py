import pytest
from unittest.mock import MagicMock, patch, call
from requests.exceptions import ConnectionError as RequestsConnectionError
from http_utils import request_json_with_retries, _sleep_seconds


# ---------------------------------------------------------------------------
# _sleep_seconds
# ---------------------------------------------------------------------------

def test_sleep_seconds_increases_with_attempt():
    # Remove jitter by seeding — just verify base grows
    s0 = _sleep_seconds(0)
    s1 = _sleep_seconds(1)
    s2 = _sleep_seconds(2)
    # base doubles each time; with jitter [0, 0.5) the floor should still grow
    assert s1 > s0 * 0.9
    assert s2 > s1 * 0.9


def test_sleep_seconds_capped_at_max():
    import config
    large_attempt = 100
    result = _sleep_seconds(large_attempt)
    assert result <= config.HTTP_BACKOFF_MAX_SECONDS + 0.5  # allow for max jitter


# ---------------------------------------------------------------------------
# request_json_with_retries — success
# ---------------------------------------------------------------------------

@patch("http_utils.time.sleep")
@patch("http_utils.requests.get")
def test_successful_request_returns_json(mock_get, mock_sleep):
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {"results": []}
    mock_get.return_value = mock_response

    result = request_json_with_retries("https://example.com", params={"q": "test"})
    assert result == {"results": []}
    mock_sleep.assert_not_called()


# ---------------------------------------------------------------------------
# request_json_with_retries — retries
# ---------------------------------------------------------------------------

@patch("http_utils.time.sleep")
@patch("http_utils.requests.get")
def test_retries_on_500_then_succeeds(mock_get, mock_sleep):
    fail_response = MagicMock()
    fail_response.status_code = 500
    fail_response.headers = {}

    ok_response = MagicMock()
    ok_response.status_code = 200
    ok_response.json.return_value = {"ok": True}

    mock_get.side_effect = [fail_response, ok_response]

    result = request_json_with_retries("https://example.com")
    assert result == {"ok": True}
    assert mock_get.call_count == 2
    assert mock_sleep.call_count == 1


@patch("http_utils.time.sleep")
@patch("http_utils.requests.get")
def test_retries_on_429_then_succeeds(mock_get, mock_sleep):
    rate_limit = MagicMock()
    rate_limit.status_code = 429
    rate_limit.headers = {"Retry-After": "5"}

    ok_response = MagicMock()
    ok_response.status_code = 200
    ok_response.json.return_value = {"data": 1}

    mock_get.side_effect = [rate_limit, ok_response]

    result = request_json_with_retries("https://example.com")
    assert result == {"data": 1}
    # Should have slept using the Retry-After value
    mock_sleep.assert_called_once_with(5.0)


@patch("http_utils.time.sleep")
@patch("http_utils.requests.get")
def test_raises_after_max_retries_on_connection_error(mock_get, mock_sleep):
    import config
    mock_get.side_effect = RequestsConnectionError("timeout")

    with pytest.raises(RuntimeError, match="Request failed after"):
        request_json_with_retries("https://example.com")

    assert mock_get.call_count == config.HTTP_MAX_RETRIES + 1


@patch("http_utils.time.sleep")
@patch("http_utils.requests.get")
def test_raises_after_max_retries_on_persistent_500(mock_get, mock_sleep):
    import config
    fail_response = MagicMock()
    fail_response.status_code = 500
    fail_response.headers = {}
    fail_response.raise_for_status.side_effect = Exception("500 Server Error")
    mock_get.return_value = fail_response

    with pytest.raises(Exception):
        request_json_with_retries("https://example.com")

    assert mock_get.call_count == config.HTTP_MAX_RETRIES + 1


@patch("http_utils.time.sleep")
@patch("http_utils.requests.get")
def test_non_retryable_status_raises_immediately(mock_get, mock_sleep):
    bad_response = MagicMock()
    bad_response.status_code = 404
    bad_response.raise_for_status.side_effect = Exception("404 Not Found")
    mock_get.return_value = bad_response

    with pytest.raises(Exception, match="404"):
        request_json_with_retries("https://example.com")

    # Should not retry on 404
    assert mock_get.call_count == 1
    mock_sleep.assert_not_called()
