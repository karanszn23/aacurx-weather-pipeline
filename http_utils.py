import logging
import random
import time
import requests
from config import (
    HTTP_MAX_RETRIES,
    HTTP_BACKOFF_BASE_SECONDS,
    HTTP_BACKOFF_MAX_SECONDS,
    HTTP_TIMEOUT_SECONDS,
)

logger = logging.getLogger(__name__)

RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}


def _sleep_seconds(attempt):
    base = HTTP_BACKOFF_BASE_SECONDS * (2 ** attempt)
    jitter = random.uniform(0, 0.5)
    return min(base + jitter, HTTP_BACKOFF_MAX_SECONDS)


def request_json_with_retries(url, params=None, timeout_seconds=HTTP_TIMEOUT_SECONDS):
    last_error = None
    for attempt in range(HTTP_MAX_RETRIES + 1):
        try:
            response = requests.get(url, params=params, timeout=timeout_seconds)
            if response.status_code in RETRYABLE_STATUS_CODES:
                if attempt == HTTP_MAX_RETRIES:
                    response.raise_for_status()
                retry_after = response.headers.get("Retry-After")
                try:
                    parsed = float(retry_after) if retry_after else None
                except ValueError:
                    parsed = None
                sleep_time = parsed if (parsed is not None and parsed > 0) else _sleep_seconds(attempt)
                logger.warning(
                    "HTTP %s from %s (attempt %d/%d); retrying in %.1fs",
                    response.status_code, url, attempt + 1, HTTP_MAX_RETRIES + 1, sleep_time,
                )
                time.sleep(sleep_time)
                continue
            response.raise_for_status()
            return response.json()
        except requests.RequestException as exc:
            last_error = exc
            if attempt == HTTP_MAX_RETRIES:
                break
            sleep_time = _sleep_seconds(attempt)
            logger.warning(
                "Request error for %s (attempt %d/%d): %s; retrying in %.1fs",
                url, attempt + 1, HTTP_MAX_RETRIES + 1, exc, sleep_time,
            )
            time.sleep(sleep_time)

    raise RuntimeError(
        f"Request failed after {HTTP_MAX_RETRIES + 1} attempts for {url}: {last_error}"
    )
