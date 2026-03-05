# src/cloudtalk_etl/api/client.py
"""
CloudTalk REST API client.

Implements authenticated HTTP Basic Auth requests against the CloudTalk API v1.7.
All calls are rate-limited through a shared TokenBucketRateLimiter and retried
automatically on transient failures using tenacity.

Error handling strategy (from TECHNICAL_SPEC Section 9.1):
  - 429  -> CloudTalkRateLimitError  (retryable -- wait for X-CloudTalkAPI-ResetTime)
  - 5xx  -> CloudTalkServerError     (retryable -- exponential backoff with jitter)
  - 4xx  -> HTTPStatusError          (NOT retryable -- log and re-raise immediately)
  - network / transport            -> httpx.TransportError (retryable)
"""
import time
import logging

import httpx
import structlog
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential_jitter,
    before_sleep_log,
)

from cloudtalk_etl.api.rate_limiter import TokenBucketRateLimiter

logger = structlog.get_logger()

# ======================================================================
# Custom exceptions
# ======================================================================


class CloudTalkAPIError(Exception):
    """Base exception for all CloudTalk API errors."""


class CloudTalkRateLimitError(CloudTalkAPIError):
    """
    Raised when the API responds with HTTP 429 Too Many Requests.

    Captures the reset timestamp from the X-CloudTalkAPI-ResetTime header
    so the retry decorator can wait for the right amount of time.
    """

    def __init__(self, reset_time: float | None = None) -> None:
        self.reset_time = reset_time
        super().__init__(f"Rate limited by CloudTalk API. Reset at unix ts: {reset_time}")


class CloudTalkServerError(CloudTalkAPIError):
    """Raised on HTTP 5xx responses -- treated as transient and retried."""

    def __init__(self, status_code: int, body: str) -> None:
        self.status_code = status_code
        super().__init__(f"Server error {status_code}: {body[:200]}")


# ======================================================================
# Client
# ======================================================================


class CloudTalkClient:
    """
    Synchronous HTTP client for the CloudTalk REST API (v1.7).

    Uses httpx for transport, Basic Auth for authentication, a shared
    TokenBucketRateLimiter to stay below 50 req/min, and tenacity to
    retry on transient failures.

    Args:
        api_key_id:     CloudTalk API key ID   (from CLOUDTALK_API_KEY_ID env var)
        api_key_secret: CloudTalk API secret   (from CLOUDTALK_API_KEY_SECRET env var)
        base_url:       API base URL           (default: https://my.cloudtalk.io/api)
        rate_limiter:   Shared TokenBucketRateLimiter instance
        max_retries:    Maximum retry attempts (default: 5)
        http_client:    Optional pre-built httpx.Client -- used in tests to inject
                        a pytest-httpx mock transport. If None, a default client
                        with a 30 s timeout is created.
    """

    def __init__(
        self,
        api_key_id: str,
        api_key_secret: str,
        base_url: str,
        rate_limiter: TokenBucketRateLimiter,
        max_retries: int = 5,
        http_client: httpx.Client | None = None,
    ) -> None:
        self._auth = (api_key_id, api_key_secret)
        self._base_url = base_url.rstrip("/")
        self._rate_limiter = rate_limiter
        self._max_retries = max_retries
        self._client = http_client or httpx.Client(
            timeout=httpx.Timeout(30.0, connect=10.0),
            headers={"Accept": "application/json"},
        )

    # ------------------------------------------------------------------
    # Core request method (with retry)
    # ------------------------------------------------------------------

    def _request(self, method: str, endpoint: str, params: dict | None = None) -> dict:
        """
        Make a rate-limited, retried request to the CloudTalk API.

        The @retry decorator is built dynamically in __init__ so the
        attempt count can be configured at runtime. We use a helper
        method _do_request() and wrap it here.
        """
        @retry(
            stop=stop_after_attempt(self._max_retries),
            wait=wait_exponential_jitter(initial=2, max=60, jitter=2),
            retry=retry_if_exception_type(
                (CloudTalkRateLimitError, CloudTalkServerError, httpx.TransportError)
            ),
            before_sleep=before_sleep_log(logger, logging.WARNING),
            reraise=True,
        )
        def _attempt() -> dict:
            return self._do_request(method, endpoint, params)

        return _attempt()

    def _do_request(self, method: str, endpoint: str, params: dict | None) -> dict:
        """
        Execute a single HTTP request (no retry logic here -- that's in _request).

        Raises:
            CloudTalkRateLimitError: on HTTP 429
            CloudTalkServerError:    on HTTP 5xx
            httpx.HTTPStatusError:   on HTTP 4xx (not retried upstream)
        """
        self._rate_limiter.wait()  # block until we have capacity

        url = f"{self._base_url}{endpoint}"
        response = self._client.request(method, url, params=params, auth=self._auth)

        # -- 429 Too Many Requests --------------------------------------
        if response.status_code == 429:
            reset_header = response.headers.get("X-CloudTalkAPI-ResetTime")
            reset_time: float | None = None

            if reset_header:
                try:
                    reset_time = float(reset_header)
                    wait_seconds = reset_time - time.time()
                    if wait_seconds > 0:
                        logger.warning(
                            "rate_limit_header_sleep",
                            wait_seconds=round(wait_seconds, 1),
                            reset_at=reset_time,
                        )
                        time.sleep(wait_seconds)
                except (ValueError, TypeError):
                    pass

            raise CloudTalkRateLimitError(reset_time=reset_time)

        # -- 5xx Server Errors -----------------------------------------
        if response.status_code >= 500:
            raise CloudTalkServerError(
                status_code=response.status_code,
                body=response.text,
            )

        # -- 4xx Client Errors (not retried) ---------------------------
        if response.status_code >= 400:
            logger.error(
                "client_error",
                status=response.status_code,
                endpoint=endpoint,
                body=response.text[:500],
            )
            response.raise_for_status()  # raises httpx.HTTPStatusError

        return response.json()

    # ------------------------------------------------------------------
    # API endpoint methods
    # ------------------------------------------------------------------

    def get_calls(
        self,
        date_from: str,
        date_to: str,
        page: int = 1,
        limit: int = 1000,
    ) -> dict:
        """
        Fetch call history for a date range.

        Args:
            date_from: Start datetime string, e.g. "2026-03-03 00:00:00"
            date_to:   End datetime string,   e.g. "2026-03-03 23:59:59"
            page:      Page number (1-indexed)
            limit:     Records per page (max 1000)

        Returns:
            Raw API response dict containing responseData envelope.
        """
        return self._request(
            "GET",
            "/calls/index.json",
            params={
                "date_from": date_from,
                "date_to": date_to,
                "page": page,
                "limit": limit,
            },
        )

    def get_agents(self, page: int = 1, limit: int = 1000) -> dict:
        """
        Fetch all agents (with their current status and contact details).

        Args:
            page:  Page number (1-indexed)
            limit: Records per page (max 1000)

        Returns:
            Raw API response dict containing responseData envelope.
        """
        return self._request(
            "GET",
            "/agents/index.json",
            params={"page": page, "limit": limit},
        )

    def get_group_stats(self) -> dict:
        """
        Fetch a real-time snapshot of group-level statistics.

        This endpoint does NOT paginate -- it returns all groups in one response.

        Returns:
            Raw API response dict containing responseData envelope.
        """
        return self._request("GET", "/statistics/realtime/groups.json")

    def get_groups(self, page: int = 1, limit: int = 1000) -> dict:
        """
        Fetch all call center groups (queue groups / "ponorna številke").

        Returns:
            Raw API response dict containing responseData envelope.
        """
        return self._request(
            "GET",
            "/groups/index.json",
            params={"page": page, "limit": limit},
        )

    def get_numbers(self, page: int = 1, limit: int = 1000) -> dict:
        """
        Fetch all registered phone numbers with their routing configuration.

        Returns:
            Raw API response dict containing responseData envelope.
        """
        return self._request(
            "GET",
            "/numbers/index.json",
            params={"page": page, "limit": limit},
        )

    def get_tags(self, page: int = 1, limit: int = 1000) -> dict:
        """
        Fetch all call reason tags.

        Returns:
            Raw API response dict containing responseData envelope.
        """
        return self._request(
            "GET",
            "/tags/index.json",
            params={"page": page, "limit": limit},
        )

    # ------------------------------------------------------------------
    # Generic paginator
    # ------------------------------------------------------------------

    def get_all_pages(self, fetch_fn, max_pages: int | None = None, **kwargs) -> list:
        """
        Generic paginator -- calls fetch_fn repeatedly until all pages are fetched.

        fetch_fn must accept a `page` keyword argument and return a dict with
        the standard CloudTalk responseData envelope::

            {
              "responseData": {
                "data": [...],
                "pageCount": 3,
                "pageNumber": 1,
                "limit": 1000
              }
            }

        Args:
            fetch_fn: A bound method such as self.get_calls or self.get_agents.
            **kwargs: Extra keyword args forwarded to fetch_fn on every call
                      (e.g. date_from, date_to for get_calls).

        Returns:
            Flat list of all records across all pages.
        """
        all_data: list = []
        page = 1

        while True:
            response = fetch_fn(page=page, **kwargs)
            response_data: dict = response.get("responseData", {})
            data: list = response_data.get("data", [])
            all_data.extend(data)

            page_count: int = int(response_data.get("pageCount", 1) or 1)

            logger.debug(
                "page_fetched",
                page=page,
                page_count=page_count,
                records_this_page=len(data),
                total_so_far=len(all_data),
            )

            if page >= page_count:
                break
            if max_pages is not None and page >= max_pages:
                break
            page += 1

        return all_data

    # ------------------------------------------------------------------
    # Resource management
    # ------------------------------------------------------------------

    def close(self) -> None:
        """Close the underlying httpx connection pool."""
        self._client.close()

    def __enter__(self) -> "CloudTalkClient":
        return self

    def __exit__(self, *args) -> None:
        self.close()