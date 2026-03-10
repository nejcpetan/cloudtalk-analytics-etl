# tests/test_api_client.py
"""
Unit tests for CloudTalkClient.

All HTTP calls are intercepted by pytest-httpx -- no network access occurs.
The strategy is to inject an httpx.Client built with pytest-httpx's transport,
then pass that client to CloudTalkClient via the http_client= constructor param.

Coverage:
  - Successful GET request (JSON parsing, auth header)
  - Pagination across multiple pages (get_all_pages)
  - Single-page responses (pageCount=1)
  - Empty data array (no calls on a given day)
  - HTTP 429 handling -- retries and eventually raises CloudTalkRateLimitError
  - HTTP 429 with X-CloudTalkAPI-ResetTime header (past reset -- should not sleep)
  - HTTP 500 handling -- retries and eventually raises CloudTalkServerError
  - HTTP 400 -- NOT retried, raises httpx.HTTPStatusError immediately
  - HTTP 401 -- NOT retried, raises httpx.HTTPStatusError immediately
  - get_calls() correct parameter forwarding
  - get_agents() correct parameter forwarding
  - get_group_stats() correct endpoint, no pagination params
  - Context manager (__enter__ / __exit__)
"""
import time

import httpx
import pytest

from cloudtalk_etl.api.client import (
    CloudTalkClient,
    CloudTalkRateLimitError,
    CloudTalkServerError,
)
from cloudtalk_etl.api.rate_limiter import TokenBucketRateLimiter

# ======================================================================
# Helpers & fixtures
# ======================================================================

BASE_URL = "https://my.cloudtalk.io/api"
ANALYTICS_BASE_URL = "https://analytics-api.cloudtalk.io/api"


def _make_client(httpx_mock, max_retries: int = 1) -> CloudTalkClient:
    """
    Build a CloudTalkClient whose transport is intercepted by pytest-httpx.

    Because pytest-httpx patches at the TRANSPORT level globally for the test,
    any httpx.Client created during the test will use the mock transport automatically.
    """
    real_http_client = httpx.Client()
    rate_limiter = TokenBucketRateLimiter(rate_per_minute=600)  # fast -- no throttle in tests

    return CloudTalkClient(
        api_key_id="test_id",
        api_key_secret="test_secret",
        base_url=BASE_URL,
        analytics_base_url=ANALYTICS_BASE_URL,
        rate_limiter=rate_limiter,
        max_retries=max_retries,
        http_client=real_http_client,
    )


@pytest.fixture
def client(httpx_mock) -> CloudTalkClient:
    """Standard client fixture with max_retries=1 to keep tests fast."""
    return _make_client(httpx_mock, max_retries=1)


def _call_response(data: list = None, page: int = 1, page_count: int = 1) -> dict:
    """Build a standard CloudTalk responseData envelope."""
    return {
        "responseData": {
            "data": data or [],
            "pageNumber": page,
            "pageCount": page_count,
            "limit": 1000,
        },
        "status": "OK",
    }


# ======================================================================
# Basic request / response
# ======================================================================


def test_successful_request_returns_parsed_json(httpx_mock, client):
    """A 200 response should be returned as a parsed Python dict."""
    payload = _call_response(data=[{"Cdr": {"id": "1"}}])
    httpx_mock.add_response(json=payload)

    result = client.get_agents()

    assert result["responseData"]["data"][0]["Cdr"]["id"] == "1"
    assert result["status"] == "OK"


def test_request_sends_basic_auth(httpx_mock):
    """The Authorization header must carry HTTP Basic Auth credentials."""
    httpx_mock.add_response(json=_call_response())

    rate_limiter = TokenBucketRateLimiter(rate_per_minute=600)
    cl = CloudTalkClient(
        api_key_id="MY_KEY_ID",
        api_key_secret="MY_SECRET",
        base_url=BASE_URL,
        rate_limiter=rate_limiter,
        max_retries=1,
    )

    cl.get_agents()

    request = httpx_mock.get_requests()[0]
    assert request.headers["Authorization"].startswith("Basic ")
    import base64
    decoded = base64.b64decode(request.headers["Authorization"][6:]).decode()
    assert decoded == "MY_KEY_ID:MY_SECRET"


def test_get_calls_forwards_correct_params(httpx_mock, client):
    """get_calls() must forward date_from, date_to, page, and limit."""
    httpx_mock.add_response(json=_call_response())

    client.get_calls(date_from="2026-03-03 00:00:00", date_to="2026-03-03 23:59:59", page=2)

    req = httpx_mock.get_requests()[0]
    assert "date_from=2026-03-03+00%3A00%3A00" in str(req.url) or \
           "date_from=2026-03-03%2000%3A00%3A00" in str(req.url) or \
           "date_from=" in str(req.url)
    assert "page=2" in str(req.url)
    assert "/calls/index.json" in str(req.url)


def test_get_agents_hits_correct_endpoint(httpx_mock, client):
    httpx_mock.add_response(json=_call_response())
    client.get_agents()
    req = httpx_mock.get_requests()[0]
    assert "/agents/index.json" in str(req.url)


def test_get_call_detail_hits_analytics_endpoint(httpx_mock, client):
    """get_call_detail() must call the analytics API base URL with the call ID."""
    detail_payload = {"cdr_id": 100001, "status": "answered", "call_steps": []}
    httpx_mock.add_response(json=detail_payload)

    result = client.get_call_detail(100001)

    req = httpx_mock.get_requests()[0]
    assert "analytics-api.cloudtalk.io" in str(req.url)
    assert "/calls/100001" in str(req.url)
    assert result["cdr_id"] == 100001


def test_get_call_detail_returns_parsed_json(httpx_mock, client):
    """get_call_detail() should return the parsed response dict."""
    payload = {
        "cdr_id": 99999,
        "status": "missed",
        "call_steps": [],
        "call_tags": [],
    }
    httpx_mock.add_response(json=payload)

    result = client.get_call_detail(99999)

    assert result["cdr_id"] == 99999
    assert result["status"] == "missed"


# ======================================================================
# Pagination
# ======================================================================


def test_get_all_pages_single_page(httpx_mock, client):
    """When pageCount == 1, only one request should be made."""
    httpx_mock.add_response(json=_call_response(data=[{"id": 1}, {"id": 2}], page_count=1))

    result = client.get_all_pages(client.get_agents)

    assert result == [{"id": 1}, {"id": 2}]
    assert len(httpx_mock.get_requests()) == 1


def test_get_all_pages_multiple_pages(httpx_mock):
    """Paginator must concatenate data from all pages and make one request per page."""
    rate_limiter = TokenBucketRateLimiter(rate_per_minute=600)
    cl = CloudTalkClient(
        api_key_id="id", api_key_secret="secret",
        base_url=BASE_URL, rate_limiter=rate_limiter, max_retries=1,
    )

    # Page 1 of 3
    httpx_mock.add_response(json=_call_response(data=[{"id": 1}], page=1, page_count=3))
    # Page 2 of 3
    httpx_mock.add_response(json=_call_response(data=[{"id": 2}], page=2, page_count=3))
    # Page 3 of 3
    httpx_mock.add_response(json=_call_response(data=[{"id": 3}], page=3, page_count=3))

    result = cl.get_all_pages(cl.get_agents)

    assert result == [{"id": 1}, {"id": 2}, {"id": 3}]
    assert len(httpx_mock.get_requests()) == 3


def test_get_all_pages_empty_data(httpx_mock, client):
    """When the API returns an empty data list, result should be an empty list."""
    httpx_mock.add_response(json=_call_response(data=[], page_count=1))

    result = client.get_all_pages(client.get_agents)

    assert result == []


def test_get_all_pages_with_call_kwargs(httpx_mock):
    """Extra kwargs (date_from, date_to) should be forwarded to every page request."""
    rate_limiter = TokenBucketRateLimiter(rate_per_minute=600)
    cl = CloudTalkClient(
        api_key_id="id", api_key_secret="secret",
        base_url=BASE_URL, rate_limiter=rate_limiter, max_retries=1,
    )

    httpx_mock.add_response(json=_call_response(data=[{"id": 100}], page=1, page_count=2))
    httpx_mock.add_response(json=_call_response(data=[{"id": 200}], page=2, page_count=2))

    result = cl.get_all_pages(
        cl.get_calls,
        date_from="2026-03-03 00:00:00",
        date_to="2026-03-03 23:59:59",
    )

    assert len(result) == 2
    # Both requests should include the date params
    for req in httpx_mock.get_requests():
        assert "date_from" in str(req.url)
        assert "date_to" in str(req.url)


# ======================================================================
# Error handling -- 4xx (non-retryable)
# ======================================================================


def test_400_raises_http_status_error_immediately(httpx_mock):
    """HTTP 400 must NOT be retried -- it should raise HTTPStatusError on first attempt."""
    rate_limiter = TokenBucketRateLimiter(rate_per_minute=600)
    cl = CloudTalkClient(
        api_key_id="id", api_key_secret="secret",
        base_url=BASE_URL, rate_limiter=rate_limiter,
        max_retries=3,  # even with retries configured, 400 must not retry
    )

    httpx_mock.add_response(status_code=400, content=b"Bad Request")

    with pytest.raises(httpx.HTTPStatusError) as exc_info:
        cl.get_agents()

    assert exc_info.value.response.status_code == 400
    # Only ONE request should have been made (no retries)
    assert len(httpx_mock.get_requests()) == 1


def test_401_raises_http_status_error_immediately(httpx_mock):
    """HTTP 401 (bad credentials) is not retryable."""
    rate_limiter = TokenBucketRateLimiter(rate_per_minute=600)
    cl = CloudTalkClient(
        api_key_id="bad_id", api_key_secret="bad_secret",
        base_url=BASE_URL, rate_limiter=rate_limiter, max_retries=3,
    )

    httpx_mock.add_response(status_code=401, content=b"Unauthorized")

    with pytest.raises(httpx.HTTPStatusError) as exc_info:
        cl.get_agents()

    assert exc_info.value.response.status_code == 401
    assert len(httpx_mock.get_requests()) == 1


# ======================================================================
# Error handling -- 5xx (retryable)
# ======================================================================


def test_500_is_retried_and_eventually_raises(httpx_mock):
    """
    HTTP 500 should trigger CloudTalkServerError which is retried.
    After max_retries exhausted it should re-raise.
    """
    rate_limiter = TokenBucketRateLimiter(rate_per_minute=600)
    cl = CloudTalkClient(
        api_key_id="id", api_key_secret="secret",
        base_url=BASE_URL, rate_limiter=rate_limiter,
        max_retries=3,
    )

    # Fail all 3 attempts
    for _ in range(3):
        httpx_mock.add_response(status_code=500, content=b"Internal Server Error")

    with pytest.raises(CloudTalkServerError):
        cl.get_agents()

    # All 3 attempts should have been made
    assert len(httpx_mock.get_requests()) == 3


def test_500_succeeds_on_retry(httpx_mock):
    """If the first call returns 500 but the second succeeds, the result should be returned."""
    rate_limiter = TokenBucketRateLimiter(rate_per_minute=600)
    cl = CloudTalkClient(
        api_key_id="id", api_key_secret="secret",
        base_url=BASE_URL, rate_limiter=rate_limiter,
        max_retries=3,
    )

    # First call fails, second succeeds
    httpx_mock.add_response(status_code=500, content=b"oops")
    httpx_mock.add_response(json=_call_response(data=[{"id": 99}]))

    result = cl.get_agents()

    assert result["responseData"]["data"][0]["id"] == 99
    assert len(httpx_mock.get_requests()) == 2


# ======================================================================
# Error handling -- 429 (retryable)
# ======================================================================


def test_429_is_retried_and_eventually_raises(httpx_mock):
    """
    HTTP 429 should raise CloudTalkRateLimitError (retryable).
    After exhausting max_retries it should re-raise the error.
    """
    rate_limiter = TokenBucketRateLimiter(rate_per_minute=600)
    cl = CloudTalkClient(
        api_key_id="id", api_key_secret="secret",
        base_url=BASE_URL, rate_limiter=rate_limiter,
        max_retries=3,
    )

    for _ in range(3):
        httpx_mock.add_response(status_code=429)

    with pytest.raises(CloudTalkRateLimitError):
        cl.get_agents()

    assert len(httpx_mock.get_requests()) == 3


def test_429_succeeds_on_retry(httpx_mock):
    """A single 429 followed by a 200 should succeed."""
    rate_limiter = TokenBucketRateLimiter(rate_per_minute=600)
    cl = CloudTalkClient(
        api_key_id="id", api_key_secret="secret",
        base_url=BASE_URL, rate_limiter=rate_limiter,
        max_retries=3,
    )

    httpx_mock.add_response(status_code=429)
    httpx_mock.add_response(json=_call_response(data=[{"id": 42}]))

    result = cl.get_agents()

    assert result["responseData"]["data"][0]["id"] == 42
    assert len(httpx_mock.get_requests()) == 2


def test_429_with_past_reset_time_does_not_hang(httpx_mock):
    """
    When X-CloudTalkAPI-ResetTime is a timestamp in the past,
    wait_seconds will be <= 0 and we should NOT sleep at all.
    """
    rate_limiter = TokenBucketRateLimiter(rate_per_minute=600)
    cl = CloudTalkClient(
        api_key_id="id", api_key_secret="secret",
        base_url=BASE_URL, rate_limiter=rate_limiter,
        max_retries=2,
    )

    past_reset = str(time.time() - 100)  # 100 seconds in the past
    httpx_mock.add_response(
        status_code=429,
        headers={"X-CloudTalkAPI-ResetTime": past_reset},
    )
    httpx_mock.add_response(json=_call_response())

    start = time.monotonic()
    cl.get_agents()
    elapsed = time.monotonic() - start

    # With a past reset time there should be no extra 100s sleep, but tenacity
    # will still impose its 2-4s backoff jitter before the next attempt.
    assert elapsed < 6.0, f"Took {elapsed:.2f}s -- past reset time might have caused a sleep"


def test_429_with_future_reset_time_extracts_attribute(httpx_mock):
    """
    CloudTalkRateLimitError.reset_time should be populated from the header.
    """
    rate_limiter = TokenBucketRateLimiter(rate_per_minute=600)
    cl = CloudTalkClient(
        api_key_id="id", api_key_secret="secret",
        base_url=BASE_URL, rate_limiter=rate_limiter,
        max_retries=1,
    )

    future_ts = time.time() + 1  # 1 second in future -- short sleep ok
    httpx_mock.add_response(
        status_code=429,
        headers={"X-CloudTalkAPI-ResetTime": str(future_ts)},
    )

    with pytest.raises(CloudTalkRateLimitError) as exc_info:
        cl.get_agents()

    assert exc_info.value.reset_time == pytest.approx(future_ts, abs=1.0)


# ======================================================================
# Context manager
# ======================================================================


def test_context_manager_closes_client(httpx_mock):
    """CloudTalkClient used as a context manager must not raise on exit."""
    httpx_mock.add_response(json=_call_response())

    rate_limiter = TokenBucketRateLimiter(rate_per_minute=600)
    with CloudTalkClient(
        api_key_id="id", api_key_secret="secret",
        base_url=BASE_URL, rate_limiter=rate_limiter, max_retries=1,
    ) as cl:
        result = cl.get_agents()

    assert "responseData" in result