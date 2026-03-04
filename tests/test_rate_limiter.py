# tests/test_rate_limiter.py
"""
Unit tests for TokenBucketRateLimiter.

Tests verify:
  - Normal instantiation and initial state
  - Tokens deplete on request
  - Bucket refills over time (via monkeypatching time.monotonic)
  - Bucket never overfills beyond rate_per_minute
  - wait() blocks when empty (timing-based, kept tight to avoid slow CI)
  - Invalid rate_per_minute raises ValueError
"""
import threading
import time

import pytest

from cloudtalk_etl.api.rate_limiter import TokenBucketRateLimiter


# ======================================================================
# Fixtures
# ======================================================================


@pytest.fixture
def limiter_50() -> TokenBucketRateLimiter:
    """Standard 50 req/min limiter."""
    return TokenBucketRateLimiter(rate_per_minute=50)


@pytest.fixture
def limiter_fast() -> TokenBucketRateLimiter:
    """High-rate limiter (600 req/min = 1 token every 100 ms) for timing tests."""
    return TokenBucketRateLimiter(rate_per_minute=600)


# ======================================================================
# Construction
# ======================================================================


def test_default_rate():
    limiter = TokenBucketRateLimiter()
    assert limiter.rate_per_minute == 50


def test_custom_rate():
    limiter = TokenBucketRateLimiter(rate_per_minute=30)
    assert limiter.rate_per_minute == 30
    assert pytest.approx(limiter.interval, rel=1e-4) == 2.0  # 60/30


def test_invalid_rate_raises():
    with pytest.raises(ValueError, match="positive"):
        TokenBucketRateLimiter(rate_per_minute=0)

    with pytest.raises(ValueError):
        TokenBucketRateLimiter(rate_per_minute=-10)


def test_starts_full(limiter_50: TokenBucketRateLimiter):
    """Bucket must start at capacity so the first burst doesn't have to wait."""
    assert limiter_50.tokens == pytest.approx(50.0)


# ======================================================================
# Token consumption
# ======================================================================


def test_single_wait_consumes_one_token(limiter_50: TokenBucketRateLimiter):
    """Each wait() call removes exactly one token when tokens are available."""
    before = limiter_50.tokens
    limiter_50.wait()
    # Token count should be very close to (before - 1) because negligible
    # time passed for refill. Allow small tolerance for monotonic drift.
    assert limiter_50.tokens == pytest.approx(before - 1.0, abs=0.1)


def test_multiple_waits_within_capacity(limiter_50: TokenBucketRateLimiter):
    """Calling wait() 10 times in rapid succession should not block (bucket starts full)."""
    start = time.monotonic()
    for _ in range(10):
        limiter_50.wait()
    elapsed = time.monotonic() - start
    # Should complete in well under 1 second (10 tokens immediately available)
    assert elapsed < 1.0, f"10 rapid waits took {elapsed:.2f}s -- limiter is blocking unexpectedly"


# ======================================================================
# Refill behaviour (via monkeypatching)
# ======================================================================


def test_bucket_does_not_overfill(monkeypatch):
    """
    Even if a long time passes, the bucket should cap at rate_per_minute tokens.
    """
    limiter = TokenBucketRateLimiter(rate_per_minute=10)
    # Drain the bucket completely
    for _ in range(10):
        limiter.wait()

    assert limiter.tokens < 1.0  # nearly empty

    # Simulate 1000 minutes passing by manipulating _last_refill
    limiter._last_refill -= 1000 * 60  # type: ignore[operator]

    # The next wait() should refill and cap at 10
    limiter.wait()
    # After consuming one, tokens should be near 9 (capped at 10 - 1)
    assert limiter.tokens <= 10.0
    assert limiter.tokens >= 8.0  # some drift ok


# ======================================================================
# Throttling -- actually blocks when empty
# ======================================================================


def test_limiter_throttles_when_empty():
    """
    A 1 req/min limiter should block ~1 second after the first request.

    We use 60 req/min (= 1 token/second) to keep the test fast.
    Drain the bucket then time a single extra call.
    """
    limiter = TokenBucketRateLimiter(rate_per_minute=60)  # 1 token per second

    # Drain all 60 tokens without sleeping
    for _ in range(60):
        limiter.wait()

    # Now bucket is empty -- next wait() must sleep ~1 second
    start = time.monotonic()
    limiter.wait()
    elapsed = time.monotonic() - start

    # Should have waited at least 0.7 s (generous tolerance for CI)
    assert elapsed >= 0.7, f"Expected throttling but wait returned after {elapsed:.2f}s"
    # Should not have waited more than 2.5 s
    assert elapsed < 2.5, f"Waited too long: {elapsed:.2f}s"


# ======================================================================
# Thread safety -- multiple threads consuming concurrently
# ======================================================================


def test_thread_safety():
    """
    N threads each make M calls. The total number of calls should equal N*M
    without race conditions or assertion errors.
    """
    limiter = TokenBucketRateLimiter(rate_per_minute=600)  # fast to avoid slow test
    n_threads = 5
    calls_per_thread = 10
    results: list[int] = []
    lock = threading.Lock()

    def worker():
        for _ in range(calls_per_thread):
            limiter.wait()
        with lock:
            results.append(1)

    threads = [threading.Thread(target=worker) for _ in range(n_threads)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=10)

    assert len(results) == n_threads, "Not all threads completed"