# src/cloudtalk_etl/api/rate_limiter.py
"""
Token bucket rate limiter for the CloudTalk API.

CloudTalk allows 60 requests/minute per company account.
We self-limit to 50 req/min (configurable) to stay well below that ceiling
and avoid accidental 429s during pagination bursts.

The bucket starts full, refills continuously, and each API call consumes
one token. If no token is available the caller blocks until one refills.
This class is thread-safe but the ETL is single-threaded, so the lock is
purely for correctness if the design ever changes.
"""
import threading
import time


class TokenBucketRateLimiter:
    """
    Token bucket rate limiter.

    Maintains a bucket of tokens that refill at a steady rate.
    Each API call consumes one token. If no tokens are available the
    caller blocks until one becomes available.

    Args:
        rate_per_minute: Maximum requests per minute (default: 50).
    """

    def __init__(self, rate_per_minute: int = 50) -> None:
        if rate_per_minute <= 0:
            raise ValueError("rate_per_minute must be a positive integer")

        self.rate_per_minute = rate_per_minute
        self.interval = 60.0 / rate_per_minute  # seconds per token
        self._lock = threading.Lock()
        self._tokens: float = float(rate_per_minute)  # start full
        self._last_refill: float = time.monotonic()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def wait(self) -> None:
        """
        Block until a token is available, then consume it.

        Calculates elapsed time since the last refill, adds the appropriate
        number of tokens (capped at the bucket size), then either consumes
        immediately or sleeps for the time needed before the next token arrives.
        """
        with self._lock:
            now = time.monotonic()
            elapsed = now - self._last_refill

            # Re-fill bucket proportionally to elapsed time
            new_tokens = elapsed / self.interval
            self._tokens = min(float(self.rate_per_minute), self._tokens + new_tokens)
            self._last_refill = now

            if self._tokens >= 1.0:
                self._tokens -= 1.0
                return  # token available -- proceed immediately

            # Calculate how long until the next token arrives
            wait_time = (1.0 - self._tokens) * self.interval
            self._tokens = 0.0

        # Sleep *outside* the lock so other threads aren't blocked
        time.sleep(wait_time)

        # Re-acquire to mark the bucket as just-consumed after the sleep
        with self._lock:
            self._tokens = 0.0
            self._last_refill = time.monotonic()

    # ------------------------------------------------------------------
    # Helpers -- useful in tests
    # ------------------------------------------------------------------

    @property
    def tokens(self) -> float:
        """Current token count (snapshot -- not synchronized read)."""
        return self._tokens