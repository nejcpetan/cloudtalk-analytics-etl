"""
cloudtalk_etl.api — CloudTalk REST API client package.

Public exports:
    CloudTalkClient        — main HTTP client
    TokenBucketRateLimiter — shared rate limiter
    CloudTalkAPIError      — base exception
    CloudTalkRateLimitError
    CloudTalkServerError
"""
from cloudtalk_etl.api.client import (
    CloudTalkAPIError,
    CloudTalkClient,
    CloudTalkRateLimitError,
    CloudTalkServerError,
)
from cloudtalk_etl.api.rate_limiter import TokenBucketRateLimiter

__all__ = [
    "CloudTalkClient",
    "TokenBucketRateLimiter",
    "CloudTalkAPIError",
    "CloudTalkRateLimitError",
    "CloudTalkServerError",
]
