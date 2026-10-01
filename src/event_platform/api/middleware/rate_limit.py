"""HTTP middleware enforcing per-client rate limits.

Clients are identified by IP. Behind a load balancer run uvicorn with
``--proxy-headers --forwarded-allow-ips=<lb>`` so ``request.client`` is the real
client and not the proxy. The limiter fails open: if Redis is down, requests are
served rather than rejected, because availability of ingestion matters more than
abuse protection during a cache outage.
"""

import logging
from collections.abc import Callable

from fastapi import Request, Response, status
from fastapi.responses import JSONResponse
from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from starlette.types import ASGIApp

from event_platform.application.errors import DependencyUnavailableError
from event_platform.application.rate_limiting import RateLimitDecision, RateLimiter

logger = logging.getLogger(__name__)

EXEMPT_PATH_PREFIXES = ("/health", "/docs", "/openapi.json")

LimiterProvider = Callable[[Request], RateLimiter | None]


class RateLimitMiddleware(BaseHTTPMiddleware):
    def __init__(self, app: ASGIApp, limiter_provider: LimiterProvider) -> None:
        super().__init__(app)
        self._limiter_provider = limiter_provider

    async def dispatch(self, request: Request, call_next: RequestResponseEndpoint) -> Response:
        limiter = self._limiter_provider(request)
        if limiter is None or request.url.path.startswith(EXEMPT_PATH_PREFIXES):
            return await call_next(request)

        try:
            decision = await limiter.hit(_client_key(request))
        except DependencyUnavailableError:
            logger.warning("Rate limiter unavailable, allowing request")
            return await call_next(request)

        if not decision.allowed:
            response: Response = JSONResponse(
                status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                content={"detail": "Rate limit exceeded"},
                headers={"Retry-After": str(decision.reset_after_seconds)},
            )
        else:
            response = await call_next(request)
        response.headers.update(_rate_limit_headers(decision))
        return response


def _client_key(request: Request) -> str:
    return request.client.host if request.client else "unknown"


def _rate_limit_headers(decision: RateLimitDecision) -> dict[str, str]:
    return {
        "X-RateLimit-Limit": str(decision.limit),
        "X-RateLimit-Remaining": str(decision.remaining),
        "X-RateLimit-Reset": str(decision.reset_after_seconds),
    }
