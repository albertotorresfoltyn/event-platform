"""Composition root: builds the FastAPI application and wires its dependencies."""

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request

from event_platform.api.errors import register_exception_handlers
from event_platform.api.middleware.rate_limit import RateLimitMiddleware
from event_platform.api.routes import admin, events, health
from event_platform.application.rate_limiting import RateLimiter
from event_platform.config import Settings, get_settings
from event_platform.container import Container, build_container
from event_platform.logging_config import configure_logging


def create_app(settings: Settings | None = None) -> FastAPI:
    settings = settings or get_settings()
    configure_logging(settings.log_level)

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        container = build_container(settings)
        app.state.container = container
        await container.start()
        try:
            yield
        finally:
            await container.stop()

    app = FastAPI(title=settings.app_name, lifespan=lifespan)
    app.add_middleware(RateLimitMiddleware, limiter_provider=_container_rate_limiter)
    register_exception_handlers(app)
    app.include_router(health.router)
    app.include_router(events.router)
    app.include_router(admin.router)
    return app


def _container_rate_limiter(request: Request) -> RateLimiter | None:
    container: Container | None = getattr(request.app.state, "container", None)
    return container.rate_limiter if container else None


app = create_app()
