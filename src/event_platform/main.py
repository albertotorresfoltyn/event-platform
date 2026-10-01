"""Composition root: builds the FastAPI application and wires its dependencies."""

from fastapi import FastAPI

from event_platform.api.routes import health
from event_platform.config import Settings, get_settings
from event_platform.logging_config import configure_logging


def create_app(settings: Settings | None = None) -> FastAPI:
    settings = settings or get_settings()
    configure_logging(settings.log_level)

    app = FastAPI(title=settings.app_name)
    app.include_router(health.router)
    return app


app = create_app()
