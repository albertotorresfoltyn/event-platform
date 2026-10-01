"""Composition root: builds the FastAPI application and wires its dependencies."""

from fastapi import FastAPI

from event_platform.api.errors import register_exception_handlers
from event_platform.api.routes import events, health
from event_platform.config import Settings, get_settings
from event_platform.container import build_container
from event_platform.logging_config import configure_logging


def create_app(settings: Settings | None = None) -> FastAPI:
    settings = settings or get_settings()
    configure_logging(settings.log_level)

    app = FastAPI(title=settings.app_name)
    app.state.container = build_container(settings)
    register_exception_handlers(app)
    app.include_router(health.router)
    app.include_router(events.router)
    return app


app = create_app()
