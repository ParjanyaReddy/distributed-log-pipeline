"""Scalable REST API package for telemetry, logs, and simulation."""

from web.api.routes import APIRouter, create_default_router

__all__ = ["APIRouter", "create_default_router"]
