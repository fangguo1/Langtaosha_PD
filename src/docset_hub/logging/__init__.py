"""Logging helpers for request-level and pipeline-level observability."""

from .frontend_search_logger import record_frontend_search_request

__all__ = ["record_frontend_search_request"]
