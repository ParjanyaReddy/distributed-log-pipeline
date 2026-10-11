"""API Base Abstractions, Response Wrappers, and HTTP Helpers."""

from __future__ import annotations

import json
from dataclasses import asdict, is_dataclass
from datetime import datetime, timezone
from typing import Any


class APIResponse:
    """Standardized HTTP response envelope."""

    def __init__(
        self,
        status_code: int = 200,
        content: Any = None,
        content_type: str = "application/json; charset=utf-8",
    ) -> None:
        self.status_code = status_code
        self.content = content
        self.content_type = content_type

    def serialize(self) -> bytes:
        """Serializes content into encoded bytes."""
        if isinstance(self.content, (bytes, bytearray)):
            return bytes(self.content)

        if isinstance(self.content, str):
            return self.content.encode("utf-8")

        def default_serializer(obj: Any) -> Any:
            if isinstance(obj, datetime):
                return obj.isoformat()
            if is_dataclass(obj) and not isinstance(obj, type):
                return asdict(obj)
            if hasattr(obj, "model_dump"):
                return obj.model_dump(mode="json")
            if hasattr(obj, "dict"):
                return obj.dict()
            return str(obj)

        return json.dumps(self.content, default=default_serializer).encode("utf-8")


def json_response(data: Any, status_code: int = 200) -> APIResponse:
    """Creates a standard 200/2xx JSON APIResponse."""
    return APIResponse(status_code=status_code, content=data)


def error_response(
    message: str,
    status_code: int = 400,
    code: str = "BAD_REQUEST",
    details: Any = None,
) -> APIResponse:
    """Creates a consistent error envelope response."""
    payload = {
        "success": False,
        "error": {
            "code": code,
            "message": message,
            "details": details,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        },
    }
    return APIResponse(status_code=status_code, content=payload)
