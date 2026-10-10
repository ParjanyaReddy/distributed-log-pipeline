from datetime import UTC, datetime
from enum import Enum
from typing import Literal
from uuid import UUID

from pydantic import BaseModel, Field, field_validator


class ReasonCode(str, Enum):
    INVALID_JSON = "invalid_json"
    MISSING_REQUIRED_FIELD = "missing_required_field"
    INVALID_UUID = "invalid_uuid"
    NAIVE_TIMESTAMP = "naive_timestamp"
    FUTURE_TIMESTAMP = "future_timestamp"
    INVALID_IP = "invalid_ip"
    STATUS_OUT_OF_RANGE = "status_out_of_range"
    NEGATIVE_BYTES = "negative_bytes"
    UNKNOWN_ACTION = "unknown_action"
    UNKNOWN_AUTH_RESULT = "unknown_auth_result"
    PAYLOAD_TOO_LARGE = "payload_too_large"


class ValidationFailure(Exception):
    def __init__(self, reason_code: ReasonCode, details: str = ""):
        self.reason_code = reason_code
        self.details = details
        super().__init__(f"{reason_code.value}: {details}")


class Event(BaseModel):
    event_id: UUID
    ts: datetime
    client_id: str
    src_ip: str
    method: str
    url: str
    domain: str
    status: int
    bytes_out: int = Field(ge=0)
    bytes_in: int = Field(ge=0)
    user_agent: str
    action: Literal["allowed", "blocked"]
    category: str
    auth_result: Literal["success", "failure", "none"]

    @field_validator('ts')
    @classmethod
    def validate_timestamp(cls, v: datetime) -> datetime:
        if v.tzinfo is None:
            raise ValueError(ReasonCode.NAIVE_TIMESTAMP.value)
        now = datetime.now(UTC)
        if (v - now).total_seconds() > 86400:
            raise ValueError(ReasonCode.FUTURE_TIMESTAMP.value)
        return v

    @field_validator('status')
    @classmethod
    def validate_status(cls, v: int) -> int:
        if not (100 <= v <= 599):
            raise ValueError(ReasonCode.STATUS_OUT_OF_RANGE.value)
        return v
