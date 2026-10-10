from pydantic import BaseModel, Field, IPvAnyAddress, field_validator
from typing import Optional
from uuid import UUID
from datetime import datetime, timezone

class Event(BaseModel):
    event_id: UUID
    ts: datetime
    client_id: str
    src_ip: Optional[IPvAnyAddress] = None
    method: Optional[str] = None
    domain: Optional[str] = None
    url: Optional[str] = None
    status: Optional[int] = None
    bytes_out: Optional[int] = Field(default=None, ge=0)
    bytes_in: Optional[int] = Field(default=None, ge=0)
    user_agent: Optional[str] = None
    action: Optional[str] = None
    category: Optional[str] = None
    auth_result: Optional[str] = None

    @field_validator('ts')
    @classmethod
    def check_ts_not_future(cls, v: datetime) -> datetime:
        if v.tzinfo is None:
            raise ValueError("Timestamp must not be naive")
        now = datetime.now(timezone.utc)
        if (v - now).total_seconds() > 86400:
            raise ValueError("Timestamp is too far in the future")
        return v
        
    @field_validator('status')
    @classmethod
    def check_status(cls, v: Optional[int]) -> Optional[int]:
        if v is not None and not (100 <= v <= 599):
            raise ValueError("HTTP status code out of range")
        return v
