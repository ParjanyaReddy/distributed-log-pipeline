"""Typed API Schemas and Request/Response Contracts."""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field


class AttackSimulationRequest(BaseModel):
    """Payload schema for adversary attack simulation triggers."""
    type: Literal["brute_force", "volume_spike", "rare_domain", "beaconing"] = Field(
        default="brute_force",
        description="Type of MITRE ATT&CK attack to inject into the pipeline."
    )


class HealthResponse(BaseModel):
    """Health check payload."""
    status: str = "healthy"
    version: str = "2.4.0"
    service: str = "apex-stream-soc"
    uptime_seconds: float = 0.0
    environment: str = "production"


class StreamingToggleResponse(BaseModel):
    """Response payload for stream pause/resume actions."""
    streaming_active: bool
    status: str
