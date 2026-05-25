"""Broker configuration."""

from pydantic import BaseModel, Field


class HAConfig(BaseModel):
    enabled: bool = False
    peers: list[str] = Field(default_factory=list)  # host:port pairs
    heartbeat_interval: float = 1.0   # seconds
    failover_timeout: float = 5.0     # seconds before promoting standby


class BrokerConfig(BaseModel):
    host: str = "0.0.0.0"
    port: int = 1883
    max_keepalive: int = 65535
    ha: HAConfig = Field(default_factory=HAConfig)
