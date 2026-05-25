"""Broker configuration."""

from pydantic import BaseModel, Field


class HAConfig(BaseModel):
    enabled: bool = False
    role: str = "primary"           # "primary" or "standby"
    ha_host: str = "0.0.0.0"
    ha_port: int = 1884
    peer_host: str = ""
    peer_ha_port: int = 1884
    ping_interval: float = 2.0
    failover_timeout: float = 8.0


class BrokerConfig(BaseModel):
    host: str = "0.0.0.0"
    port: int = 1883
    max_keepalive: int = 65535
    ha: HAConfig = Field(default_factory=HAConfig)
