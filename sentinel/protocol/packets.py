"""MQTT packet type definitions and constants."""

from enum import IntEnum
from dataclasses import dataclass, field


class PacketType(IntEnum):
    CONNECT     = 1
    CONNACK     = 2
    PUBLISH     = 3
    PUBACK      = 4
    PUBREC      = 5
    PUBREL      = 6
    PUBCOMP     = 7
    SUBSCRIBE   = 8
    SUBACK      = 9
    UNSUBSCRIBE = 10
    UNSUBACK    = 11
    PINGREQ     = 12
    PINGRESP    = 13
    DISCONNECT  = 14
    AUTH        = 15  # MQTT 5.0


class QoS(IntEnum):
    AT_MOST_ONCE  = 0
    AT_LEAST_ONCE = 1
    EXACTLY_ONCE  = 2


class ConnectReturnCode(IntEnum):
    ACCEPTED                    = 0
    UNACCEPTABLE_PROTOCOL       = 1
    IDENTIFIER_REJECTED         = 2
    SERVER_UNAVAILABLE          = 3
    BAD_USERNAME_OR_PASSWORD    = 4
    NOT_AUTHORIZED              = 5


@dataclass
class ConnectPacket:
    client_id: str
    clean_session: bool
    keepalive: int
    username: str | None = None
    password: bytes | None = None
    will_topic: str | None = None
    will_message: bytes | None = None
    will_qos: QoS = QoS.AT_MOST_ONCE
    will_retain: bool = False
    protocol_version: int = 4  # 4 = MQTT 3.1.1, 5 = MQTT 5.0


@dataclass
class PublishPacket:
    topic: str
    payload: bytes
    qos: QoS = QoS.AT_MOST_ONCE
    retain: bool = False
    dup: bool = False
    packet_id: int | None = None


@dataclass
class SubscribeRequest:
    topic: str
    qos: QoS


@dataclass
class SubscribePacket:
    packet_id: int
    subscriptions: list[SubscribeRequest] = field(default_factory=list)


@dataclass
class UnsubscribePacket:
    packet_id: int
    topics: list[str] = field(default_factory=list)
