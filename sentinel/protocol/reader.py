"""MQTT packet reader — parses bytes from an asyncio StreamReader."""

import asyncio
import struct

from .packets import (
    ConnectPacket, PublishPacket, SubscribePacket, SubscribeRequest,
    UnsubscribePacket, PacketType, QoS,
)


class MQTTParseError(Exception):
    pass


async def _read_exactly(reader: asyncio.StreamReader, n: int) -> bytes:
    data = await reader.readexactly(n)
    return data


async def _decode_remaining_length(reader: asyncio.StreamReader) -> int:
    multiplier = 1
    value = 0
    for _ in range(4):
        byte = (await _read_exactly(reader, 1))[0]
        value += (byte & 0x7F) * multiplier
        multiplier *= 128
        if not (byte & 0x80):
            return value
    raise MQTTParseError("remaining length encoding exceeded 4 bytes")


def _decode_string(data: bytes, offset: int) -> tuple[str, int]:
    if offset + 2 > len(data):
        raise MQTTParseError("string length prefix out of bounds")
    length = struct.unpack_from("!H", data, offset)[0]
    offset += 2
    if offset + length > len(data):
        raise MQTTParseError("string data out of bounds")
    return data[offset:offset + length].decode("utf-8"), offset + length


async def read_packet(reader: asyncio.StreamReader) -> tuple[PacketType, int, bytes]:
    """Read one MQTT packet; returns (packet_type, flags, payload_bytes)."""
    header = (await _read_exactly(reader, 1))[0]
    packet_type = PacketType(header >> 4)
    flags = header & 0x0F
    remaining = await _decode_remaining_length(reader)
    payload = await _read_exactly(reader, remaining) if remaining else b""
    return packet_type, flags, payload


def parse_connect(payload: bytes) -> ConnectPacket:
    offset = 0
    proto_name, offset = _decode_string(payload, offset)
    proto_version = payload[offset]; offset += 1
    connect_flags = payload[offset]; offset += 1
    keepalive = struct.unpack_from("!H", payload, offset)[0]; offset += 2

    clean_session  = bool(connect_flags & 0x02)
    will_flag      = bool(connect_flags & 0x04)
    will_qos       = QoS((connect_flags >> 3) & 0x03)
    will_retain    = bool(connect_flags & 0x20)
    has_password   = bool(connect_flags & 0x40)
    has_username   = bool(connect_flags & 0x80)

    client_id, offset = _decode_string(payload, offset)

    will_topic = will_message = None
    if will_flag:
        will_topic, offset = _decode_string(payload, offset)
        will_len = struct.unpack_from("!H", payload, offset)[0]; offset += 2
        will_message = payload[offset:offset + will_len]; offset += will_len

    username = password = None
    if has_username:
        username, offset = _decode_string(payload, offset)
    if has_password:
        pwd_len = struct.unpack_from("!H", payload, offset)[0]; offset += 2
        password = payload[offset:offset + pwd_len]

    return ConnectPacket(
        client_id=client_id,
        clean_session=clean_session,
        keepalive=keepalive,
        username=username,
        password=password,
        will_topic=will_topic,
        will_message=will_message,
        will_qos=will_qos,
        will_retain=will_retain,
        protocol_version=proto_version,
    )


def parse_publish(flags: int, payload: bytes) -> PublishPacket:
    dup    = bool(flags & 0x08)
    qos    = QoS((flags >> 1) & 0x03)
    retain = bool(flags & 0x01)

    offset = 0
    topic, offset = _decode_string(payload, offset)

    packet_id = None
    if qos != QoS.AT_MOST_ONCE:
        packet_id = struct.unpack_from("!H", payload, offset)[0]; offset += 2

    return PublishPacket(
        topic=topic, payload=payload[offset:],
        qos=qos, retain=retain, dup=dup, packet_id=packet_id,
    )


def parse_subscribe(payload: bytes) -> SubscribePacket:
    packet_id = struct.unpack_from("!H", payload, 0)[0]
    offset = 2
    subs = []
    while offset < len(payload):
        topic, offset = _decode_string(payload, offset)
        qos = QoS(payload[offset]); offset += 1
        subs.append(SubscribeRequest(topic=topic, qos=qos))
    return SubscribePacket(packet_id=packet_id, subscriptions=subs)


def parse_unsubscribe(payload: bytes) -> UnsubscribePacket:
    packet_id = struct.unpack_from("!H", payload, 0)[0]
    offset = 2
    topics = []
    while offset < len(payload):
        topic, offset = _decode_string(payload, offset)
        topics.append(topic)
    return UnsubscribePacket(packet_id=packet_id, topics=topics)
