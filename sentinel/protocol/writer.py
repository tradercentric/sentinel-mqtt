"""MQTT packet encoder — builds bytes to write to an asyncio StreamWriter."""

import struct
from .packets import ConnectReturnCode, QoS


def _encode_remaining_length(length: int) -> bytes:
    result = []
    while True:
        byte = length % 128
        length //= 128
        if length:
            byte |= 0x80
        result.append(byte)
        if not length:
            break
    return bytes(result)


def _encode_string(s: str) -> bytes:
    encoded = s.encode("utf-8")
    return struct.pack("!H", len(encoded)) + encoded


def encode_connack(return_code: ConnectReturnCode, session_present: bool = False) -> bytes:
    flags = 0x01 if session_present else 0x00
    payload = bytes([flags, int(return_code)])
    return bytes([0x20, 0x02]) + payload


def encode_publish(topic: str, payload: bytes, qos: QoS = QoS.AT_MOST_ONCE,
                   retain: bool = False, packet_id: int | None = None) -> bytes:
    flags = (int(qos) << 1) | (0x01 if retain else 0x00)
    header_byte = (0x03 << 4) | flags
    body = _encode_string(topic)
    if qos != QoS.AT_MOST_ONCE:
        assert packet_id is not None
        body += struct.pack("!H", packet_id)
    body += payload
    return bytes([header_byte]) + _encode_remaining_length(len(body)) + body


def encode_puback(packet_id: int) -> bytes:
    return bytes([0x40, 0x02]) + struct.pack("!H", packet_id)


def encode_pubrec(packet_id: int) -> bytes:
    return bytes([0x50, 0x02]) + struct.pack("!H", packet_id)


def encode_pubrel(packet_id: int) -> bytes:
    return bytes([0x62, 0x02]) + struct.pack("!H", packet_id)


def encode_pubcomp(packet_id: int) -> bytes:
    return bytes([0x70, 0x02]) + struct.pack("!H", packet_id)


def encode_suback(packet_id: int, return_codes: list[int]) -> bytes:
    payload = struct.pack("!H", packet_id) + bytes(return_codes)
    return bytes([0x90]) + _encode_remaining_length(len(payload)) + payload


def encode_unsuback(packet_id: int) -> bytes:
    return bytes([0xB0, 0x02]) + struct.pack("!H", packet_id)


def encode_pingresp() -> bytes:
    return bytes([0xD0, 0x00])


def encode_disconnect() -> bytes:
    return bytes([0xE0, 0x00])
