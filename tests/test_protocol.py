"""Tests for MQTT packet encoding/decoding."""

import pytest
import asyncio
import struct

from sentinel.protocol.packets import QoS, ConnectReturnCode
from sentinel.protocol import writer as encode
from sentinel.protocol.reader import parse_connect, parse_publish, parse_subscribe


def _make_connect_payload(client_id="testclient", clean=True, keepalive=60) -> bytes:
    proto_name = b"\x00\x04MQTT"
    proto_ver = b"\x04"
    connect_flags = b"\x02" if clean else b"\x00"
    ka = struct.pack("!H", keepalive)
    cid = struct.pack("!H", len(client_id)) + client_id.encode()
    return proto_name + proto_ver + connect_flags + ka + cid


def test_parse_connect():
    payload = _make_connect_payload("myclient", clean=True)
    pkt = parse_connect(payload)
    assert pkt.client_id == "myclient"
    assert pkt.clean_session is True
    assert pkt.keepalive == 60


def test_encode_connack():
    data = encode.encode_connack(ConnectReturnCode.ACCEPTED, False)
    assert data == bytes([0x20, 0x02, 0x00, 0x00])


def test_encode_connack_session_present():
    data = encode.encode_connack(ConnectReturnCode.ACCEPTED, True)
    assert data == bytes([0x20, 0x02, 0x01, 0x00])


def test_encode_decode_publish_qos0():
    data = encode.encode_publish("home/temp", b"22.5", QoS.AT_MOST_ONCE)
    # first byte: packet type 3, flags 0 -> 0x30
    assert data[0] == 0x30
    pkt = parse_publish(0x00, data[2:])  # skip header + remaining length byte
    assert pkt.topic == "home/temp"
    assert pkt.payload == b"22.5"
    assert pkt.qos == QoS.AT_MOST_ONCE


def test_encode_puback():
    data = encode.encode_puback(42)
    assert data == bytes([0x40, 0x02, 0x00, 0x2A])


def test_encode_pingresp():
    assert encode.encode_pingresp() == bytes([0xD0, 0x00])
