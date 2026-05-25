"""Integration tests — connect real clients to a live broker instance."""

from __future__ import annotations
import asyncio
import struct
import pytest

from sentinel.broker.server import Broker
from sentinel.protocol.packets import PacketType, QoS
from sentinel.protocol.reader import read_packet, parse_publish
from sentinel.protocol import writer as encode


# ---------------------------------------------------------------------------
# Packet-building helpers (raw bytes, no dependency on broker internals)
# ---------------------------------------------------------------------------

def _enc_str(s: str) -> bytes:
    b = s.encode()
    return struct.pack("!H", len(b)) + b


def _enc_remaining(n: int) -> bytes:
    out = []
    while True:
        byte = n % 128
        n //= 128
        if n:
            byte |= 0x80
        out.append(byte)
        if not n:
            break
    return bytes(out)


def _build_connect(client_id: str, clean: bool = True, keepalive: int = 60,
                   will_topic: str | None = None, will_message: bytes | None = None,
                   will_qos: int = 0) -> bytes:
    flags = 0x02 if clean else 0x00
    if will_topic and will_message:
        flags |= 0x04 | (will_qos << 3)
    var = b"\x00\x04MQTT\x04" + bytes([flags]) + struct.pack("!H", keepalive)
    payload = _enc_str(client_id)
    if will_topic and will_message:
        payload += _enc_str(will_topic) + struct.pack("!H", len(will_message)) + will_message
    body = var + payload
    return bytes([0x10]) + _enc_remaining(len(body)) + body


def _build_subscribe(packet_id: int, topic_filter: str, qos: int = 0) -> bytes:
    body = struct.pack("!H", packet_id) + _enc_str(topic_filter) + bytes([qos])
    return bytes([0x82]) + _enc_remaining(len(body)) + body


def _build_unsubscribe(packet_id: int, topic_filter: str) -> bytes:
    body = struct.pack("!H", packet_id) + _enc_str(topic_filter)
    return bytes([0xA2]) + _enc_remaining(len(body)) + body


# ---------------------------------------------------------------------------
# TestClient — minimal async MQTT client for test use only
# ---------------------------------------------------------------------------

class MQTTClient:
    def __init__(self, host: str, port: int) -> None:
        self._host = host
        self._port = port
        self._reader: asyncio.StreamReader | None = None
        self._writer: asyncio.StreamWriter | None = None
        self._pid = 1

    def _alloc_pid(self) -> int:
        pid = self._pid
        self._pid = (self._pid % 65535) + 1
        return pid

    async def connect(self, client_id: str = "test", clean: bool = True,
                      will_topic: str | None = None, will_message: bytes | None = None,
                      will_qos: int = 0) -> int:
        self._reader, self._writer = await asyncio.open_connection(self._host, self._port)
        self._writer.write(_build_connect(client_id, clean,
                                          will_topic=will_topic,
                                          will_message=will_message,
                                          will_qos=will_qos))
        await self._writer.drain()
        ptype, _, data = await read_packet(self._reader)
        assert ptype == PacketType.CONNACK
        return data[1]  # return code

    async def subscribe(self, topic_filter: str, qos: int = 0) -> list:
        """Subscribe and return any retained messages delivered before SUBACK."""
        pid = self._alloc_pid()
        self._writer.write(_build_subscribe(pid, topic_filter, qos))
        await self._writer.drain()
        retained = []
        while True:
            ptype, flags, data = await asyncio.wait_for(read_packet(self._reader), timeout=2.0)
            if ptype == PacketType.PUBLISH:
                retained.append(parse_publish(flags, data))
            elif ptype == PacketType.SUBACK:
                break
        return retained

    async def unsubscribe(self, topic_filter: str) -> None:
        pid = self._alloc_pid()
        self._writer.write(_build_unsubscribe(pid, topic_filter))
        await self._writer.drain()
        ptype, _, _ = await asyncio.wait_for(read_packet(self._reader), timeout=2.0)
        assert ptype == PacketType.UNSUBACK

    async def publish(self, topic: str, payload: bytes,
                      qos: int = 0, retain: bool = False) -> None:
        pid = self._alloc_pid() if qos > 0 else None
        self._writer.write(encode.encode_publish(topic, payload, QoS(qos), retain, pid))
        await self._writer.drain()
        if qos == 1:
            ptype, _, _ = await asyncio.wait_for(read_packet(self._reader), timeout=2.0)
            assert ptype == PacketType.PUBACK

    async def read_message(self, timeout: float = 2.0):
        ptype, flags, data = await asyncio.wait_for(read_packet(self._reader), timeout=timeout)
        assert ptype == PacketType.PUBLISH
        pkt = parse_publish(flags, data)
        if pkt.qos == QoS.AT_LEAST_ONCE and pkt.packet_id:
            self._writer.write(encode.encode_puback(pkt.packet_id))
            await self._writer.drain()
        return pkt

    async def ping(self) -> None:
        self._writer.write(bytes([0xC0, 0x00]))  # PINGREQ
        await self._writer.drain()
        ptype, _, _ = await asyncio.wait_for(read_packet(self._reader), timeout=2.0)
        assert ptype == PacketType.PINGRESP

    async def disconnect(self) -> None:
        self._writer.write(encode.encode_disconnect())
        await self._writer.drain()
        self._writer.close()
        await self._writer.wait_closed()

    async def close(self) -> None:
        if self._writer:
            self._writer.close()


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
async def broker_addr():
    broker = Broker(host="127.0.0.1", port=0)
    port = await broker.listen()
    yield ("127.0.0.1", port)
    await broker.close()


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

async def test_connect(broker_addr):
    host, port = broker_addr
    client = MQTTClient(host, port)
    rc = await client.connect("client1")
    assert rc == 0  # ACCEPTED
    await client.disconnect()


async def test_publish_subscribe_qos0(broker_addr):
    host, port = broker_addr
    pub = MQTTClient(host, port)
    sub = MQTTClient(host, port)

    await sub.connect("sub1")
    await sub.subscribe("sensors/temp")

    await pub.connect("pub1")
    await pub.publish("sensors/temp", b"22.5")

    msg = await sub.read_message()
    assert msg.topic == "sensors/temp"
    assert msg.payload == b"22.5"
    assert msg.qos == QoS.AT_MOST_ONCE

    await pub.disconnect()
    await sub.disconnect()


async def test_publish_subscribe_qos1(broker_addr):
    host, port = broker_addr
    pub = MQTTClient(host, port)
    sub = MQTTClient(host, port)

    await sub.connect("sub-q1")
    await sub.subscribe("data/reading", qos=1)

    await pub.connect("pub-q1")
    await pub.publish("data/reading", b"hello", qos=1)

    msg = await sub.read_message()
    assert msg.topic == "data/reading"
    assert msg.payload == b"hello"
    assert msg.qos == QoS.AT_LEAST_ONCE

    await pub.disconnect()
    await sub.disconnect()


async def test_retained_message_delivered_on_subscribe(broker_addr):
    host, port = broker_addr
    pub = MQTTClient(host, port)
    sub = MQTTClient(host, port)

    await pub.connect("pub-ret")
    await pub.publish("config/mode", b"auto", retain=True)
    await pub.disconnect()

    await sub.connect("sub-ret")
    retained = await sub.subscribe("config/mode")

    assert len(retained) == 1
    assert retained[0].topic == "config/mode"
    assert retained[0].payload == b"auto"
    assert retained[0].retain is True

    await sub.disconnect()


async def test_retained_cleared_by_empty_payload(broker_addr):
    host, port = broker_addr
    pub = MQTTClient(host, port)
    sub = MQTTClient(host, port)

    await pub.connect("pub-clr")
    await pub.publish("config/flag", b"on", retain=True)
    await pub.publish("config/flag", b"", retain=True)  # clear it
    await pub.disconnect()

    await sub.connect("sub-clr")
    retained = await sub.subscribe("config/flag")
    assert retained == []

    await sub.disconnect()


async def test_wildcard_single_level(broker_addr):
    host, port = broker_addr
    pub = MQTTClient(host, port)
    sub = MQTTClient(host, port)

    await sub.connect("sub-wc")
    await sub.subscribe("home/+/temp")

    await pub.connect("pub-wc")
    await pub.publish("home/room1/temp", b"21")
    await pub.publish("home/room2/temp", b"23")

    msg1 = await sub.read_message()
    msg2 = await sub.read_message()
    topics = {msg1.topic, msg2.topic}
    assert topics == {"home/room1/temp", "home/room2/temp"}

    await pub.disconnect()
    await sub.disconnect()


async def test_wildcard_multi_level(broker_addr):
    host, port = broker_addr
    pub = MQTTClient(host, port)
    sub = MQTTClient(host, port)

    await sub.connect("sub-hash")
    await sub.subscribe("sensors/#")

    await pub.connect("pub-hash")
    await pub.publish("sensors/temperature", b"20")
    await pub.publish("sensors/humidity/room1", b"55")

    msg1 = await sub.read_message()
    msg2 = await sub.read_message()
    topics = {msg1.topic, msg2.topic}
    assert "sensors/temperature" in topics
    assert "sensors/humidity/room1" in topics

    await pub.disconnect()
    await sub.disconnect()


async def test_multiple_subscribers(broker_addr):
    host, port = broker_addr
    pub = MQTTClient(host, port)
    sub1 = MQTTClient(host, port)
    sub2 = MQTTClient(host, port)

    await sub1.connect("sub-m1")
    await sub2.connect("sub-m2")
    await sub1.subscribe("alerts")
    await sub2.subscribe("alerts")

    await pub.connect("pub-m")
    await pub.publish("alerts", b"fire!")

    msg1 = await sub1.read_message()
    msg2 = await sub2.read_message()
    assert msg1.payload == b"fire!"
    assert msg2.payload == b"fire!"

    await pub.disconnect()
    await sub1.disconnect()
    await sub2.disconnect()


async def test_will_message_on_ungraceful_disconnect(broker_addr):
    host, port = broker_addr
    sub = MQTTClient(host, port)
    dying = MQTTClient(host, port)

    await sub.connect("sub-will")
    await sub.subscribe("status/dying-client")

    await dying.connect("dying", will_topic="status/dying-client",
                        will_message=b"offline")
    # Close TCP connection without sending DISCONNECT — triggers will
    await dying.close()

    msg = await sub.read_message(timeout=3.0)
    assert msg.topic == "status/dying-client"
    assert msg.payload == b"offline"

    await sub.disconnect()


async def test_no_will_on_clean_disconnect(broker_addr):
    host, port = broker_addr
    sub = MQTTClient(host, port)
    client = MQTTClient(host, port)

    await sub.connect("sub-nowill")
    await sub.subscribe("status/clean-client")

    await client.connect("clean-client", will_topic="status/clean-client",
                         will_message=b"offline")
    await client.disconnect()  # clean disconnect — will must NOT fire

    with pytest.raises(asyncio.TimeoutError):
        await sub.read_message(timeout=0.3)

    await sub.disconnect()


async def test_pingreq_pingresp(broker_addr):
    host, port = broker_addr
    client = MQTTClient(host, port)
    await client.connect("ping-client")
    await client.ping()
    await client.disconnect()


async def test_unsubscribe(broker_addr):
    host, port = broker_addr
    pub = MQTTClient(host, port)
    sub = MQTTClient(host, port)

    await sub.connect("sub-unsub")
    await sub.subscribe("topic/x")
    await sub.unsubscribe("topic/x")

    await pub.connect("pub-unsub")
    await pub.publish("topic/x", b"should not arrive")

    with pytest.raises(asyncio.TimeoutError):
        await sub.read_message(timeout=0.3)

    await pub.disconnect()
    await sub.disconnect()
