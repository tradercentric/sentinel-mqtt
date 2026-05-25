"""HA replication layer integration tests."""

from __future__ import annotations
import asyncio
import pytest

from sentinel.broker.server import Broker
from sentinel.ha.cluster import HAManager, NodeRole
from tests.test_integration import MQTTClient


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
async def primary():
    """Primary broker with HA manager. Yields (broker, ha, mqtt_port, ha_port)."""
    ha = HAManager(
        role=NodeRole.PRIMARY,
        ha_host="127.0.0.1",
        ha_port=0,
    )
    broker = Broker(host="127.0.0.1", port=0, ha_manager=ha)
    mqtt_port = await broker.listen()
    ha_port = await ha.start()
    yield broker, ha, mqtt_port, ha_port
    await ha.stop()
    await broker.close()


@pytest.fixture
async def standby(primary):
    """Standby HA manager connected to the primary fixture."""
    _, _, _, ha_port = primary
    ha = HAManager(
        role=NodeRole.STANDBY,
        ha_host="127.0.0.1",
        ha_port=0,
        peer_host="127.0.0.1",
        peer_ha_port=ha_port,
        ping_interval=0.2,
        failover_timeout=1.0,
    )
    await ha.start()
    # Give standby time to connect and receive snapshot
    await asyncio.sleep(0.15)
    yield ha
    await ha.stop()


# ---------------------------------------------------------------------------
# Snapshot tests
# ---------------------------------------------------------------------------

async def test_snapshot_sent_on_connect(primary, standby):
    """Standby receives a snapshot immediately on connecting to primary."""
    broker, ha_primary, mqtt_port, _ = primary
    ha_standby = standby

    # Publish retained messages via primary BEFORE standby connects
    # (standby fixture already connected, so publish after)
    pub = MQTTClient("127.0.0.1", mqtt_port)
    await pub.connect("snap-pub")
    await pub.publish("snap/a", b"1", retain=True)
    await pub.publish("snap/b", b"2", retain=True)
    await pub.disconnect()

    # Wait for replication
    await asyncio.sleep(0.1)

    assert "snap/a" in ha_standby._retained
    assert "snap/b" in ha_standby._retained
    assert bytes.fromhex(ha_standby._retained["snap/a"]["payload"]) == b"1"
    assert bytes.fromhex(ha_standby._retained["snap/b"]["payload"]) == b"2"


async def test_snapshot_includes_pre_existing_retained(primary):
    """Standby receives retained messages published BEFORE it connects."""
    broker, ha_primary, mqtt_port, ha_port = primary

    # Publish retained messages before standby connects
    pub = MQTTClient("127.0.0.1", mqtt_port)
    await pub.connect("pre-pub")
    await pub.publish("pre/x", b"hello", retain=True)
    await pub.disconnect()
    await asyncio.sleep(0.05)

    # Now connect standby
    ha_standby = HAManager(
        role=NodeRole.STANDBY,
        ha_host="127.0.0.1",
        ha_port=0,
        peer_host="127.0.0.1",
        peer_ha_port=ha_port,
        ping_interval=0.2,
        failover_timeout=10.0,
    )
    await ha_standby.start()
    await asyncio.sleep(0.15)

    assert "pre/x" in ha_standby._retained
    assert bytes.fromhex(ha_standby._retained["pre/x"]["payload"]) == b"hello"

    await ha_standby.stop()


# ---------------------------------------------------------------------------
# Incremental replication tests
# ---------------------------------------------------------------------------

async def test_incremental_retained_set(primary, standby):
    """Retained message published on primary is replicated to standby."""
    broker, _, mqtt_port, _ = primary
    ha_standby = standby

    pub = MQTTClient("127.0.0.1", mqtt_port)
    await pub.connect("inc-pub")
    await pub.publish("sensors/temp", b"23.5", retain=True)
    await pub.disconnect()

    await asyncio.sleep(0.1)

    assert "sensors/temp" in ha_standby._retained
    assert bytes.fromhex(ha_standby._retained["sensors/temp"]["payload"]) == b"23.5"
    assert ha_standby._retained["sensors/temp"]["qos"] == 0


async def test_incremental_retained_delete(primary, standby):
    """Clearing a retained message on primary is replicated to standby."""
    broker, _, mqtt_port, _ = primary
    ha_standby = standby

    pub = MQTTClient("127.0.0.1", mqtt_port)
    await pub.connect("del-pub")
    await pub.publish("sensors/hum", b"55", retain=True)
    await asyncio.sleep(0.1)
    assert "sensors/hum" in ha_standby._retained

    await pub.publish("sensors/hum", b"", retain=True)  # clear
    await pub.disconnect()
    await asyncio.sleep(0.1)

    assert "sensors/hum" not in ha_standby._retained


async def test_incremental_session_subscription(primary, standby):
    """Persistent session subscription is replicated to standby."""
    broker, _, mqtt_port, _ = primary
    ha_standby = standby

    sub = MQTTClient("127.0.0.1", mqtt_port)
    await sub.connect("persistent-sub", clean=False)
    await sub.subscribe("device/status", qos=1)
    await asyncio.sleep(0.1)

    assert "persistent-sub" in ha_standby._sessions
    subs = ha_standby._sessions["persistent-sub"]["subscriptions"]
    assert "device/status" in subs
    assert subs["device/status"] == 1

    await sub.disconnect()


# ---------------------------------------------------------------------------
# Failover tests
# ---------------------------------------------------------------------------

async def test_failover_promotes_standby(primary):
    """Standby promotes to primary after primary HA server stops."""
    broker, ha_primary, mqtt_port, ha_port = primary

    promoted = asyncio.Event()
    promoted_state: dict = {}

    ha_standby = HAManager(
        role=NodeRole.STANDBY,
        ha_host="127.0.0.1",
        ha_port=0,
        peer_host="127.0.0.1",
        peer_ha_port=ha_port,
        ping_interval=0.2,
        failover_timeout=1.0,
    )

    async def on_promote(retained, sessions):
        promoted_state.update(retained)
        promoted.set()

    ha_standby.on_promote(on_promote)
    await ha_standby.start()
    await asyncio.sleep(0.15)

    # Publish some state on primary
    pub = MQTTClient("127.0.0.1", mqtt_port)
    await pub.connect("fo-pub")
    await pub.publish("status/node", b"online", retain=True)
    await pub.disconnect()
    await asyncio.sleep(0.1)

    # Kill primary HA
    await ha_primary.stop()

    # Wait for standby to detect failure and promote
    await asyncio.wait_for(promoted.wait(), timeout=5.0)

    assert ha_standby.role == NodeRole.PRIMARY
    assert "status/node" in promoted_state
    assert promoted_state["status/node"]["payload"] == b"online"

    await ha_standby.stop()


async def test_promoted_standby_starts_broker(primary):
    """When standby promotes, it can start a new broker with replicated state."""
    broker, ha_primary, mqtt_port, ha_port = primary

    promoted = asyncio.Event()
    promoted_broker: list[Broker] = []

    ha_standby = HAManager(
        role=NodeRole.STANDBY,
        ha_host="127.0.0.1",
        ha_port=0,
        peer_host="127.0.0.1",
        peer_ha_port=ha_port,
        ping_interval=0.2,
        failover_timeout=1.0,
    )

    async def on_promote(retained, sessions):
        new_broker = Broker(host="127.0.0.1", port=0)
        # Pre-load replicated retained messages
        for topic, msg in retained.items():
            new_broker._retained.set(topic, msg["payload"], msg["qos"])
        new_port = await new_broker.listen()
        promoted_broker.append((new_broker, new_port))
        promoted.set()

    ha_standby.on_promote(on_promote)
    await ha_standby.start()
    await asyncio.sleep(0.15)

    # Publish retained message on primary
    pub = MQTTClient("127.0.0.1", mqtt_port)
    await pub.connect("fo2-pub")
    await pub.publish("config/mode", b"auto", retain=True)
    await pub.disconnect()
    await asyncio.sleep(0.1)

    # Kill primary HA
    await ha_primary.stop()

    # Wait for promotion and new broker to start
    await asyncio.wait_for(promoted.wait(), timeout=5.0)

    new_broker, new_port = promoted_broker[0]

    # Connect a fresh client to the promoted broker and subscribe
    sub = MQTTClient("127.0.0.1", new_port)
    await sub.connect("fo2-sub")
    retained_msgs = await sub.subscribe("config/mode")

    # Retained message should be available on the promoted broker
    assert len(retained_msgs) == 1
    assert retained_msgs[0].topic == "config/mode"
    assert retained_msgs[0].payload == b"auto"

    await sub.disconnect()
    await new_broker.close()
    await ha_standby.stop()
