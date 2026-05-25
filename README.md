# sentinel-mqtt

A failsafe, high-availability MQTT broker built in Python using asyncio.

## Features

- **Full MQTT 3.1.1 broker** — CONNECT, PUBLISH, SUBSCRIBE, QoS 0/1/2, retained messages, will messages, persistent sessions, wildcards (`+` and `#`)
- **Active-passive HA replication** — primary streams full state snapshot + incremental updates to standby over a dedicated TCP channel
- **Automatic failover** — standby promotes to primary when heartbeat times out; `on_promote` callback receives replicated state to start a new broker instantly
- **Lightweight** — pure Python asyncio, no external dependencies beyond `uvloop` and `pydantic`; suitable for Raspberry Pi and industrial gateways

## Architecture

```
┌─────────────────────────────────┐      ┌─────────────────────────────────┐
│         PRIMARY Node            │      │         STANDBY Node            │
├─────────────────────────────────┤      ├─────────────────────────────────┤
│  MQTT Broker (port 1883)        │      │  (no MQTT until promoted)       │
│  ├── TCP listener (asyncio)     │      │                                 │
│  ├── Packet parser / encoder    │      │  HA Manager                     │
│  ├── Topic router (trie)        │      │  ├── Connects to primary HA port │
│  ├── QoS 0/1/2 state machines   │      │  ├── Receives snapshot + events  │
│  ├── Session manager            │      │  ├── Sends periodic PINGs        │
│  └── Retained message store     │      │  └── Promotes on PONG timeout   │
│                                 │      │                                 │
│  HA Manager                     │─────►│  Shadow state (retained,        │
│  ├── HA TCP server (port 1884)  │      │  sessions) kept in sync         │
│  ├── Sends state snapshot       │      │                                 │
│  ├── Streams incremental events │      │  On promotion:                  │
│  └── Responds to PINGs          │      │  └── Starts new broker with     │
└─────────────────────────────────┘      │      replicated state           │
                                         └─────────────────────────────────┘
```

## Replicated state

| State | Replicated |
|---|---|
| Retained messages (set / delete) | ✓ |
| Persistent session subscriptions | ✓ |
| In-flight QoS 1/2 messages | Planned |
| Clean session subscriptions | Not needed (ephemeral) |

## Install

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
```

## Run a single broker

```bash
source .venv/bin/activate
sentinel-mqtt --host 0.0.0.0 --port 1883
```

## Run an HA pair

**Primary** (node A):
```python
from sentinel.broker.server import Broker
from sentinel.ha.cluster import HAManager, NodeRole
import asyncio

async def main():
    ha = HAManager(role=NodeRole.PRIMARY, ha_host="0.0.0.0", ha_port=1884)
    broker = Broker(host="0.0.0.0", port=1883, ha_manager=ha)
    await broker.listen()
    await ha.start()
    await asyncio.get_running_loop().create_future()  # run forever

asyncio.run(main())
```

**Standby** (node B):
```python
from sentinel.ha.cluster import HAManager, NodeRole
from sentinel.broker.server import Broker
import asyncio

async def main():
    ha = HAManager(
        role=NodeRole.STANDBY,
        peer_host="<primary-ip>",
        peer_ha_port=1884,
        ping_interval=2.0,
        failover_timeout=8.0,
    )

    async def on_promote(retained, sessions):
        broker = Broker(host="0.0.0.0", port=1883)
        for topic, msg in retained.items():
            broker._retained.set(topic, msg["payload"], msg["qos"])
        await broker.listen()

    ha.on_promote(on_promote)
    await ha.start()
    await asyncio.get_running_loop().create_future()

asyncio.run(main())
```

## Test

```bash
source .venv/bin/activate
pytest tests/ -v
```

31 tests covering: protocol encoding/decoding, topic routing (wildcards), broker integration (QoS 0/1, retained, will, ping, unsubscribe), and HA replication (snapshot, incremental sync, failover, promoted broker serving clients).

## Roadmap

- [ ] QoS 2 in-flight state replication
- [ ] MQTT 5.0 `Server Reference` for seamless client redirect on failover
- [ ] TLS support
- [ ] Persistence to disk (retained messages survive restart)
- [ ] Leader election (remove need for manual role assignment)
