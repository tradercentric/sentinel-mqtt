# sentinel-mqtt

A failsafe, high-availability MQTT broker built in Python using asyncio.

## Goals

- Full MQTT 3.1.1 and MQTT 5.0 protocol support
- Active-passive replication for broker failover
- Retained message and session state synchronization across nodes
- Lightweight enough to run on edge devices (Raspberry Pi, industrial gateways)
- MQTT 5.0 `Server Reference` for seamless client redirect on failover

## Architecture

```
┌─────────────────────────────┐
│        MQTT Node            │
├─────────────────────────────┤
│  TCP Listener (asyncio)     │
│  Packet parser / encoder    │
│  Topic router (trie)        │
│  QoS state machine          │
│  Session manager            │
│  Retained message store     │
├─────────────────────────────┤
│  HA Replication Layer       │
│  ├── Heartbeat / leader     │
│  │   election               │
│  ├── State sync log         │
│  └── Peer TCP channels      │
└─────────────────────────────┘
```

## Build order

1. Minimal broker — CONNECT, PUBLISH, SUBSCRIBE, QoS 0
2. Retained messages + persistent sessions
3. QoS 1 & 2 state machines
4. Peer discovery + heartbeat
5. Active-passive failover
6. Session replication
7. MQTT 5.0 client redirect on failover

## Install

```bash
pip install -e ".[dev]"
```

## Run

```bash
sentinel-mqtt --host 0.0.0.0 --port 1883
```

## Test

```bash
pytest tests/
```
