"""
HA cluster manager — active-passive replication between primary and standby.

Primary:
  - Runs MQTT broker normally.
  - Listens on HA port; accepts one standby connection.
  - On connect: sends full state snapshot, then streams incremental events.
  - Responds to standby PINGs with PONGs.

Standby:
  - Does not accept MQTT connections until promoted.
  - Connects to primary HA port.
  - Applies snapshot + incremental events to local shadow state.
  - Sends periodic PINGs; if PONG times out → promotes to primary.
  - On promotion: fires on_promote callbacks with current shadow state.
"""

from __future__ import annotations
import asyncio
import json
import logging
import time
from enum import Enum

log = logging.getLogger("sentinel.ha")


class NodeRole(Enum):
    PRIMARY = "primary"
    STANDBY = "standby"


class HAManager:
    def __init__(
        self,
        role: NodeRole,
        ha_host: str = "0.0.0.0",
        ha_port: int = 1884,
        peer_host: str = "",
        peer_ha_port: int = 1884,
        ping_interval: float = 2.0,
        failover_timeout: float = 8.0,
    ) -> None:
        self.role = role
        self._ha_host = ha_host
        self._ha_port = ha_port
        self._peer_host = peer_host
        self._peer_ha_port = peer_ha_port
        self._ping_interval = ping_interval
        self._failover_timeout = failover_timeout

        # Shadow state — primary keeps this in sync with broker;
        # standby applies incoming events here.
        self._retained: dict[str, dict] = {}   # topic -> {payload_hex, qos}
        self._sessions: dict[str, dict] = {}   # client_id -> {subscriptions: {filter: qos}}

        # Primary: open writer connections to standby(ies)
        self._standby_writers: list[asyncio.StreamWriter] = []

        # Standby: last time we received anything from primary
        self._last_activity: float = 0.0

        # Callbacks fired on promotion
        self._promote_callbacks: list = []

        # Background tasks and server handle
        self._tasks: list[asyncio.Task] = []
        self._server: asyncio.AbstractServer | None = None

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def on_promote(self, fn) -> None:
        """Register an async callback(retained, sessions) called on promotion."""
        self._promote_callbacks.append(fn)

    async def start(self) -> int:
        """Start the HA manager. Returns the actual HA port (primary) or 0 (standby)."""
        if self.role == NodeRole.PRIMARY:
            return await self._start_primary()
        else:
            await self._start_standby()
            return 0

    async def stop(self) -> None:
        # Close active connections before wait_closed() — otherwise wait_closed()
        # blocks indefinitely waiting for handlers that are still reading.
        for writer in self._standby_writers:
            writer.close()
        self._standby_writers.clear()
        for task in self._tasks:
            task.cancel()
        self._tasks.clear()
        if self._server:
            self._server.close()
            await self._server.wait_closed()
            self._server = None

    # ------------------------------------------------------------------
    # Replication hooks — called by broker on PRIMARY to propagate changes
    # ------------------------------------------------------------------

    def replicate_retained_set(self, topic: str, payload: bytes, qos: int) -> None:
        self._retained[topic] = {"payload": payload.hex(), "qos": qos}
        self._broadcast({"type": "retained_set", "topic": topic,
                         "payload": payload.hex(), "qos": qos})

    def replicate_retained_del(self, topic: str) -> None:
        self._retained.pop(topic, None)
        self._broadcast({"type": "retained_del", "topic": topic})

    def replicate_session_sub(self, client_id: str, topic_filter: str, qos: int) -> None:
        self._sessions.setdefault(client_id, {"subscriptions": {}})
        self._sessions[client_id]["subscriptions"][topic_filter] = qos
        self._broadcast({"type": "session_sub", "client_id": client_id,
                         "topic_filter": topic_filter, "qos": qos})

    def replicate_session_unsub(self, client_id: str, topic_filter: str) -> None:
        if client_id in self._sessions:
            self._sessions[client_id]["subscriptions"].pop(topic_filter, None)
        self._broadcast({"type": "session_unsub", "client_id": client_id,
                         "topic_filter": topic_filter})

    def replicate_session_del(self, client_id: str) -> None:
        self._sessions.pop(client_id, None)
        self._broadcast({"type": "session_del", "client_id": client_id})

    # ------------------------------------------------------------------
    # Primary internals
    # ------------------------------------------------------------------

    async def _start_primary(self) -> int:
        self._server = await asyncio.start_server(
            self._handle_standby_connection, self._ha_host, self._ha_port
        )
        port = self._server.sockets[0].getsockname()[1]
        log.info("HA server listening on port %d (PRIMARY)", port)
        self._tasks.append(asyncio.create_task(self._primary_serve()))
        return port

    async def _primary_serve(self) -> None:
        async with self._server:
            await self._server.serve_forever()

    async def _handle_standby_connection(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        peer = writer.get_extra_info("peername")
        log.info("standby connected from %s", peer)
        self._standby_writers.append(writer)
        try:
            # Full snapshot
            snapshot = {
                "type": "snapshot",
                "retained": self._retained,
                "sessions": self._sessions,
            }
            writer.write(json.dumps(snapshot).encode() + b"\n")
            await writer.drain()

            # Respond to pings
            async for line in reader:
                try:
                    msg = json.loads(line)
                    if msg.get("type") == "ping":
                        writer.write(b'{"type":"pong"}\n')
                        await writer.drain()
                except json.JSONDecodeError:
                    pass
        except Exception as exc:
            log.debug("standby connection closed: %s", exc)
        finally:
            if writer in self._standby_writers:
                self._standby_writers.remove(writer)
            writer.close()

    def _broadcast(self, event: dict) -> None:
        if not self._standby_writers:
            return
        line = json.dumps(event).encode() + b"\n"
        dead = []
        for writer in self._standby_writers:
            try:
                writer.write(line)
                asyncio.ensure_future(writer.drain())
            except Exception:
                dead.append(writer)
        for w in dead:
            self._standby_writers.remove(w)

    # ------------------------------------------------------------------
    # Standby internals
    # ------------------------------------------------------------------

    async def _start_standby(self) -> None:
        self._last_activity = time.monotonic()
        self._tasks.append(asyncio.create_task(self._connect_loop()))
        self._tasks.append(asyncio.create_task(self._failover_watchdog()))

    async def _connect_loop(self) -> None:
        while self.role == NodeRole.STANDBY:
            try:
                reader, writer = await asyncio.wait_for(
                    asyncio.open_connection(self._peer_host, self._peer_ha_port),
                    timeout=3.0,
                )
                log.info("connected to primary HA at %s:%d",
                         self._peer_host, self._peer_ha_port)
                self._last_activity = time.monotonic()
                ping_task = asyncio.create_task(self._ping_loop(writer))
                try:
                    async for line in reader:
                        self._last_activity = time.monotonic()
                        try:
                            await self._apply_event(json.loads(line))
                        except json.JSONDecodeError:
                            pass
                except Exception as exc:
                    log.debug("lost connection to primary: %s", exc)
                finally:
                    ping_task.cancel()
                    writer.close()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                log.debug("cannot reach primary HA: %s", exc)
            if self.role == NodeRole.STANDBY:
                await asyncio.sleep(0.5)

    async def _ping_loop(self, writer: asyncio.StreamWriter) -> None:
        while True:
            await asyncio.sleep(self._ping_interval)
            try:
                writer.write(b'{"type":"ping"}\n')
                await writer.drain()
            except Exception:
                break

    async def _failover_watchdog(self) -> None:
        while self.role == NodeRole.STANDBY:
            await asyncio.sleep(self._ping_interval / 2)
            elapsed = time.monotonic() - self._last_activity
            if elapsed >= self._failover_timeout:
                log.warning(
                    "primary unreachable for %.1fs — promoting to PRIMARY", elapsed
                )
                await self._promote()
                return

    async def _promote(self) -> None:
        self.role = NodeRole.PRIMARY
        log.info("this node is now PRIMARY")
        retained = {
            k: {"payload": bytes.fromhex(v["payload"]), "qos": v["qos"]}
            for k, v in self._retained.items()
        }
        for fn in self._promote_callbacks:
            try:
                await fn(retained, self._sessions)
            except Exception as exc:
                log.error("promote callback error: %s", exc)

    async def _apply_event(self, msg: dict) -> None:
        t = msg.get("type")
        if t == "snapshot":
            self._retained = msg.get("retained", {})
            self._sessions = msg.get("sessions", {})
            log.info("snapshot applied: %d retained, %d sessions",
                     len(self._retained), len(self._sessions))
        elif t == "retained_set":
            self._retained[msg["topic"]] = {"payload": msg["payload"], "qos": msg["qos"]}
        elif t == "retained_del":
            self._retained.pop(msg["topic"], None)
        elif t == "session_sub":
            cid = msg["client_id"]
            self._sessions.setdefault(cid, {"subscriptions": {}})
            self._sessions[cid]["subscriptions"][msg["topic_filter"]] = msg["qos"]
        elif t == "session_unsub":
            cid = msg["client_id"]
            if cid in self._sessions:
                self._sessions[cid]["subscriptions"].pop(msg["topic_filter"], None)
        elif t == "session_del":
            self._sessions.pop(msg["client_id"], None)
        elif t == "pong":
            pass  # _last_activity already updated above
