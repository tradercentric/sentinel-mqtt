"""State replication — primary streams retained messages and sessions to standby."""

from __future__ import annotations
import asyncio
import json
import logging

log = logging.getLogger("sentinel.ha.replication")

REPLICATION_PORT_OFFSET = 100  # replication listens on broker_port + 100


class ReplicationSender:
    """Runs on primary — pushes state changes to standby."""

    def __init__(self, standby_host: str, standby_port: int) -> None:
        self.standby_host = standby_host
        self.standby_port = standby_port
        self._writer: asyncio.StreamWriter | None = None
        self._queue: asyncio.Queue = asyncio.Queue()

    async def connect(self) -> None:
        while True:
            try:
                _, self._writer = await asyncio.open_connection(self.standby_host, self.standby_port)
                log.info("replication connected to standby %s:%d", self.standby_host, self.standby_port)
                return
            except Exception:
                log.debug("waiting for standby replication endpoint...")
                await asyncio.sleep(2.0)

    async def run(self) -> None:
        await self.connect()
        while True:
            event = await self._queue.get()
            try:
                line = json.dumps(event).encode() + b"\n"
                self._writer.write(line)
                await self._writer.drain()
            except Exception as exc:
                log.warning("replication send failed: %s — reconnecting", exc)
                await self.connect()

    def publish_retained(self, topic: str, payload: bytes, qos: int) -> None:
        self._queue.put_nowait({
            "type": "retained",
            "topic": topic,
            "payload": payload.hex(),
            "qos": qos,
        })

    def delete_retained(self, topic: str) -> None:
        self._queue.put_nowait({"type": "retained_delete", "topic": topic})


class ReplicationReceiver:
    """Runs on standby — receives state from primary and applies it locally."""

    def __init__(self, port: int, on_retained, on_retained_delete) -> None:
        self.port = port
        self._on_retained = on_retained
        self._on_retained_delete = on_retained_delete

    async def start(self) -> None:
        server = await asyncio.start_server(self._handle, "0.0.0.0", self.port)
        log.info("replication receiver listening on port %d", self.port)
        async with server:
            await server.serve_forever()

    async def _handle(self, reader: asyncio.StreamReader, _writer: asyncio.StreamWriter) -> None:
        log.info("primary connected for replication")
        async for line in reader:
            try:
                event = json.loads(line)
                etype = event["type"]
                if etype == "retained":
                    self._on_retained(event["topic"], bytes.fromhex(event["payload"]), event["qos"])
                elif etype == "retained_delete":
                    self._on_retained_delete(event["topic"])
            except Exception as exc:
                log.warning("bad replication event: %s", exc)
