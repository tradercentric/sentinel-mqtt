"""Heartbeat — detects peer failure for active-passive failover."""

from __future__ import annotations
import asyncio
import logging
import time

log = logging.getLogger("sentinel.ha.heartbeat")


class HeartbeatMonitor:
    def __init__(self, peer_host: str, peer_port: int,
                 interval: float = 1.0, timeout: float = 5.0) -> None:
        self.peer_host = peer_host
        self.peer_port = peer_port
        self.interval = interval
        self.timeout = timeout
        self._last_seen: float = time.monotonic()
        self._alive: bool = True
        self._on_failure: list[asyncio.coroutine] = []

    def on_failure(self, coro_fn) -> None:
        self._on_failure.append(coro_fn)

    @property
    def is_alive(self) -> bool:
        return self._alive

    async def run(self) -> None:
        while True:
            await asyncio.sleep(self.interval)
            try:
                reader, writer = await asyncio.wait_for(
                    asyncio.open_connection(self.peer_host, self.peer_port),
                    timeout=2.0,
                )
                writer.write(b"PING\n")
                await writer.drain()
                writer.close()
                self._last_seen = time.monotonic()
                if not self._alive:
                    log.info("peer %s:%d recovered", self.peer_host, self.peer_port)
                self._alive = True
            except Exception:
                elapsed = time.monotonic() - self._last_seen
                if elapsed >= self.timeout and self._alive:
                    self._alive = False
                    log.warning("peer %s:%d unreachable for %.1fs — triggering failover",
                                self.peer_host, self.peer_port, elapsed)
                    for fn in self._on_failure:
                        asyncio.ensure_future(fn())
