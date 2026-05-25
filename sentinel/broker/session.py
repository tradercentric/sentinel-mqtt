"""Client session management."""

from __future__ import annotations
import asyncio
from dataclasses import dataclass, field
from .router import TopicRouter, Subscriber


@dataclass
class Session:
    client_id: str
    clean_session: bool
    writer: asyncio.StreamWriter | None = None
    subscriptions: dict[str, int] = field(default_factory=dict)  # topic_filter -> qos
    pending_qos1: dict[int, bytes] = field(default_factory=dict)  # packet_id -> encoded packet
    pending_qos2_in: set[int] = field(default_factory=set)   # packet_ids awaiting PUBREL
    pending_qos2_out: dict[int, bytes] = field(default_factory=dict)  # packet_id -> PUBCOMP pending
    next_packet_id: int = 1
    connected: bool = False
    will_topic: str | None = None
    will_message: bytes | None = None
    will_qos: int = 0
    will_retain: bool = False

    def allocate_packet_id(self) -> int:
        pid = self.next_packet_id
        self.next_packet_id = (pid % 65535) + 1
        return pid


class SessionManager:
    def __init__(self, router: TopicRouter) -> None:
        self._sessions: dict[str, Session] = {}
        self._router = router

    def get_or_create(self, client_id: str, clean_session: bool) -> tuple[Session, bool]:
        """Returns (session, session_present)."""
        existing = self._sessions.get(client_id)
        if existing and not clean_session:
            return existing, True
        if existing:
            self._router.remove_client(client_id)
        session = Session(client_id=client_id, clean_session=clean_session)
        self._sessions[client_id] = session
        return session, False

    def get(self, client_id: str) -> Session | None:
        return self._sessions.get(client_id)

    def disconnect(self, client_id: str) -> None:
        session = self._sessions.get(client_id)
        if session:
            session.connected = False
            session.writer = None
            if session.clean_session:
                self._router.remove_client(client_id)
                del self._sessions[client_id]

    def add_subscription(self, session: Session, topic_filter: str, qos: int,
                         callback: Subscriber) -> None:
        session.subscriptions[topic_filter] = qos
        self._router.subscribe(session.client_id, topic_filter, qos, callback)

    def remove_subscription(self, session: Session, topic_filter: str) -> None:
        session.subscriptions.pop(topic_filter, None)
        self._router.unsubscribe(session.client_id, topic_filter)

    def all_sessions(self) -> list[Session]:
        return list(self._sessions.values())
