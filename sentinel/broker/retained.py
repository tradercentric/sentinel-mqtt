"""Retained message store."""

from __future__ import annotations
from dataclasses import dataclass


@dataclass
class RetainedMessage:
    topic: str
    payload: bytes
    qos: int


class RetainedStore:
    def __init__(self) -> None:
        self._store: dict[str, RetainedMessage] = {}

    def set(self, topic: str, payload: bytes, qos: int) -> None:
        if payload:
            self._store[topic] = RetainedMessage(topic=topic, payload=payload, qos=qos)
        else:
            self._store.pop(topic, None)

    def get(self, topic: str) -> RetainedMessage | None:
        return self._store.get(topic)

    def match(self, topic_filter: str) -> list[RetainedMessage]:
        """Return retained messages matching a topic filter."""
        from .router import TopicRouter
        router = TopicRouter()
        collected: list[RetainedMessage] = []

        def collect(topic: str, payload: bytes, qos: int, retain: bool) -> None:
            msg = self._store.get(topic)
            if msg:
                collected.append(msg)

        router.subscribe("_retained_lookup", topic_filter, 0, collect)
        for msg in self._store.values():
            router.match(msg.topic)
            for _, cb, _ in router.match(msg.topic):
                cb(msg.topic, msg.payload, msg.qos, True)
        return collected

    def all(self) -> dict[str, RetainedMessage]:
        return dict(self._store)

    def load(self, messages: dict[str, RetainedMessage]) -> None:
        self._store = messages
