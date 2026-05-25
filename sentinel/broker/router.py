"""Topic router using a trie structure with MQTT wildcard support (+ and #)."""

from __future__ import annotations
from dataclasses import dataclass, field
from typing import Callable


Subscriber = Callable[[str, bytes, int, bool], None]  # topic, payload, qos, retain


@dataclass
class TrieNode:
    children: dict[str, "TrieNode"] = field(default_factory=dict)
    subscribers: dict[str, tuple[Subscriber, int]] = field(default_factory=dict)  # client_id -> (cb, qos)


class TopicRouter:
    def __init__(self) -> None:
        self._root = TrieNode()

    def subscribe(self, client_id: str, topic_filter: str, qos: int, callback: Subscriber) -> None:
        node = self._root
        for part in topic_filter.split("/"):
            node = node.children.setdefault(part, TrieNode())
        node.subscribers[client_id] = (callback, qos)

    def unsubscribe(self, client_id: str, topic_filter: str) -> None:
        parts = topic_filter.split("/")
        self._unsubscribe_node(self._root, parts, 0, client_id)

    def _unsubscribe_node(self, node: TrieNode, parts: list[str], depth: int, client_id: str) -> None:
        if depth == len(parts):
            node.subscribers.pop(client_id, None)
            return
        child = node.children.get(parts[depth])
        if child:
            self._unsubscribe_node(child, parts, depth + 1, client_id)

    def remove_client(self, client_id: str) -> None:
        self._remove_from_node(self._root, client_id)

    def _remove_from_node(self, node: TrieNode, client_id: str) -> None:
        node.subscribers.pop(client_id, None)
        for child in node.children.values():
            self._remove_from_node(child, client_id)

    def match(self, topic: str) -> list[tuple[str, Subscriber, int]]:
        """Return list of (client_id, callback, qos) matching the topic."""
        results: dict[str, tuple[Subscriber, int]] = {}
        self._match_node(self._root, topic.split("/"), 0, results)
        return [(cid, cb, qos) for cid, (cb, qos) in results.items()]

    def _match_node(self, node: TrieNode, parts: list[str], depth: int,
                    results: dict[str, tuple[Subscriber, int]]) -> None:
        if "#" in node.children:
            results.update(node.children["#"].subscribers)

        if depth == len(parts):
            results.update(node.subscribers)
            return

        part = parts[depth]
        if "+" in node.children:
            self._match_node(node.children["+"], parts, depth + 1, results)
        if part in node.children:
            self._match_node(node.children[part], parts, depth + 1, results)
