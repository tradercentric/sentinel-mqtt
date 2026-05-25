"""Tests for the topic router."""

import pytest
from sentinel.broker.router import TopicRouter


def test_exact_match():
    router = TopicRouter()
    received = []
    router.subscribe("c1", "home/temp", 0, lambda t, p, q, r: received.append(t))
    router.match("home/temp")[0][1]("home/temp", b"22", 0, False)
    assert received == ["home/temp"]


def test_single_wildcard():
    router = TopicRouter()
    received = []
    router.subscribe("c1", "home/+/temp", 0, lambda t, p, q, r: received.append(t))
    matches = router.match("home/room1/temp")
    assert len(matches) == 1
    matches[0][1]("home/room1/temp", b"22", 0, False)
    assert received == ["home/room1/temp"]


def test_multi_wildcard():
    router = TopicRouter()
    received = []
    router.subscribe("c1", "home/#", 0, lambda t, p, q, r: received.append(t))
    matches = router.match("home/room1/temp")
    assert len(matches) == 1


def test_no_match():
    router = TopicRouter()
    router.subscribe("c1", "sensors/light", 0, lambda t, p, q, r: None)
    assert router.match("sensors/temp") == []


def test_unsubscribe():
    router = TopicRouter()
    router.subscribe("c1", "test/topic", 0, lambda t, p, q, r: None)
    router.unsubscribe("c1", "test/topic")
    assert router.match("test/topic") == []


def test_remove_client():
    router = TopicRouter()
    router.subscribe("c1", "a/b", 0, lambda t, p, q, r: None)
    router.subscribe("c1", "c/d", 0, lambda t, p, q, r: None)
    router.remove_client("c1")
    assert router.match("a/b") == []
    assert router.match("c/d") == []
