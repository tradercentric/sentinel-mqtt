"""MQTT broker server — asyncio TCP listener and client handler."""

from __future__ import annotations
import asyncio
import logging
import argparse

from ..protocol.packets import PacketType, QoS, ConnectReturnCode
from ..protocol.reader import read_packet, parse_connect, parse_publish, parse_subscribe, parse_unsubscribe
from ..protocol import writer as encode
from .router import TopicRouter
from .session import Session, SessionManager
from .retained import RetainedStore

log = logging.getLogger("sentinel.broker")


class Broker:
    def __init__(self, host: str = "0.0.0.0", port: int = 1883) -> None:
        self.host = host
        self.port = port
        self._router = TopicRouter()
        self._sessions = SessionManager(self._router)
        self._retained = RetainedStore()
        self._server: asyncio.AbstractServer | None = None

    async def listen(self) -> int:
        """Start listening; return the actual bound port."""
        self._server = await asyncio.start_server(self._handle_client, self.host, self.port)
        port = self._server.sockets[0].getsockname()[1]
        log.info("sentinel-mqtt listening on %s:%d", self.host, port)
        return port

    async def close(self) -> None:
        if self._server:
            self._server.close()
            await self._server.wait_closed()

    async def start(self) -> None:
        await self.listen()
        async with self._server:
            await self._server.serve_forever()

    async def _handle_client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        peer = writer.get_extra_info("peername")
        log.debug("new connection from %s", peer)
        session: Session | None = None
        try:
            session = await self._do_connect(reader, writer)
            if session is None:
                return
            await self._client_loop(session, reader, writer)
        except asyncio.IncompleteReadError:
            log.debug("client %s disconnected", peer)
        except Exception as exc:
            log.exception("error handling client %s: %s", peer, exc)
        finally:
            if session:
                await self._send_will(session)
                self._sessions.disconnect(session.client_id)
            writer.close()

    async def _do_connect(self, reader: asyncio.StreamReader,
                          writer: asyncio.StreamWriter) -> Session | None:
        ptype, flags, payload = await read_packet(reader)
        if ptype != PacketType.CONNECT:
            writer.close()
            return None

        pkt = parse_connect(payload)
        session, session_present = self._sessions.get_or_create(pkt.client_id, pkt.clean_session)
        session.writer = writer
        session.connected = True
        session.will_topic = pkt.will_topic
        session.will_message = pkt.will_message
        session.will_qos = int(pkt.will_qos)
        session.will_retain = pkt.will_retain

        writer.write(encode.encode_connack(ConnectReturnCode.ACCEPTED, session_present))
        await writer.drain()
        log.info("client connected: %s (clean=%s)", pkt.client_id, pkt.clean_session)

        # deliver retained messages for persistent session subscriptions
        if session_present:
            await self._deliver_retained_for_session(session)

        return session

    async def _client_loop(self, session: Session, reader: asyncio.StreamReader,
                           writer: asyncio.StreamWriter) -> None:
        while True:
            ptype, flags, payload = await read_packet(reader)

            if ptype == PacketType.PUBLISH:
                await self._handle_publish(session, flags, payload)

            elif ptype == PacketType.PUBACK:
                import struct
                pid = struct.unpack("!H", payload)[0]
                session.pending_qos1.pop(pid, None)

            elif ptype == PacketType.PUBREC:
                import struct
                pid = struct.unpack("!H", payload)[0]
                writer.write(encode.encode_pubrel(pid))
                await writer.drain()

            elif ptype == PacketType.PUBREL:
                import struct
                pid = struct.unpack("!H", payload)[0]
                session.pending_qos2_in.discard(pid)
                writer.write(encode.encode_pubcomp(pid))
                await writer.drain()

            elif ptype == PacketType.PUBCOMP:
                import struct
                pid = struct.unpack("!H", payload)[0]
                session.pending_qos2_out.pop(pid, None)

            elif ptype == PacketType.SUBSCRIBE:
                await self._handle_subscribe(session, payload)

            elif ptype == PacketType.UNSUBSCRIBE:
                await self._handle_unsubscribe(session, payload)

            elif ptype == PacketType.PINGREQ:
                writer.write(encode.encode_pingresp())
                await writer.drain()

            elif ptype == PacketType.DISCONNECT:
                session.will_topic = None
                break

    async def _handle_publish(self, session: Session, flags: int, payload: bytes) -> None:
        pkt = parse_publish(flags, payload)

        if pkt.retain:
            self._retained.set(pkt.topic, pkt.payload, int(pkt.qos))

        if pkt.qos == QoS.AT_LEAST_ONCE:
            assert pkt.packet_id is not None
            session.writer.write(encode.encode_puback(pkt.packet_id))
            await session.writer.drain()
        elif pkt.qos == QoS.EXACTLY_ONCE:
            assert pkt.packet_id is not None
            if pkt.packet_id in session.pending_qos2_in:
                return  # duplicate — already processing
            session.pending_qos2_in.add(pkt.packet_id)
            session.writer.write(encode.encode_pubrec(pkt.packet_id))
            await session.writer.drain()

        await self._route(pkt.topic, pkt.payload, int(pkt.qos), pkt.retain)

    async def _route(self, topic: str, payload: bytes, qos: int, retain: bool) -> None:
        for client_id, callback, sub_qos in self._router.match(topic):
            effective_qos = min(qos, sub_qos)
            callback(topic, payload, effective_qos, retain)

    async def _handle_subscribe(self, session: Session, payload: bytes) -> None:
        pkt = parse_subscribe(payload)
        return_codes = []
        for sub in pkt.subscriptions:
            effective_qos = min(int(sub.qos), 2)

            def make_callback(s: Session):
                def callback(topic: str, msg_payload: bytes, msg_qos: int, msg_retain: bool) -> None:
                    if s.connected and s.writer:
                        pid = s.allocate_packet_id() if msg_qos > 0 else None
                        data = encode.encode_publish(topic, msg_payload, QoS(msg_qos), False, pid)
                        s.writer.write(data)
                        asyncio.ensure_future(s.writer.drain())
                return callback

            self._sessions.add_subscription(session, sub.topic, effective_qos, make_callback(session))
            return_codes.append(effective_qos)

            # deliver matching retained messages immediately
            for msg in self._get_retained_matching(sub.topic):
                if session.connected and session.writer:
                    session.writer.write(
                        encode.encode_publish(msg.topic, msg.payload, QoS(msg.qos), True)
                    )

        session.writer.write(encode.encode_suback(pkt.packet_id, return_codes))
        await session.writer.drain()

    async def _handle_unsubscribe(self, session: Session, payload: bytes) -> None:
        pkt = parse_unsubscribe(payload)
        for topic in pkt.topics:
            self._sessions.remove_subscription(session, topic)
        session.writer.write(encode.encode_unsuback(pkt.packet_id))
        await session.writer.drain()

    async def _send_will(self, session: Session) -> None:
        if session.will_topic and session.will_message:
            await self._route(session.will_topic, session.will_message,
                              session.will_qos, session.will_retain)

    async def _deliver_retained_for_session(self, session: Session) -> None:
        for topic_filter in session.subscriptions:
            for msg in self._get_retained_matching(topic_filter):
                if session.writer:
                    session.writer.write(
                        encode.encode_publish(msg.topic, msg.payload, QoS(msg.qos), True)
                    )
        if session.writer:
            await session.writer.drain()

    def _get_retained_matching(self, topic_filter: str):
        from .router import TopicRouter
        router = TopicRouter()
        matches = []

        def collect(topic, payload, qos, retain):
            pass

        router.subscribe("_match", topic_filter, 0, collect)
        for msg in self._retained.all().values():
            if router.match(msg.topic):
                matches.append(msg)
        return matches


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    parser = argparse.ArgumentParser(description="sentinel-mqtt broker")
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=1883)
    args = parser.parse_args()

    broker = Broker(host=args.host, port=args.port)
    try:
        asyncio.run(broker.start())
    except KeyboardInterrupt:
        log.info("broker stopped")


if __name__ == "__main__":
    main()
