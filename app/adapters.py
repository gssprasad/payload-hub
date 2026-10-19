"""Connection adapters: how Payload Hub talks to the outside world.

Each connection type has an Adapter with the same three operations:

    test()                    check the connection works
    send(target, body, ...)   deliver a message (outbound)
    listen(source, handler)   receive messages and hand each to `handler` (inbound)

Supported: RabbitMQ (AMQP), ActiveMQ / Artemis (STOMP), IBM MQ (needs IBM's MQ client libraries),
HTTP (outbound webhooks) and Kafka (external clusters or topics).
"""
import asyncio
import logging
import threading
from typing import Awaitable, Callable

log = logging.getLogger("adapters")

Handler = Callable[[bytes, dict], Awaitable[None]]

# Field specs drive the Connections form in the UI.
# kind: text | int | bool | secret | textarea; secret fields are never returned by the API.
TYPES = {
    "rabbitmq": {
        "label": "RabbitMQ (AMQP)", "icon": "🐇", "inbound": True, "outbound": True, "target": "Queue name",
        "fields": [
            {"key": "host", "label": "Host", "kind": "text", "default": "localhost"},
            {"key": "port", "label": "Port", "kind": "int", "default": 5672},
            {"key": "vhost", "label": "Virtual host", "kind": "text", "default": "/"},
            {"key": "username", "label": "Username", "kind": "text", "default": "guest"},
            {"key": "password", "label": "Password", "kind": "secret", "default": "guest"},
            {"key": "tls", "label": "Use TLS (amqps)", "kind": "bool", "default": False},
        ],
    },
    "activemq": {
        "label": "ActiveMQ / Artemis (STOMP)", "icon": "📨", "inbound": True, "outbound": True, "target": "Queue name",
        "fields": [
            {"key": "host", "label": "Host", "kind": "text", "default": "localhost"},
            {"key": "port", "label": "STOMP port", "kind": "int", "default": 61613},
            {"key": "username", "label": "Username", "kind": "text", "default": "admin"},
            {"key": "password", "label": "Password", "kind": "secret", "default": "admin"},
            {"key": "destination_prefix", "label": "Queue prefix", "kind": "text", "default": "/queue/",
             "help": "ActiveMQ Classic uses /queue/. For Artemis, use /queue/ if its STOMP acceptor sets anycastPrefix, otherwise leave empty."},
            {"key": "tls", "label": "Use TLS", "kind": "bool", "default": False},
        ],
    },
    "ibmmq": {
        "label": "IBM MQ", "icon": "🟦", "inbound": True, "outbound": True, "target": "Queue name",
        "fields": [
            {"key": "host", "label": "Host", "kind": "text", "default": "localhost"},
            {"key": "port", "label": "Port", "kind": "int", "default": 1414},
            {"key": "queue_manager", "label": "Queue manager", "kind": "text", "default": "QM1"},
            {"key": "channel", "label": "Channel", "kind": "text", "default": "DEV.APP.SVRCONN"},
            {"key": "username", "label": "Username", "kind": "text", "default": "app"},
            {"key": "password", "label": "Password", "kind": "secret", "default": ""},
        ],
    },
    "http": {
        "label": "HTTP endpoint (webhook)", "icon": "🌐", "inbound": False, "outbound": True,
        "target": "Path appended to the base URL (optional)",
        "fields": [
            {"key": "base_url", "label": "Base URL", "kind": "text", "default": "https://"},
            {"key": "method", "label": "Method", "kind": "text", "default": "POST"},
            {"key": "auth_header", "label": "Auth header (optional)", "kind": "secret", "default": "",
             "help": "Sent with every request, e.g. 'Authorization: Bearer <token>' or 'X-API-Key: <key>'."},
            {"key": "timeout", "label": "Timeout (seconds)", "kind": "int", "default": 15},
        ],
    },
    "kafka": {
        "label": "Kafka", "icon": "🟧", "inbound": True, "outbound": True, "target": "Topic",
        "fields": [
            {"key": "bootstrap", "label": "Bootstrap servers (host:port)", "kind": "text", "default": "localhost:9092"},
            {"key": "security_protocol", "label": "Security protocol", "kind": "text", "default": "PLAINTEXT",
             "help": "PLAINTEXT, SSL or SASL_SSL"},
            {"key": "sasl_mechanism", "label": "SASL mechanism", "kind": "text", "default": "SCRAM-SHA-256"},
            {"key": "username", "label": "Username (SASL)", "kind": "text", "default": ""},
            {"key": "password", "label": "Password (SASL)", "kind": "secret", "default": ""},
            {"key": "ca_cert", "label": "CA certificate (PEM, for SSL)", "kind": "textarea", "default": ""},
        ],
    },
}


class AdapterError(Exception):
    """A connection or delivery problem, with a message fit for the UI."""


class Listener:
    """Handle for a running inbound listener."""

    def __init__(self, stop: Callable[[], Awaitable[None]], describe: str):
        self._stop, self.describe = stop, describe

    async def stop(self):
        await self._stop()


class Adapter:
    def __init__(self, config: dict, secret: str | None):
        self.config, self.secret = config, secret or ""

    async def test(self) -> str:
        raise NotImplementedError

    async def send(self, target: str, body: bytes, headers: dict, content_type: str) -> str:
        raise NotImplementedError

    async def listen(self, source: str, handler: Handler) -> Listener:
        raise AdapterError(f"{TYPES[self.kind]['label']} connections can't be used for inbound messages.")

    async def close(self):
        pass

    kind = ""


# ---------- RabbitMQ ----------

class RabbitMQ(Adapter):
    kind = "rabbitmq"

    def __init__(self, config, secret):
        super().__init__(config, secret)
        self._conn = None
        self._lock = asyncio.Lock()

    async def _connection(self):
        import aio_pika

        async with self._lock:
            if self._conn is None or self._conn.is_closed:
                c = self.config
                try:
                    self._conn = await aio_pika.connect_robust(
                        host=c.get("host", "localhost"), port=int(c.get("port", 5672)),
                        login=c.get("username", "guest"), password=self.secret,
                        virtualhost=c.get("vhost", "/"), ssl=bool(c.get("tls")), timeout=10)
                except Exception as e:  # noqa: BLE001
                    raise AdapterError(f"Can't connect to RabbitMQ at {c.get('host')}:{c.get('port')}: {e}") from e
            return self._conn

    async def test(self) -> str:
        conn = await self._connection()
        channel = await conn.channel()
        await channel.close()
        return f"Connected to RabbitMQ {self.config.get('host')}:{self.config.get('port')} vhost {self.config.get('vhost', '/')}"

    async def send(self, target, body, headers, content_type):
        import aio_pika

        conn = await self._connection()
        async with conn.channel() as channel:
            await channel.declare_queue(target, durable=True)
            await channel.default_exchange.publish(
                aio_pika.Message(body, headers=headers, content_type=content_type,
                                 delivery_mode=aio_pika.DeliveryMode.PERSISTENT),
                routing_key=target)
        return f"published to RabbitMQ queue '{target}'"

    async def listen(self, source, handler):
        conn = await self._connection()
        channel = await conn.channel()
        await channel.set_qos(prefetch_count=10)
        queue = await channel.declare_queue(source, durable=True)

        async def on_message(message):
            try:
                await handler(message.body, {"headers": dict(message.headers or {}),
                                             "source": f"rabbitmq:{source}"})
            except Exception:  # noqa: BLE001 - leave it on the queue and try again later
                log.exception("inbound handler failed for RabbitMQ queue %s", source)
                await asyncio.sleep(2)
                await message.nack(requeue=True)
            else:
                await message.ack()

        tag = await queue.consume(on_message)

        async def stop():
            try:
                await queue.cancel(tag)
            finally:
                await channel.close()

        return Listener(stop, f"RabbitMQ queue '{source}'")

    async def close(self):
        if self._conn is not None and not self._conn.is_closed:
            await self._conn.close()


# ---------- ActiveMQ / Artemis over STOMP ----------

class ActiveMQ(Adapter):
    kind = "activemq"

    def _destination(self, name: str) -> str:
        return f"{self.config.get('destination_prefix', '/queue/')}{name}"

    def _connect(self, listener=None):
        import stomp

        c = self.config
        conn = stomp.Connection([(c.get("host", "localhost"), int(c.get("port", 61613)))], heartbeats=(10000, 10000))
        if c.get("tls"):
            conn.set_ssl(for_hosts=[(c.get("host"), int(c.get("port", 61613)))])
        if listener:
            conn.set_listener("payload-hub", listener)
        try:
            conn.connect(c.get("username") or None, self.secret or None, wait=True)
        except Exception as e:  # noqa: BLE001
            raise AdapterError(f"Can't connect to ActiveMQ at {c.get('host')}:{c.get('port')}: {str(e) or type(e).__name__}") from e
        return conn

    async def test(self):
        conn = await asyncio.to_thread(self._connect)
        await asyncio.to_thread(conn.disconnect)
        return f"Connected to ActiveMQ {self.config.get('host')}:{self.config.get('port')} over STOMP"

    async def send(self, target, body, headers, content_type):
        def run():
            conn = self._connect()
            try:
                conn.send(destination=self._destination(target), body=body, content_type=content_type,
                          headers={**{k: str(v) for k, v in headers.items()}, "persistent": "true"})
            finally:
                conn.disconnect()

        await asyncio.to_thread(run)
        return f"sent to ActiveMQ queue '{target}'"

    async def listen(self, source, handler):
        import stomp

        loop = asyncio.get_running_loop()
        destination = self._destination(source)
        state = {"conn": None, "stopped": False}

        adapter = self

        class OnMessage(stomp.ConnectionListener):
            def on_message(self, frame):
                conn = state["conn"]
                msg_id, sub = frame.headers.get("message-id"), frame.headers.get("subscription", "1")
                body = frame.body.encode() if isinstance(frame.body, str) else frame.body
                future = asyncio.run_coroutine_threadsafe(
                    handler(body, {"headers": dict(frame.headers), "source": f"activemq:{source}"}), loop)
                try:
                    future.result(timeout=60)
                    conn.ack(msg_id, sub)
                except Exception:  # noqa: BLE001
                    log.exception("inbound handler failed for ActiveMQ queue %s", source)
                    conn.nack(msg_id, sub)

            def on_disconnected(self):
                if not state["stopped"]:
                    log.warning("ActiveMQ listener on %s disconnected; the supervisor will reconnect it", source)

        def start():
            conn = adapter._connect(OnMessage())
            conn.subscribe(destination=destination, id="1", ack="client-individual")
            state["conn"] = conn

        await asyncio.to_thread(start)

        async def stop():
            state["stopped"] = True
            if state["conn"] is not None:
                await asyncio.to_thread(state["conn"].disconnect)

        listener = Listener(stop, f"ActiveMQ queue '{source}'")
        listener.alive = lambda: state["conn"] is not None and state["conn"].is_connected()
        return listener


# ---------- IBM MQ ----------

def _ibmmq():
    try:
        import ibmmq as mq  # IBM's current package
        return mq
    except ImportError:
        try:
            import pymqi as mq  # older community package with the same API
            return mq
        except ImportError as e:
            raise AdapterError(
                "IBM MQ support needs IBM's MQ client: install the IBM MQ redistributable client (or MQ for "
                "Developers) from IBM, then `pip install ibmmq` in this project's environment.") from e


class IBMMQ(Adapter):
    kind = "ibmmq"

    def _connect(self):
        mq = _ibmmq()
        c = self.config
        try:
            return mq.connect(c.get("queue_manager", "QM1"), c.get("channel", "DEV.APP.SVRCONN"),
                              f"{c.get('host', 'localhost')}({int(c.get('port', 1414))})",
                              c.get("username") or None, self.secret or None)
        except Exception as e:  # noqa: BLE001
            raise AdapterError(f"Can't connect to IBM MQ queue manager {c.get('queue_manager')} at "
                               f"{c.get('host')}({c.get('port')}): {e}") from e

    async def test(self):
        qmgr = await asyncio.to_thread(self._connect)
        await asyncio.to_thread(qmgr.disconnect)
        return f"Connected to IBM MQ queue manager {self.config.get('queue_manager')}"

    async def send(self, target, body, headers, content_type):
        mq = _ibmmq()

        def run():
            qmgr = self._connect()
            try:
                queue = mq.Queue(qmgr, target)
                md = mq.MD()
                md.Format = mq.CMQC.MQFMT_STRING
                queue.put(body, md)
                queue.close()
            finally:
                qmgr.disconnect()

        await asyncio.to_thread(run)
        return f"put to IBM MQ queue '{target}'"

    async def listen(self, source, handler):
        mq = _ibmmq()
        loop = asyncio.get_running_loop()
        stop_event = threading.Event()
        qmgr = await asyncio.to_thread(self._connect)

        def run():
            queue = mq.Queue(qmgr, source)
            gmo = mq.GMO()
            gmo.Options = mq.CMQC.MQGMO_WAIT | mq.CMQC.MQGMO_FAIL_IF_QUIESCING | mq.CMQC.MQGMO_SYNCPOINT
            gmo.WaitInterval = 1000
            try:
                while not stop_event.is_set():
                    md = mq.MD()
                    try:
                        body = queue.get(None, md, gmo)
                    except mq.MQMIError as e:
                        if e.reason == mq.CMQC.MQRC_NO_MSG_AVAILABLE:
                            continue
                        raise
                    try:
                        asyncio.run_coroutine_threadsafe(
                            handler(body, {"headers": {}, "source": f"ibmmq:{source}"}), loop).result(timeout=60)
                        qmgr.commit()
                    except Exception:  # noqa: BLE001
                        log.exception("inbound handler failed for IBM MQ queue %s", source)
                        qmgr.backout()
            finally:
                queue.close()
                qmgr.disconnect()

        thread = threading.Thread(target=run, name=f"ibmmq-{source}", daemon=True)
        thread.start()

        async def stop():
            stop_event.set()
            await asyncio.to_thread(thread.join, 5)

        listener = Listener(stop, f"IBM MQ queue '{source}'")
        listener.alive = thread.is_alive
        return listener


# ---------- HTTP (outbound only) ----------

class HTTP(Adapter):
    kind = "http"

    def _headers(self) -> dict:
        header = self.secret.strip()
        if ":" in header:
            name, value = header.split(":", 1)
            return {name.strip(): value.strip()}
        return {}

    def _url(self, target: str) -> str:
        base = self.config.get("base_url", "").rstrip("/")
        return f"{base}/{target.lstrip('/')}" if target else base

    async def test(self):
        import httpx

        url = self._url("")
        try:
            async with httpx.AsyncClient(timeout=int(self.config.get("timeout", 15))) as client:
                r = await client.request("HEAD", url, headers=self._headers())
        except Exception as e:  # noqa: BLE001
            raise AdapterError(f"Can't reach {url}: {str(e) or type(e).__name__}") from e
        if r.status_code >= 500:
            raise AdapterError(f"{url} answered HTTP {r.status_code}")
        return f"Reached {url} (HTTP {r.status_code})"

    async def send(self, target, body, headers, content_type):
        import httpx

        url = self._url(target)
        try:
            async with httpx.AsyncClient(timeout=int(self.config.get("timeout", 15))) as client:
                r = await client.request(self.config.get("method", "POST").upper(), url, content=body,
                                         headers={"Content-Type": content_type, **self._headers(),
                                                  **{k: str(v) for k, v in headers.items()}})
        except Exception as e:  # noqa: BLE001
            raise AdapterError(f"Can't reach {url}: {str(e) or type(e).__name__}") from e
        if r.status_code >= 300:
            raise AdapterError(f"{url} answered HTTP {r.status_code}: {r.text[:200]}")
        return f"{self.config.get('method', 'POST').upper()} {url} → HTTP {r.status_code}"


# ---------- Kafka (external cluster or topic) ----------

class Kafka(Adapter):
    kind = "kafka"

    def _conn(self) -> dict:
        from aiokafka.helpers import create_ssl_context

        c = self.config
        protocol = (c.get("security_protocol") or "PLAINTEXT").upper()
        conn = {"bootstrap_servers": c.get("bootstrap", "localhost:9092"), "security_protocol": protocol}
        if protocol in ("SSL", "SASL_SSL"):
            conn["ssl_context"] = create_ssl_context(cadata=c.get("ca_cert") or None)
        if protocol.startswith("SASL"):
            conn.update(sasl_mechanism=c.get("sasl_mechanism") or "SCRAM-SHA-256",
                        sasl_plain_username=c.get("username"), sasl_plain_password=self.secret)
        return conn

    async def test(self):
        from aiokafka.admin import AIOKafkaAdminClient

        admin = AIOKafkaAdminClient(**self._conn(), request_timeout_ms=15000)
        try:
            await asyncio.wait_for(admin.start(), 20)
            topics = await admin.list_topics()
        except Exception as e:  # noqa: BLE001
            raise AdapterError(f"Can't connect to Kafka at {self.config.get('bootstrap')}: {str(e) or type(e).__name__}") from e
        finally:
            await admin.close()
        return f"Connected to Kafka {self.config.get('bootstrap')} ({len(topics)} topics)"

    async def send(self, target, body, headers, content_type):
        from aiokafka import AIOKafkaProducer

        producer = AIOKafkaProducer(**self._conn())
        try:
            await producer.start()
            meta = await producer.send_and_wait(target, body, headers=[(k, str(v).encode()) for k, v in headers.items()])
        except Exception as e:  # noqa: BLE001
            raise AdapterError(f"Can't produce to Kafka topic '{target}': {str(e) or type(e).__name__}") from e
        finally:
            await producer.stop()
        return f"produced to Kafka topic '{target}' partition {meta.partition} offset {meta.offset}"

    async def listen(self, source, handler):
        from aiokafka import AIOKafkaConsumer, TopicPartition

        consumer = AIOKafkaConsumer(source, **self._conn(), group_id=f"payload-hub-inbound-{source}",
                                    enable_auto_commit=False, auto_offset_reset="earliest")
        await consumer.start()

        async def run():
            async for rec in consumer:
                try:
                    await handler(rec.value, {"headers": {k: v.decode(errors="replace") for k, v in rec.headers or []},
                                              "source": f"kafka:{source}"})
                    await consumer.commit()
                except Exception:  # noqa: BLE001
                    log.exception("inbound handler failed for Kafka topic %s", source)
                    await asyncio.sleep(2)
                    consumer.seek(TopicPartition(rec.topic, rec.partition), rec.offset)  # retry this message

        task = asyncio.create_task(run(), name=f"kafka-inbound-{source}")

        async def stop():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
            await consumer.stop()

        listener = Listener(stop, f"Kafka topic '{source}'")
        listener.alive = lambda: not task.done()
        return listener


ADAPTERS = {cls.kind: cls for cls in (RabbitMQ, ActiveMQ, IBMMQ, HTTP, Kafka)}


def make(conn: dict) -> Adapter:
    """Build an adapter from a stored connection row (type, config, secret)."""
    return ADAPTERS[conn["type"]](conn["config"], conn.get("secret"))


def ibmmq_available() -> bool:
    try:
        _ibmmq()
        return True
    except AdapterError:
        return False
