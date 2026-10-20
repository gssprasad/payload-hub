"""Connection adapters: how Payload Hub talks to the outside world.

Each connection type has an Adapter with the same three operations:

    test()                    check the connection works
    send(target, body, ...)   deliver a message (outbound)
    listen(source, handler)   receive messages and hand each to `handler` (inbound)

Supported: RabbitMQ (AMQP), ActiveMQ / Artemis (STOMP), IBM MQ (needs IBM's MQ client libraries),
HTTP (outbound webhooks) and Kafka (external clusters or topics).
"""
import asyncio
import json
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
    "database": {
        "label": "Database (PostgreSQL, MySQL, SQL Server, Oracle, SQLite)", "icon": "🗄️", "inbound": True,
        "outbound": True, "target": "Table (inbound: table:key_column)",
        "fields": [
            {"key": "engine", "label": "Database type", "kind": "select", "default": "postgresql",
             "options": ["postgresql", "mysql", "sqlserver", "oracle", "sqlite"]},
            {"key": "host", "label": "Host", "kind": "text", "default": "localhost"},
            {"key": "port", "label": "Port", "kind": "int", "default": 5432,
             "help": "PostgreSQL 5432 · MySQL 3306 · SQL Server 1433 · Oracle 1521 · ignored for SQLite"},
            {"key": "database", "label": "Database / service name / SQLite file", "kind": "text", "default": ""},
            {"key": "username", "label": "Username", "kind": "text", "default": ""},
            {"key": "password", "label": "Password", "kind": "secret", "default": ""},
            {"key": "options", "label": "Extra options (query string)", "kind": "text", "default": "",
             "help": "e.g. sslmode=require for hosted PostgreSQL, or charset=utf8mb4 for MySQL."},
            {"key": "poll_interval", "label": "Inbound poll interval (seconds)", "kind": "int", "default": 10,
             "help": "How often new rows are checked when this connection is used for inbound messages."},
        ],
    },
    "ftp": {
        "label": "FTP / FTPS / SFTP (files)", "icon": "📁", "inbound": True, "outbound": True,
        "target": "Folder (relative to the base folder)",
        "fields": [
            {"key": "protocol", "label": "Protocol", "kind": "select", "default": "sftp", "options": ["sftp", "ftp", "ftps"]},
            {"key": "host", "label": "Host", "kind": "text", "default": "localhost"},
            {"key": "port", "label": "Port", "kind": "int", "default": 22, "help": "SFTP 22 · FTP/FTPS 21"},
            {"key": "username", "label": "Username", "kind": "text", "default": ""},
            {"key": "password", "label": "Password", "kind": "secret", "default": ""},
            {"key": "base_dir", "label": "Base folder", "kind": "text", "default": "/",
             "help": "Inbound/outbound folders are relative to this."},
            {"key": "file_pattern", "label": "Inbound file pattern", "kind": "text", "default": "*",
             "help": "Which files to pick up, e.g. *.xml or orders_*.json"},
            {"key": "after_read", "label": "After reading a file", "kind": "select", "default": "archive",
             "options": ["archive", "delete"]},
            {"key": "archive_dir", "label": "Archive subfolder", "kind": "text", "default": "processed"},
            {"key": "filename_pattern", "label": "Outbound file name", "kind": "text",
             "default": "{partner}_{timestamp}_{txn}.{ext}",
             "help": "Placeholders: {partner} {txn} {timestamp} {date} {ext}"},
            {"key": "poll_interval", "label": "Inbound poll interval (seconds)", "kind": "int", "default": 15},
            {"key": "passive", "label": "Passive mode (FTP/FTPS)", "kind": "bool", "default": True},
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


RETRY_FIELDS = [
    {"key": "retry_attempts", "label": "Delivery attempts", "kind": "int", "default": 3, "group": "retry",
     "help": "How many times to try an outbound delivery before giving up (1 = no retries)."},
    {"key": "retry_backoff", "label": "First retry after (seconds)", "kind": "int", "default": 2, "group": "retry",
     "help": "Wait before the first retry; each later retry waits twice as long."},
    {"key": "retry_backoff_max", "label": "Longest wait (seconds)", "kind": "int", "default": 30, "group": "retry",
     "help": "Cap on the wait between retries."},
]
HTTP_RETRY_STATUS = {"key": "retry_on_status", "label": "Retry on HTTP status", "kind": "text",
                     "default": "408,425,429,500,502,503,504", "group": "retry",
                     "help": "Comma-separated. Other errors (e.g. 400, 401, 404) are treated as permanent."}


class AdapterError(Exception):
    """A connection or delivery problem, with a message fit for the UI.
    retryable=False marks permanent failures (e.g. HTTP 400) that retrying won't fix."""

    def __init__(self, message: str, retryable: bool = True):
        super().__init__(message)
        self.retryable = retryable


def retry_policy(config: dict) -> dict:
    def number(key, default, low):
        try:
            return max(low, int(config.get(key, default)))
        except (TypeError, ValueError):
            return default

    return {"attempts": number("retry_attempts", 3, 1), "backoff": number("retry_backoff", 2, 0),
            "backoff_max": number("retry_backoff_max", 30, 0)}


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
            async with httpx.AsyncClient(timeout=int(self.config.get("timeout", 15)), follow_redirects=True) as client:
                r = await client.request("HEAD", url, headers=self._headers())
        except Exception as e:  # noqa: BLE001
            raise AdapterError(f"Can't reach {url}: {str(e) or type(e).__name__}") from e
        if r.status_code >= 500:
            raise AdapterError(f"{url} answered HTTP {r.status_code}")
        return f"Reached {url} (HTTP {r.status_code})"

    async def request(self, target, body, headers, content_type, method: str | None = None) -> tuple[int, str, str, str]:
        """Call the endpoint and return (status, content_type, body, url) - used for request/reply forwarding
        and workflow API steps. method overrides the connection's default method."""
        import httpx

        url = self._url(target)
        method = (method or self.config.get("method", "POST")).upper()
        try:
            async with httpx.AsyncClient(timeout=int(self.config.get("timeout", 15)), follow_redirects=True) as client:
                r = await client.request(method, url, content=body if method not in ("GET", "HEAD") else None,
                                         headers={"Content-Type": content_type, "Accept": content_type,
                                                  **self._headers(), **{k: str(v) for k, v in headers.items()}})
        except Exception as e:  # noqa: BLE001
            raise AdapterError(f"Can't reach {url}: {str(e) or type(e).__name__}") from e
        return r.status_code, r.headers.get("content-type", ""), r.text, url

    def _retry_statuses(self) -> set[int]:
        raw = str(self.config.get("retry_on_status", HTTP_RETRY_STATUS["default"]))
        return {int(x) for x in raw.replace(" ", "").split(",") if x.isdigit()}

    async def send(self, target, body, headers, content_type):
        status, _, text, url = await self.request(target, body, headers, content_type)
        if status >= 300:
            raise AdapterError(f"{url} answered HTTP {status}: {text[:200]}",
                               retryable=status in self._retry_statuses())
        return f"{self.config.get('method', 'POST').upper()} {url} → HTTP {status}"


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

    async def topics(self) -> list[dict]:
        """Topics with partitions and message counts (internal topics hidden)."""
        from aiokafka import AIOKafkaConsumer, TopicPartition
        from aiokafka.admin import AIOKafkaAdminClient

        admin = AIOKafkaAdminClient(**self._conn(), request_timeout_ms=15000)
        probe = AIOKafkaConsumer(**self._conn(), enable_auto_commit=False)
        try:
            await asyncio.wait_for(admin.start(), 20)
            await asyncio.wait_for(probe.start(), 20)
            names = sorted(t for t in await admin.list_topics() if not t.startswith("__"))
            described = await admin.describe_topics(names) if names else []
            parts = [TopicPartition(d["topic"], p["partition"]) for d in described for p in d["partitions"]]
            ends = await probe.end_offsets(parts) if parts else {}
            starts = await probe.beginning_offsets(parts) if parts else {}
            return sorted([{"topic": d["topic"], "partitions": len(d["partitions"]),
                            "messages": sum(ends[tp] - starts[tp] for tp in parts if tp.topic == d["topic"])}
                           for d in described], key=lambda t: t["topic"])
        except Exception as e:  # noqa: BLE001
            raise AdapterError(f"Kafka {self.config.get('bootstrap')}: {str(e) or type(e).__name__}") from e
        finally:
            await probe.stop()
            await admin.close()

    async def peek(self, topic: str, limit: int = 20) -> list[dict]:
        """The latest `limit` messages of a topic (read-only: no consumer group, nothing committed)."""
        from aiokafka import AIOKafkaConsumer, TopicPartition

        from aiokafka.admin import AIOKafkaAdminClient

        admin = AIOKafkaAdminClient(**self._conn(), request_timeout_ms=15000)
        try:
            await asyncio.wait_for(admin.start(), 20)
            if topic not in await admin.list_topics():
                raise AdapterError(f"Topic '{topic}' not found.", retryable=False)
            partitions = [p["partition"] for p in (await admin.describe_topics([topic]))[0]["partitions"]]
        finally:
            await admin.close()
        consumer = AIOKafkaConsumer(**self._conn(), enable_auto_commit=False, group_id=None)
        try:
            await asyncio.wait_for(consumer.start(), 20)
            tps = [TopicPartition(topic, p) for p in partitions]
            consumer.assign(tps)
            ends = await consumer.end_offsets(tps)
            starts = await consumer.beginning_offsets(tps)
            wanted = 0
            for tp in tps:
                start = max(starts[tp], ends[tp] - limit)
                wanted += ends[tp] - start
                consumer.seek(tp, start)
            out = []
            while len(out) < wanted:
                batch = await consumer.getmany(timeout_ms=2000, max_records=500)
                if not batch:
                    break
                for tp, records in batch.items():
                    for r in records:
                        out.append({"partition": r.partition, "offset": r.offset, "timestamp": r.timestamp,
                                    "key": r.key.decode(errors="replace") if r.key else None,
                                    "value": (r.value or b"").decode(errors="replace")})
            return sorted(out, key=lambda m: m["timestamp"], reverse=True)[:limit]
        except AdapterError:
            raise
        except Exception as e:  # noqa: BLE001
            raise AdapterError(f"Kafka {self.config.get('bootstrap')}: {str(e) or type(e).__name__}") from e
        finally:
            await consumer.stop()

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


# ---------- Files over FTP / FTPS / SFTP ----------

class FileTransfer(Adapter):
    """Reads files from and writes files to an FTP, FTPS or SFTP server."""

    kind = "ftp"

    def _path(self, *parts) -> str:
        import posixpath

        return posixpath.normpath(posixpath.join(self.config.get("base_dir") or "/", *[p for p in parts if p]))

    def _describe(self) -> str:
        c = self.config
        return f"{c.get('protocol', 'sftp')}://{c.get('host')}:{c.get('port')}"

    # -- low-level operations: one session per call (simple and robust for polling) --

    async def _ftp(self, fn):
        """Run fn(ftp) in a thread with a fresh FTP/FTPS session."""
        import ftplib

        c = self.config

        def run():
            ftp = ftplib.FTP_TLS(timeout=20) if c.get("protocol") == "ftps" else ftplib.FTP(timeout=20)
            try:
                ftp.connect(c.get("host"), int(c.get("port") or 21))
                ftp.login(c.get("username") or "anonymous", self.secret or "")
                if c.get("protocol") == "ftps":
                    ftp.prot_p()
                ftp.set_pasv(bool(c.get("passive", True)))
                return fn(ftp)
            finally:
                try:
                    ftp.quit()
                except Exception:  # noqa: BLE001
                    ftp.close()

        try:
            return await asyncio.to_thread(run)
        except AdapterError:
            raise
        except Exception as e:  # noqa: BLE001
            raise AdapterError(f"{self._describe()}: {str(e) or type(e).__name__}") from e

    async def _sftp(self, fn):
        """Run async fn(sftp) with a fresh SFTP session."""
        import asyncssh

        c = self.config
        try:
            async with asyncssh.connect(c.get("host"), port=int(c.get("port") or 22), username=c.get("username"),
                                        password=self.secret or None, known_hosts=None,
                                        connect_timeout=20) as conn:
                async with conn.start_sftp_client() as sftp:
                    return await fn(sftp)
        except AdapterError:
            raise
        except Exception as e:  # noqa: BLE001
            raise AdapterError(f"{self._describe()}: {str(e) or type(e).__name__}") from e

    async def list_dir(self, folder: str = "") -> list[dict]:
        path = self._path(folder)
        if self.config.get("protocol") == "sftp":
            async def run(sftp):
                out = []
                for e in await sftp.readdir(path):
                    if e.filename in (".", ".."):
                        continue
                    out.append({"name": e.filename, "size": e.attrs.size or 0,
                                "is_dir": e.attrs.type == 2, "modified": e.attrs.mtime})
                return out
            entries = await self._sftp(run)
        else:
            def run(ftp):
                try:
                    return [{"name": n, "size": int(f.get("size", 0) or 0), "is_dir": f.get("type") == "dir",
                             "modified": f.get("modify")}
                            for n, f in ftp.mlsd(path, facts=["type", "size", "modify"]) if f.get("type") in ("file", "dir")]
                except Exception:  # noqa: BLE001 - servers without MLSD
                    out = []
                    for full in ftp.nlst(path):
                        n = full.rsplit("/", 1)[-1]
                        try:
                            out.append({"name": n, "size": ftp.size(f"{path}/{n}") or 0, "is_dir": False, "modified": None})
                        except Exception:  # noqa: BLE001
                            out.append({"name": n, "size": 0, "is_dir": True, "modified": None})
                    return out
            entries = await self._ftp(run)
        return sorted(entries, key=lambda e: (not e["is_dir"], e["name"]))

    async def read_file(self, path: str, max_bytes: int | None = None) -> bytes:
        full = self._path(path)
        if self.config.get("protocol") == "sftp":
            async def run(sftp):
                async with sftp.open(full, "rb") as f:
                    return await f.read(max_bytes if max_bytes else -1)
            return await self._sftp(run)

        def run(ftp):
            import io

            buf = io.BytesIO()
            ftp.retrbinary(f"RETR {full}", buf.write)
            return buf.getvalue()[:max_bytes] if max_bytes else buf.getvalue()
        return await self._ftp(run)

    async def write_file(self, folder: str, name: str, body: bytes) -> str:
        """Write via a temporary .part name, then rename, so readers never see half a file."""
        import posixpath

        folder_path = self._path(folder)
        final, temp = posixpath.join(folder_path, name), posixpath.join(folder_path, f".{name}.part")
        if self.config.get("protocol") == "sftp":
            async def run(sftp):
                if not await sftp.exists(folder_path):
                    await sftp.makedirs(folder_path, exist_ok=True)
                async with sftp.open(temp, "wb") as f:
                    await f.write(body)
                if await sftp.exists(final):
                    await sftp.remove(final)
                await sftp.rename(temp, final)
            await self._sftp(run)
        else:
            def run(ftp):
                import io

                try:
                    ftp.mkd(folder_path)
                except Exception:  # noqa: BLE001 - already exists
                    pass
                ftp.storbinary(f"STOR {temp}", io.BytesIO(body))
                try:
                    ftp.delete(final)
                except Exception:  # noqa: BLE001
                    pass
                ftp.rename(temp, final)
            await self._ftp(run)
        return final

    async def finish(self, folder: str, name: str):
        """After a file was processed: move it to the archive subfolder, or delete it."""
        import posixpath

        src = self._path(folder, name)
        archive_dir = self._path(folder, self.config.get("archive_dir") or "processed")
        stamp = __import__("datetime").datetime.now().strftime("%Y%m%dT%H%M%S")
        dst = posixpath.join(archive_dir, f"{stamp}_{name}")
        delete = self.config.get("after_read") == "delete"
        if self.config.get("protocol") == "sftp":
            async def run(sftp):
                if delete:
                    await sftp.remove(src)
                    return
                await sftp.makedirs(archive_dir, exist_ok=True)
                await sftp.rename(src, dst)
            await self._sftp(run)
        else:
            def run(ftp):
                if delete:
                    ftp.delete(src)
                    return
                try:
                    ftp.mkd(archive_dir)
                except Exception:  # noqa: BLE001
                    pass
                ftp.rename(src, dst)
            await self._ftp(run)

    # -- adapter interface --

    async def test(self):
        entries = await self.list_dir("")
        return f"Connected to {self._describe()}{self._path()} ({len(entries)} entries)"

    async def send(self, target, body, headers, content_type):
        from datetime import datetime, timezone

        now = datetime.now(timezone.utc)
        ext = "xml" if "xml" in content_type else "json" if "json" in content_type else "txt"
        pattern = self.config.get("filename_pattern") or "{partner}_{timestamp}_{txn}.{ext}"
        if headers.get("X-Filename"):  # e.g. a PDF from a workflow keeps its own name
            name = headers["X-Filename"]
            path = await self.write_file(target, name, body)
            return f"wrote {path} ({len(body)} bytes) on {self._describe()}"
        try:
            name = pattern.format(partner=headers.get("X-Partner", "payload"),
                                  txn=str(headers.get("X-Transaction-Id", "manual"))[:8],
                                  timestamp=now.strftime("%Y%m%dT%H%M%S%f")[:-3], date=now.strftime("%Y-%m-%d"),
                                  ext=ext)
        except (KeyError, IndexError) as e:
            raise AdapterError(f"Unknown placeholder {e} in the outbound file name pattern.", retryable=False) from e
        if "/" in name or name.startswith("."):
            raise AdapterError("The outbound file name can't contain '/' or start with '.'.", retryable=False)
        path = await self.write_file(target, name, body)
        return f"wrote {path} ({len(body)} bytes) on {self._describe()}"

    async def listen(self, source, handler):
        import fnmatch

        pattern = self.config.get("file_pattern") or "*"
        interval = max(3, int(self.config.get("poll_interval") or 15))
        await self.list_dir(source)  # fail fast if the folder doesn't exist
        stopped = asyncio.Event()
        last_sizes: dict[str, int] = {}

        async def run():
            nonlocal last_sizes
            while not stopped.is_set():
                try:
                    files = [e for e in await self.list_dir(source)
                             if not e["is_dir"] and not e["name"].startswith(".") and fnmatch.fnmatch(e["name"], pattern)]
                    sizes = {e["name"]: e["size"] for e in files}
                    for name, size in sizes.items():
                        if last_sizes.get(name) != size:
                            continue  # new or still growing - wait until the size is stable for one poll
                        body = await self.read_file(f"{source}/{name}" if source else name)
                        await handler(body, {"headers": {"filename": name},
                                             "source": f"{self.config.get('protocol')}:{self._path(source, name)}"})
                        await self.finish(source, name)
                        sizes.pop(name, None)
                    last_sizes = sizes
                except Exception:  # noqa: BLE001
                    log.exception("file poll on %s failed", source)
                try:
                    await asyncio.wait_for(stopped.wait(), interval)
                except asyncio.TimeoutError:
                    pass

        task = asyncio.create_task(run(), name=f"ftp-poll-{source}")

        async def stop():
            stopped.set()
            await asyncio.gather(task, return_exceptions=True)

        listener = Listener(stop, f"{self.config.get('protocol', 'sftp').upper()} folder "
                                  f"'{self._path(source)}' ({pattern}, every {interval} s)")
        listener.alive = lambda: not task.done()
        return listener


# ---------- Databases (SQLAlchemy) ----------

DIALECTS = {"postgresql": "postgresql+psycopg", "mysql": "mysql+pymysql", "sqlserver": "mssql+pymssql",
            "oracle": "oracle+oracledb", "sqlite": "sqlite"}
READ_KINDS = {"SELECT", "WITH", "SHOW", "EXPLAIN", "VALUES", "TABLE", "DESCRIBE", "DESC"}
WRITE_KINDS = {"INSERT", "UPDATE", "CREATE", "ALTER", "COMMENT", "MERGE"}
DELETE_KINDS = {"DELETE", "DROP", "TRUNCATE"}
SQL_TYPES = {"text": "String", "integer": "Integer", "bigint": "BigInteger", "numeric": "Numeric",
             "boolean": "Boolean", "date": "Date", "timestamp": "DateTime", "json": "JSON", "uuid": "Uuid"}


def classify_sql(statement: str) -> tuple[str, str]:
    """(clean statement, permission needed: read | write | delete); one statement at a time."""
    import re

    text = re.sub(r"--[^\n]*", " ", statement)
    text = re.sub(r"/\*.*?\*/", " ", text, flags=re.S).strip().rstrip(";").strip()
    if not text:
        raise AdapterError("Enter a SQL statement.", retryable=False)
    if ";" in text:
        raise AdapterError("Run one statement at a time (remove the extra ';').", retryable=False)
    kind = text.split(None, 1)[0].upper()
    for kinds, perm in ((READ_KINDS, "read"), (WRITE_KINDS, "write"), (DELETE_KINDS, "delete")):
        if kind in kinds:
            return text, perm
    raise AdapterError(f"{kind} statements aren't allowed here.", retryable=False)


class Database(Adapter):
    kind = "database"

    def __init__(self, config, secret):
        super().__init__(config, secret)
        self._engine = None

    def engine(self):
        import sqlalchemy as sa
        from urllib.parse import parse_qsl

        if self._engine is None:
            c = self.config
            eng = c.get("engine", "postgresql")
            query = dict(parse_qsl(c.get("options") or ""))
            if eng == "sqlite":
                url = sa.engine.URL.create("sqlite", database=c.get("database") or ":memory:")
            elif eng == "oracle":
                query.setdefault("service_name", c.get("database") or "")
                url = sa.engine.URL.create(DIALECTS[eng], username=c.get("username") or None,
                                           password=self.secret or None, host=c.get("host"),
                                           port=int(c.get("port") or 1521), query=query)
            else:
                url = sa.engine.URL.create(DIALECTS[eng], username=c.get("username") or None,
                                           password=self.secret or None, host=c.get("host"),
                                           port=int(c.get("port")) if c.get("port") else None,
                                           database=c.get("database") or None, query=query)
            connect_args = self.config.get("_connect_args") or {}
            self._engine = sa.create_engine(url, pool_pre_ping=True, pool_size=3, max_overflow=2,
                                            connect_args=connect_args) \
                if eng != "sqlite" else sa.create_engine(url)
        return self._engine

    def _describe(self) -> str:
        c = self.config
        if c.get("engine") == "sqlite":
            return f"SQLite {c.get('database')}"
        return f"{c.get('engine')} {c.get('host')}:{c.get('port')}/{c.get('database')}"

    async def _run(self, fn):
        try:
            return await asyncio.to_thread(fn)
        except AdapterError:
            raise
        except Exception as e:  # noqa: BLE001
            msg = str(getattr(e, "orig", None) or e).strip().splitlines()[0][:300]
            raise AdapterError(f"{self._describe()}: {msg}") from e

    async def test(self):
        import sqlalchemy as sa

        def run():
            with self.engine().connect() as conn:
                one = "SELECT 1 FROM DUAL" if self.config.get("engine") == "oracle" else "SELECT 1"
                conn.execute(sa.text(one))
                return conn.dialect.server_version_info

        version = await self._run(run)
        v = ".".join(str(x) for x in version[:3]) if version else ""
        return f"Connected to {self._describe()}" + (f" (server {v})" if v else "")

    async def tables(self) -> list[dict]:
        import sqlalchemy as sa

        def run():
            insp = sa.inspect(self.engine())
            out = []
            for name in sorted(insp.get_table_names()):
                cols = [{"name": c["name"], "type": str(c["type"]).lower(), "nullable": c.get("nullable", True),
                         "default": c.get("default")} for c in insp.get_columns(name)]
                with self.engine().connect() as conn:
                    rows = conn.execute(sa.select(sa.func.count()).select_from(sa.table(name))).scalar()
                out.append({"name": name, "columns": cols, "rows": rows})
            return out

        return await self._run(run)

    async def query(self, statement: str, permission: str, max_rows: int = 1000) -> dict:
        """Run one statement. Reads always roll back, so they can't change anything."""
        import sqlalchemy as sa

        def run():
            with self.engine().connect() as conn:
                trans = conn.begin()
                try:
                    result = conn.execute(sa.text(statement))
                    columns = list(result.keys()) if result.returns_rows else []
                    rows = [dict(r) for r in result.mappings().fetchmany(max_rows)] if columns else []
                    count = result.rowcount
                    if permission == "read":
                        trans.rollback()
                    else:
                        trans.commit()
                except Exception:
                    trans.rollback()
                    raise
            return {"columns": columns, "rows": rows, "rowcount": count,
                    "status": f"{statement.split(None, 1)[0].upper()} {count if count >= 0 else ''}".strip(),
                    "truncated": len(rows) == max_rows}

        return await self._run(run)

    async def execute(self, sql: str, params: dict, write: bool = False, max_rows: int = 1000) -> dict:
        """Run one parameterised statement (:name placeholders). Reads are rolled back; writes are committed."""
        import sqlalchemy as sa

        if not (sql or "").strip():
            raise AdapterError("The step has no SQL.", retryable=False)

        def run():
            with self.engine().connect() as conn:
                trans = conn.begin()
                try:
                    result = conn.execute(sa.text(sql), params or {})
                    rows = [dict(r) for r in result.mappings().fetchmany(max_rows)] if result.returns_rows else []
                    count = result.rowcount
                    (trans.commit if write else trans.rollback)()
                except Exception:
                    trans.rollback()
                    raise
            return {"rows": rows, "rowcount": count}

        return await self._run(run)

    async def create_table(self, name: str, columns: list[dict], add_tracking: bool = True):
        import sqlalchemy as sa

        def run():
            md = sa.MetaData()
            cols = []
            has_pk = any(c.get("primary_key") for c in columns)
            if add_tracking and not has_pk:
                cols.append(sa.Column("id", sa.BigInteger().with_variant(sa.Integer, "sqlite"),
                                      primary_key=True, autoincrement=True))
            for c in columns:
                type_ = getattr(sa, SQL_TYPES.get(c.get("type", "text"), "String"))
                type_ = type_(255) if type_ is sa.String else type_()
                cols.append(sa.Column(c["name"], type_, primary_key=bool(c.get("primary_key")),
                                      nullable=bool(c.get("nullable", True)) or bool(c.get("primary_key"))))
            if add_tracking:
                cols += [sa.Column("transaction_id", sa.String(36)), sa.Column("partner_code", sa.String(80)),
                         sa.Column("stored_at", sa.DateTime, server_default=sa.func.current_timestamp())]
            sa.Table(name, md, *cols).create(self.engine())

        await self._run(run)

    # ---------- table designer ----------

    DESIGN_TYPES = ["varchar", "text", "integer", "bigint", "numeric", "boolean", "date", "timestamp", "json", "uuid"]

    def _column(self, c: dict):
        import re

        import sqlalchemy as sa

        name = str(c.get("name") or "").strip()
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]{0,62}", name):
            raise AdapterError(f"Invalid column name '{name}': letters, digits and _, starting with a letter.",
                               retryable=False)
        t = c.get("type") or "varchar"
        if t == "varchar":
            type_ = sa.String(int(c.get("length") or 255))
        elif t == "numeric":
            type_ = sa.Numeric(int(c.get("precision") or 18), int(c.get("scale") or 2))
        elif t == "timestamp":
            type_ = sa.DateTime(timezone=True)
        else:
            type_ = {"text": sa.Text, "integer": sa.Integer, "bigint": sa.BigInteger, "boolean": sa.Boolean,
                     "date": sa.Date, "json": sa.JSON, "uuid": sa.Uuid}.get(t)
            if type_ is None:
                raise AdapterError(f"Unknown type '{t}' for column '{name}'.", retryable=False)
            type_ = type_()
        kwargs = {"nullable": bool(c.get("nullable", True)) and not c.get("primary_key")}
        default = str(c.get("default") or "").strip()
        if default:
            low = default.lower()
            if low in ("now", "now()", "current_timestamp"):
                kwargs["server_default"] = sa.func.current_timestamp()
            elif low in ("true", "false"):
                kwargs["server_default"] = sa.true() if low == "true" else sa.false()
            elif re.fullmatch(r"-?\d+(\.\d+)?", default):
                kwargs["server_default"] = sa.text(default)
            else:
                kwargs["server_default"] = sa.text("'" + default.replace("'", "''") + "'")
        args = []
        if c.get("auto_increment"):
            if t not in ("integer", "bigint"):
                raise AdapterError(f"Auto-increment column '{name}' must be integer or bigint.", retryable=False)
            if not c.get("primary_key"):
                raise AdapterError(f"Auto-increment column '{name}' must be part of the primary key.", retryable=False)
            if self.config.get("engine") in ("oracle", "sqlserver", "postgresql"):
                args.append(sa.Identity())
            kwargs["autoincrement"] = True
            if self.config.get("engine") == "sqlite":
                type_ = sa.Integer()  # SQLite only auto-increments INTEGER PRIMARY KEY
        else:
            kwargs["autoincrement"] = False
        return sa.Column(name, type_, *args, **kwargs)

    def _table_from_spec(self, spec: dict):
        import re

        import sqlalchemy as sa

        name = str(spec.get("name") or "").strip()
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]{0,62}", name):
            raise AdapterError("Table name: letters, digits and _, starting with a letter.", retryable=False)
        columns = [c for c in spec.get("columns") or [] if str(c.get("name") or "").strip()]
        if not columns:
            raise AdapterError("Add at least one column.", retryable=False)
        names = [str(c["name"]).strip() for c in columns]
        dupes = {n for n in names if names.count(n) > 1}
        if dupes:
            raise AdapterError(f"Column(s) listed twice: {', '.join(sorted(dupes))}", retryable=False)
        md = sa.MetaData()
        cols = [self._column(c) for c in columns]
        pk = [str(c["name"]).strip() for c in columns if c.get("primary_key")]
        if spec.get("add_tracking", True):
            if not pk and "id" not in names:
                cols.insert(0, self._column({"name": "id", "type": "bigint", "primary_key": True,
                                             "auto_increment": True}))
                pk = ["id"]
            for extra, type_ in (("transaction_id", sa.String(36)), ("partner_code", sa.String(80))):
                if extra not in names:
                    cols.append(sa.Column(extra, type_))
            if "stored_at" not in names:
                cols.append(sa.Column("stored_at", sa.DateTime(timezone=True),
                                      server_default=sa.func.current_timestamp()))
        constraints = [sa.PrimaryKeyConstraint(*pk, name=f"{name}_pkey")] if pk else []
        for c in columns:
            if c.get("unique") and not c.get("primary_key"):
                constraints.append(sa.UniqueConstraint(str(c["name"]).strip(), name=f"uq_{name}_{c['name']}"[:63]))
        for col in cols:
            col.primary_key = False  # the explicit constraint below defines the key
        return sa.Table(name, md, *cols, *constraints)

    def ddl(self, spec: dict) -> str:
        import sqlalchemy as sa

        table = self._table_from_spec(spec)
        return str(sa.schema.CreateTable(table).compile(dialect=self.engine().dialect)).strip() + ";"

    async def create_designed(self, spec: dict):
        table = self._table_from_spec(spec)
        await self._run(lambda: table.create(self.engine()))

    async def schema(self, name: str) -> dict:
        import sqlalchemy as sa

        def run():
            insp = sa.inspect(self.engine())
            pk = insp.get_pk_constraint(name) or {}
            try:
                uniques = insp.get_unique_constraints(name)
            except NotImplementedError:
                uniques = []
            return {"name": name,
                    "columns": [{"name": c["name"], "type": str(c["type"]).lower(), "nullable": c.get("nullable", True),
                                 "default": c.get("default"),
                                 "primary_key": c["name"] in (pk.get("constrained_columns") or [])}
                                for c in insp.get_columns(name)],
                    "primary_key": {"name": pk.get("name"), "columns": pk.get("constrained_columns") or []},
                    "unique": [{"name": u.get("name"), "columns": u.get("column_names")} for u in uniques]}

        return await self._run(run)

    async def alter(self, name: str, action: str, body: dict) -> str:
        """add_column | drop_column | set_primary_key | drop_primary_key | add_unique | drop_unique"""
        import sqlalchemy as sa

        eng = self.config.get("engine")

        def run():
            dialect = self.engine().dialect
            q = dialect.identifier_preparer.quote
            insp = sa.inspect(self.engine())
            statements = []
            if action == "add_column":
                spec = dict(body.get("column") or {})
                if spec.get("primary_key") or spec.get("auto_increment"):
                    raise AdapterError("Add the column first, then set the primary key.", retryable=False)
                col = self._column(spec)
                coldef = str(sa.schema.CreateColumn(col).compile(dialect=dialect)).strip()
                verb = {"oracle": f"ADD ({coldef})", "sqlserver": f"ADD {coldef}"}.get(eng, f"ADD COLUMN {coldef}")
                statements.append(f"ALTER TABLE {q(name)} {verb}")
            elif action == "drop_column":
                statements.append(f"ALTER TABLE {q(name)} DROP COLUMN {q(body['column'])}")
            elif action in ("set_primary_key", "drop_primary_key"):
                if eng == "sqlite":
                    raise AdapterError("SQLite can't change the primary key of an existing table - create a new "
                                       "table with the key you want.", retryable=False)
                current = insp.get_pk_constraint(name) or {}
                if current.get("constrained_columns"):
                    statements.append(f"ALTER TABLE {q(name)} DROP PRIMARY KEY" if eng == "mysql" else
                                      f"ALTER TABLE {q(name)} DROP CONSTRAINT {q(current['name'])}")
                if action == "set_primary_key":
                    cols = body.get("columns") or []
                    if not cols:
                        raise AdapterError("Choose at least one column for the primary key.", retryable=False)
                    statements.append(f"ALTER TABLE {q(name)} ADD CONSTRAINT {q(name + '_pkey')} PRIMARY KEY "
                                      f"({', '.join(q(c) for c in cols)})")
            elif action == "add_unique":
                if eng == "sqlite":
                    raise AdapterError("SQLite can't add a unique constraint to an existing table.", retryable=False)
                cols = body.get("columns") or []
                if not cols:
                    raise AdapterError("Choose the column(s) that must be unique.", retryable=False)
                cname = f"uq_{name}_{'_'.join(cols)}"[:63]
                statements.append(f"ALTER TABLE {q(name)} ADD CONSTRAINT {q(cname)} UNIQUE "
                                  f"({', '.join(q(c) for c in cols)})")
            elif action == "drop_unique":
                statements.append(f"ALTER TABLE {q(name)} DROP INDEX {q(body['name'])}" if eng == "mysql" else
                                  f"ALTER TABLE {q(name)} DROP CONSTRAINT {q(body['name'])}")
            else:
                raise AdapterError(f"Unknown change '{action}'.", retryable=False)
            with self.engine().begin() as conn:
                for stmt in statements:
                    conn.execute(sa.text(stmt))
            return "; ".join(statements)

        return await self._run(run)

    async def drop_table(self, name: str):
        import sqlalchemy as sa

        await self._run(lambda: sa.Table(name, sa.MetaData(), autoload_with=self.engine()).drop(self.engine()))

    async def table_rows(self, name: str, limit: int = 100) -> dict:
        import sqlalchemy as sa

        def run():
            t = sa.Table(name, sa.MetaData(), autoload_with=self.engine())
            with self.engine().connect() as conn:
                order = list(t.primary_key.columns) or [list(t.columns)[0]]
                result = conn.execute(sa.select(t).order_by(*[c.desc() for c in order]).limit(limit))
                return {"columns": list(result.keys()), "rows": [dict(r) for r in result.mappings()]}

        return await self._run(run)

    async def insert_rows(self, table: str, rows: list[dict], txn_id: str | None = None,
                          partner_code: str | None = None) -> int:
        import sqlalchemy as sa

        def run():
            t = sa.Table(table, sa.MetaData(), autoload_with=self.engine())
            names = set(t.columns.keys())
            prepared = []
            for row in rows:
                values = dict(row)
                if "transaction_id" in names and txn_id and "transaction_id" not in values:
                    values["transaction_id"] = txn_id
                if "partner_code" in names and partner_code and "partner_code" not in values:
                    values["partner_code"] = partner_code
                unknown = set(values) - names
                if unknown:
                    raise AdapterError(f"Table '{table}' has no column(s): {', '.join(sorted(unknown))}",
                                       retryable=False)
                prepared.append({k: (json.dumps(v) if isinstance(v, (dict, list)) and
                                     not isinstance(t.c[k].type, sa.JSON) else v) for k, v in values.items()})
            if prepared:
                with self.engine().begin() as conn:
                    conn.execute(t.insert(), prepared)
            return len(prepared)

        return await self._run(run)

    # ---- the workflow DB connector: queries built from settings, never from typed SQL ----
    _OPS = {"=": "__eq__", "!=": "__ne__", ">": "__gt__", ">=": "__ge__", "<": "__lt__", "<=": "__le__"}

    def _structured(self, spec: dict):
        import sqlalchemy as sa

        try:
            t = sa.Table(spec["table"], sa.MetaData(), autoload_with=self.engine())
        except sa.exc.NoSuchTableError as e:
            raise AdapterError(f"{self._describe()} has no table '{spec['table']}'.", retryable=False) from e

        def col(name):
            if name not in t.c:
                raise AdapterError(f"Table '{spec['table']}' has no column '{name}'.", retryable=False)
            return t.c[name]

        conds = []
        for w in spec.get("where") or []:
            if not w.get("column"):
                continue
            c, op, v = col(w["column"]), (w.get("op") or "=").lower(), w.get("value")
            if op == "is null":
                conds.append(c.is_(None))
            elif op == "is not null":
                conds.append(c.is_not(None))
            elif op == "like":
                conds.append(c.like(v))
            elif op == "in":
                items = v if isinstance(v, list) else [x.strip() for x in str(v or "").split(",") if x.strip()]
                conds.append(c.in_(items))
            elif op in self._OPS:
                conds.append(getattr(c, self._OPS[op])(v))
            else:
                raise AdapterError(f"Unknown condition '{op}'.", retryable=False)
        op = spec.get("operation")
        if op in ("select_one", "select_all"):
            cols = [col(n) for n in spec.get("columns") or []] or [t]
            stmt = sa.select(*cols).where(*conds)
            if spec.get("order_by"):
                c = col(spec["order_by"])
                stmt = stmt.order_by(c.desc() if spec.get("order_desc") else c)
            limit = 1 if op == "select_one" else int(spec.get("limit") or 1000)
            return stmt.limit(min(limit, 10000))
        values = {k: (json.dumps(v) if isinstance(v, (dict, list)) and not isinstance(col(k).type, sa.JSON) else v)
                  for k, v in (spec.get("values") or {}).items()}
        for k in values:
            col(k)
        if op == "insert":
            if not values:
                raise AdapterError("Insert needs at least one column value.", retryable=False)
            return t.insert().values(**values)
        if op in ("update", "delete") and not conds and not spec.get("allow_all_rows"):
            raise AdapterError(f"Refusing to {op} every row - add a condition.", retryable=False)
        if op == "update":
            if not values:
                raise AdapterError("Update needs at least one column value.", retryable=False)
            return t.update().where(*conds).values(**values)
        if op == "delete":
            return t.delete().where(*conds)
        raise AdapterError(f"Unknown operation '{op}'.", retryable=False)

    async def preview_structured(self, spec: dict) -> str:
        """The SQL the connector would run, with :placeholders for the values."""
        return await self._run(lambda: str(self._structured(spec).compile(
            dialect=self.engine().dialect, compile_kwargs={"render_postcompile": True})).strip())

    async def run_structured(self, spec: dict) -> dict:
        def run():
            stmt = self._structured(spec)
            if spec.get("operation", "").startswith("select"):
                with self.engine().connect() as conn:
                    return {"rows": [dict(r) for r in conn.execute(stmt).mappings()], "rowcount": None}
            with self.engine().begin() as conn:
                result = conn.execute(stmt)
                if spec.get("operation") == "insert":
                    key = result.inserted_primary_key
                    return {"rows": [], "rowcount": 1, "key": _key(key[0]) if key and len(key) == 1
                            else [_key(k) for k in key or []]}
                return {"rows": [], "rowcount": result.rowcount}

        return await self._run(run)

    async def send(self, target, body, headers, content_type):
        """Outbound: insert the payload into table `target`; JSON fields go to columns of the same name."""
        if "json" not in content_type:
            raise AdapterError("Database delivery needs a JSON payload (set the partner's output format to JSON), "
                               "or use 'Store in the database' with a column mapping.", retryable=False)
        data = json.loads(body)
        rows = data if isinstance(data, list) else [data]
        if not all(isinstance(r, dict) for r in rows):
            raise AdapterError("Database delivery needs a JSON object (or a list of objects).", retryable=False)
        n = await self.insert_rows(target, rows, headers.get("X-Transaction-Id"), headers.get("X-Partner"))
        return f"inserted {n} row(s) into {self._describe()} table '{target}'"

    async def listen(self, source, handler):
        """Inbound: poll `table:key_column` for new rows; each row becomes a JSON message."""
        import sqlalchemy as sa

        table, _, key = source.partition(":")
        key = key.strip() or "id"
        table = table.strip()
        state_key = f"{self.config.get('engine')}|{self.config.get('host')}|{self.config.get('database')}|{source}"
        interval = max(2, int(self.config.get("poll_interval") or 10))

        def fetch(after):
            t = sa.Table(table, sa.MetaData(), autoload_with=self.engine())
            if key not in t.c:
                raise AdapterError(f"Table '{table}' has no key column '{key}'.", retryable=False)
            stmt = sa.select(t).order_by(t.c[key]).limit(100)
            if after is not None:
                stmt = stmt.where(t.c[key] > after)
            with self.engine().connect() as conn:
                return [dict(r) for r in conn.execute(stmt).mappings()]

        await self._run(lambda: fetch(None) and None)  # fail fast if the table/key is wrong
        stopped = asyncio.Event()

        async def run():
            from . import db

            last = await db.get_state(state_key)
            while not stopped.is_set():
                try:
                    rows = await self._run(lambda: fetch(_typed(last)))
                    for row in rows:
                        await handler(json.dumps(row, default=_json_value).encode(),
                                      {"headers": {}, "source": f"database:{table}", "format": "json"})
                        last = str(row[key])
                        await db.set_state(state_key, last)
                except Exception:  # noqa: BLE001
                    log.exception("database poll on %s failed", source)
                try:
                    await asyncio.wait_for(stopped.wait(), interval)
                except asyncio.TimeoutError:
                    pass

        task = asyncio.create_task(run(), name=f"db-poll-{source}")

        async def stop():
            stopped.set()
            await asyncio.gather(task, return_exceptions=True)

        listener = Listener(stop, f"database table '{table}' (new rows by {key}, every {interval} s)")
        listener.alive = lambda: not task.done()
        return listener

    async def close(self):
        if self._engine is not None:
            await asyncio.to_thread(self._engine.dispose)


def _key(v):
    return v if v is None or isinstance(v, (int, float, str)) else _json_value(v)


def _json_value(v):
    """JSON for database values: decimals stay numbers, dates and times become ISO text."""
    from decimal import Decimal

    if isinstance(v, Decimal):
        return int(v) if v == v.to_integral_value() else float(v)
    if hasattr(v, "isoformat"):
        return v.isoformat()
    return str(v)


def _typed(value):
    """Stored poll positions are text; compare numbers as numbers."""
    if value is None:
        return None
    try:
        return int(value)
    except ValueError:
        try:
            return float(value)
        except ValueError:
            return value


ADAPTERS = {cls.kind: cls for cls in (RabbitMQ, ActiveMQ, IBMMQ, HTTP, Kafka, Database, FileTransfer)}

for _kind, _spec in TYPES.items():
    if _spec["outbound"]:
        _spec["fields"] += RETRY_FIELDS + ([HTTP_RETRY_STATUS] if _kind == "http" else [])


def make(conn: dict) -> Adapter:
    """Build an adapter from a stored connection row (type, config, secret)."""
    return ADAPTERS[conn["type"]](conn["config"], conn.get("secret"))


def ibmmq_available() -> bool:
    try:
        _ibmmq()
        return True
    except AdapterError:
        return False
