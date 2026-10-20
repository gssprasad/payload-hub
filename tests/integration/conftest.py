"""Runs the real app (uvicorn on a free port) against a fresh test database and isolated Kafka topics."""
import os
import socket
import sqlite3
import threading
import time
from urllib.parse import urlparse

import httpx
import psycopg
import pytest

from tests.conftest import TEST_DATABASE_URL

ADMIN = ("Administrator", "test-admin-pass")


def _reachable(host: str, port: int) -> bool:
    try:
        with socket.create_connection((host, port), timeout=2):
            return True
    except OSError:
        return False


def _require(ok: bool, what: str):
    if not ok:
        if os.getenv("CI"):
            pytest.fail(f"{what} is not reachable - the CI job must provide it")
        pytest.skip(f"{what} is not running - integration tests skipped")


def _recreate_database():
    url = urlparse(TEST_DATABASE_URL)
    name = url.path.lstrip("/")
    assert "test" in name, "refusing to drop a database whose name doesn't contain 'test'"
    with psycopg.connect(TEST_DATABASE_URL.replace(f"/{name}", "/postgres", 1), autocommit=True) as conn:
        conn.execute(f'DROP DATABASE IF EXISTS "{name}" WITH (FORCE)')
        conn.execute(f'CREATE DATABASE "{name}"')


def _drop_test_topics():
    import asyncio

    from aiokafka.admin import AIOKafkaAdminClient

    from app import pipeline

    async def run():
        admin = AIOKafkaAdminClient(**pipeline._connection())
        await admin.start()
        try:
            mine = [t for t in await admin.list_topics() if t.startswith(pipeline.PREFIX)]
            if mine:
                await admin.delete_topics(mine)
        finally:
            await admin.close()

    try:
        asyncio.run(run())
    except Exception:  # noqa: BLE001 - leftover test topics are harmless
        pass


@pytest.fixture(scope="session")
def base_url():
    db_url = urlparse(TEST_DATABASE_URL)
    _require(_reachable(db_url.hostname or "localhost", db_url.port or 5432), "PostgreSQL")
    kafka_host, _, kafka_port = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092").split(",")[0].rpartition(":")
    _require(_reachable(kafka_host or "localhost", int(kafka_port)), "Kafka")
    _recreate_database()

    import uvicorn

    from app.main import app

    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        port = s.getsockname()[1]
    server = uvicorn.Server(uvicorn.Config(app, host="127.0.0.1", port=port, log_level="warning"))
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()
    deadline = time.monotonic() + 60
    while not server.started:
        if not thread.is_alive() or time.monotonic() > deadline:
            pytest.fail("the app didn't start - see the log above")
        time.sleep(0.1)
    yield f"http://127.0.0.1:{port}"
    server.should_exit = True
    thread.join(timeout=30)
    _drop_test_topics()


def login(base_url, username, password) -> httpx.Client:
    r = httpx.post(f"{base_url}/api/auth/login", json={"username": username, "password": password})
    assert r.status_code == 200, r.text
    return httpx.Client(base_url=base_url, timeout=60, headers={"Authorization": f"Bearer {r.json()['token']}"})


@pytest.fixture(scope="session")
def admin(base_url):
    with login(base_url, *ADMIN) as client:
        yield client


@pytest.fixture(scope="session")
def shop_db(admin, tmp_path_factory):
    """A SQLite 'target system' database registered as a connection: customers, plus an empty orders_in table."""
    path = tmp_path_factory.mktemp("db") / "shop.db"
    con = sqlite3.connect(path)
    con.executescript("""
        CREATE TABLE customers (id TEXT PRIMARY KEY, name TEXT, country TEXT);
        INSERT INTO customers VALUES ('C1', 'Asha', 'IN'), ('C2', 'Ben', 'GB');
        CREATE TABLE orders_in (id INTEGER PRIMARY KEY AUTOINCREMENT, order_id TEXT, total REAL);
        CREATE TABLE followups (id INTEGER PRIMARY KEY AUTOINCREMENT, customer_id TEXT, note TEXT);
    """)
    con.commit()
    con.close()
    r = admin.post("/api/connections", json={"name": "Test shop", "type": "database",
                                             "config": {"engine": "sqlite", "database": str(path)}})
    assert r.status_code == 201, r.text
    return {"id": r.json()["id"], "path": path}


@pytest.fixture(scope="session")
def receiver(admin, base_url):
    """An HTTP 'target system': the app's own test receiver."""
    r = admin.post("/api/connections", json={"name": "Test receiver API", "type": "http",
                                             "config": {"base_url": f"{base_url}/test-receiver", "method": "POST",
                                                        "retry_attempts": 1}})
    assert r.status_code == 201, r.text
    return r.json()["id"]


@pytest.fixture(scope="session")
def mock_api(admin, base_url):
    r = admin.post("/api/connections", json={"name": "Test mock API", "type": "http",
                                             "config": {"base_url": f"{base_url}/mock", "retry_attempts": 1}})
    assert r.status_code == 201, r.text
    return r.json()["id"]


def query(path, sql, *args):
    con = sqlite3.connect(path)
    try:
        return con.execute(sql, args).fetchall()
    finally:
        con.close()


def wait_for(fetch, done, timeout=20):
    """Poll fetch() until done(result) - for steps that finish just after the caller gets its answer."""
    deadline = time.monotonic() + timeout
    while True:
        result = fetch()
        if done(result) or time.monotonic() > deadline:
            return result
        time.sleep(0.2)
