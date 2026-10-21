"""Test settings. They are set before any app module is imported, because the app reads them at import time.

Integration tests use their own database (dropped and re-created each run) and their own Kafka topics and
consumer groups (a unique prefix), so they never touch a running Payload Hub's data or messages.
"""
import os
import uuid

from cryptography.fernet import Fernet

TEST_DATABASE_URL = os.getenv("PAYLOAD_HUB_TEST_DATABASE_URL", "postgresql://localhost/payloadhub_test")
os.environ["DATABASE_URL"] = TEST_DATABASE_URL
os.environ["KAFKA_TOPIC_PREFIX"] = f"pytest-{uuid.uuid4().hex[:8]}."
os.environ["KAFKA_TOPIC_PARTITIONS"] = "1"
os.environ["SCHEDULER_TICK_SECONDS"] = "1"
os.environ["ALERT_WATCH_SECONDS"] = "1"
os.environ["PAYLOAD_HUB_SUPERVISE_SECONDS"] = "2"
os.environ["PAYLOAD_HUB_METRICS_REFRESH_SECONDS"] = "2"
os.environ["PAYLOAD_HUB_BACKUP_DIR"] = __import__("tempfile").mkdtemp(prefix="payloadhub-backups-")
os.environ["PAYLOAD_HUB_SECRET_KEY"] = Fernet.generate_key().decode()
os.environ["ADMIN_PASSWORD"] = "test-admin-pass"
for key in ("ANTHROPIC_API_KEY", "OPENAI_API_KEY", "GEMINI_API_KEY", "GOOGLE_API_KEY", "GITHUB_TOKEN",
            "AZURE_OPENAI_API_KEY"):
    os.environ.pop(key, None)  # never call real AI providers from tests
