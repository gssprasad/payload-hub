"""Build and test the Streamlit Cloud secrets for Payload Hub.

Run it in your own terminal from the project folder:

    .venv/bin/python scripts/setup_cloud_secrets.py

It asks for your Neon (PostgreSQL) and Aiven (Kafka) connection details - passwords are hidden
while you type - reads Aiven's CA certificate file, checks that both services are reachable,
writes .streamlit/secrets.toml (git-ignored, readable only by you) and can copy it to the
clipboard so you can paste it into Streamlit Cloud: Manage app > Settings > Secrets.
"""
import asyncio
import getpass
import os
import secrets
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
SECRETS_FILE = ROOT / ".streamlit" / "secrets.toml"
sys.path.insert(0, str(ROOT))


def ask(prompt: str, default: str | None = None, hidden: bool = False, required: bool = True) -> str:
    suffix = f" [{default}]" if default else ""
    while True:
        value = (getpass.getpass if hidden else input)(f"{prompt}{suffix}: ").strip()
        if not value and default is not None:
            return default
        if value or not required:
            return value
        print("  This value is required.")


def find_ca_cert() -> str:
    default = Path.home() / "Downloads" / "ca.pem"
    while True:
        path = Path(ask("Path to Aiven's CA certificate (ca.pem)", str(default))).expanduser()
        if path.is_file():
            text = path.read_text().strip()
            if "BEGIN CERTIFICATE" in text:
                return text
            print(f"  {path} doesn't look like a certificate (no 'BEGIN CERTIFICATE').")
        else:
            print(f"  File not found: {path}")


def check_database(url: str) -> str | None:
    import psycopg

    try:
        with psycopg.connect(url, connect_timeout=10) as conn:
            conn.execute("SELECT 1")
    except Exception as e:  # noqa: BLE001
        return str(e).strip()
    return None


def check_kafka(env: dict) -> str | None:
    os.environ.update(env)
    from app import pipeline  # first import, so it reads the settings just entered

    async def probe():
        from aiokafka.admin import AIOKafkaAdminClient

        admin = AIOKafkaAdminClient(**pipeline._connection(), request_timeout_ms=15000)
        await admin.start()
        try:
            await admin.list_topics()
        finally:
            await admin.close()

    try:
        asyncio.run(asyncio.wait_for(probe(), 30))
    except Exception as e:  # noqa: BLE001
        return str(e) or type(e).__name__
    return None


def toml_string(value: str) -> str:
    if "\n" in value:
        return '"""\n' + value + '\n"""'
    return '"' + value.replace("\\", "\\\\").replace('"', '\\"') + '"'


def main():
    print(__doc__.split("\n\n")[0])
    print("\n1) PostgreSQL - Neon dashboard > your project > Connect > copy the connection string")
    database_url = ask("DATABASE_URL (postgresql://...)", hidden=True)
    if "sslmode=" not in database_url:
        database_url += ("&" if "?" in database_url else "?") + "sslmode=require"

    print("\n2) Kafka - Aiven console > your Kafka service > Overview > Connection information > SASL")
    bootstrap = ask("Kafka Service URI (host:port)")
    username = ask("Kafka User", "avnadmin")
    password = ask("Kafka Password", hidden=True)
    ca_cert = find_ca_cert()

    print("\n3) App")
    app_password = ask("Password for the demo app (Enter = generate one)", hidden=True, required=False)
    generated = not app_password
    app_password = app_password or secrets.token_urlsafe(9)
    anthropic_key = ask("Anthropic API key for the AI agent (Enter = skip)", hidden=True, required=False)

    env = {
        "EMBED_BACKEND": "true",
        "APP_PASSWORD": app_password,
        "DATABASE_URL": database_url,
        "KAFKA_BOOTSTRAP": bootstrap,
        "KAFKA_SECURITY_PROTOCOL": "SASL_SSL",
        "KAFKA_SASL_MECHANISM": "SCRAM-SHA-256",
        "KAFKA_USERNAME": username,
        "KAFKA_PASSWORD": password,
        "KAFKA_CA_CERT": ca_cert,
    }
    if anthropic_key:
        env["ANTHROPIC_API_KEY"] = anthropic_key

    print("\nChecking connections from this laptop…")
    db_error = check_database(database_url)
    print("  PostgreSQL:", "OK" if not db_error else f"FAILED - {db_error}")
    kafka_error = check_kafka(env)
    print("  Kafka:     ", "OK" if not kafka_error else f"FAILED - {kafka_error}")

    SECRETS_FILE.parent.mkdir(exist_ok=True)
    SECRETS_FILE.write_text("".join(f"{k} = {toml_string(v)}\n" for k, v in env.items()))
    SECRETS_FILE.chmod(0o600)
    print(f"\nSaved {SECRETS_FILE.relative_to(ROOT)} (not committed to git).")
    if generated:
        print(f"Demo app password (generated): {app_password}")

    if db_error or kafka_error:
        print("\nFix the failed connection and run this again before deploying. Common causes:")
        print("  - Kafka: the service isn't 'Running' yet, SASL isn't enabled, or the port is the client-certificate one")
        print("  - PostgreSQL: the connection string was copied incompletely")
        sys.exit(1)

    if sys.platform == "darwin" and input("\nCopy the secrets to the clipboard for Streamlit Cloud? [Y/n] ").strip().lower() != "n":
        subprocess.run(["pbcopy"], input=SECRETS_FILE.read_text(), text=True, check=True)
        print("Copied. In Streamlit Cloud: Manage app > ⋮ > Settings > Secrets > paste > Save.")
    else:
        print(f"Paste the contents of {SECRETS_FILE.relative_to(ROOT)} into Streamlit Cloud: Settings > Secrets.")


if __name__ == "__main__":
    main()
