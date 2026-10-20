"""Local FTP and SFTP servers for trying out Payload Hub's file adapter. For development only.

    .venv/bin/python scripts/dev_file_servers.py

    FTP   localhost:2121   user demo / demo123   folder data/ftp
    SFTP  localhost:2222   user demo / demo123   folder data/sftp

Both listen on 127.0.0.1 only. Needs the dev packages: pip install pyftpdlib asyncssh
"""
import asyncio
import threading
from pathlib import Path

import asyncssh
from pyftpdlib.authorizers import DummyAuthorizer
from pyftpdlib.handlers import FTPHandler
from pyftpdlib.servers import FTPServer

ROOT = Path(__file__).resolve().parent.parent / "data"
USER, PASSWORD = "demo", "demo123"  # demo-only credentials for these local test servers


def run_ftp():
    home = ROOT / "ftp"
    for folder in ("inbound", "outbound"):
        (home / folder).mkdir(parents=True, exist_ok=True)
    auth = DummyAuthorizer()
    auth.add_user(USER, PASSWORD, str(home), perm="elradfmwMT")
    handler = FTPHandler
    handler.authorizer = auth
    handler.passive_ports = range(60000, 60100)
    server = FTPServer(("127.0.0.1", 2121), handler)
    print(f"FTP  ready on 127.0.0.1:2121 → {home}")
    server.serve_forever()


class _Auth(asyncssh.SSHServer):
    def password_auth_supported(self):
        return True

    def validate_password(self, username, password):
        return username == USER and password == PASSWORD


async def run_sftp():
    home = ROOT / "sftp"
    for folder in ("inbound", "outbound"):
        (home / folder).mkdir(parents=True, exist_ok=True)
    key_file = ROOT / "dev_sftp_host_key"
    if not key_file.exists():
        asyncssh.generate_private_key("ssh-ed25519").write_private_key(str(key_file))
    await asyncssh.listen("127.0.0.1", 2222, server_factory=_Auth, server_host_keys=[str(key_file)],
                          sftp_factory=lambda chan: asyncssh.SFTPServer(chan, chroot=str(home)))
    print(f"SFTP ready on 127.0.0.1:2222 → {home}")
    await asyncio.Event().wait()


if __name__ == "__main__":
    threading.Thread(target=run_ftp, daemon=True).start()
    asyncio.run(run_sftp())
