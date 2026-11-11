"""Generate local RabbitMQ credentials and a boot definitions file."""

import argparse
import base64
import hashlib
import json
import os
import re
import secrets
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parent


def prepare(directory: Path) -> None:
    directory.mkdir(mode=0o700, parents=True, exist_ok=True)
    definitions = json.loads((ROOT / "topology.json").read_text(encoding="utf-8"))
    users = []
    passwords = set()
    for permission in definitions["permissions"]:
        name = permission["user"]
        path = directory / f"{name}.password"
        try:
            fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        except FileExistsError:
            pass
        else:
            with os.fdopen(fd, "w", encoding="ascii") as stream:
                stream.write(secrets.token_urlsafe(32) + "\n")
        password = path.read_text(encoding="ascii").strip()
        if not re.fullmatch(r"[A-Za-z0-9_-]{43}", password) or password in passwords:
            raise ValueError(f"Invalid or duplicate broker credential in {path.name}")
        passwords.add(password)
        salt = secrets.token_bytes(4)
        digest = hashlib.sha256(salt + password.encode("ascii")).digest()
        users.append(
            {
                "name": name,
                "password_hash": base64.b64encode(salt + digest).decode("ascii"),
                "hashing_algorithm": "rabbit_password_hashing_sha256",
                "tags": [],
            }
        )
    definitions["users"] = users
    with tempfile.NamedTemporaryFile(
        mode="w", encoding="utf-8", dir=directory, delete=False
    ) as stream:
        temporary = Path(stream.name)
        json.dump(definitions, stream, indent=2)
        stream.write("\n")
    try:
        # The non-root broker reads the hash file through its read-only bind mount.
        temporary.chmod(0o644)
        temporary.replace(directory / "definitions.json")
    finally:
        temporary.unlink(missing_ok=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--secrets-dir", type=Path, default=ROOT / "secrets")
    prepare(parser.parse_args().secrets_dir)
