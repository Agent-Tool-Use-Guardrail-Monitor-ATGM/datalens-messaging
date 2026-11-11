"""Run the broker acceptance checks in an isolated Compose project."""

import json
import os
import subprocess
import sys
import tempfile
import time
import uuid
from pathlib import Path

import pika

ROOT = Path(__file__).resolve().parents[1]
EXCHANGE = "identity.events.v1"
QUEUE = "agent.identity.v1"
DEAD_QUEUE = "agent.identity.dead.v1"
PUBLISHER = "identity-publisher"
CONSUMER = "agent-identity-consumer"


def denied(channel, operation):
    try:
        operation(channel)
    except pika.exceptions.ChannelClosedByBroker as failure:
        assert failure.reply_code == 403, failure
    else:
        raise AssertionError("Broker permitted a forbidden operation")


def publish(channel, body, exchange=EXCHANGE, key="user.created"):
    channel.confirm_delivery()
    channel.basic_publish(
        exchange, key, body, pika.BasicProperties(delivery_mode=2), mandatory=True
    )


def receive(channel, expected):
    deadline = time.monotonic() + 15
    while time.monotonic() < deadline:
        method, _, body = channel.basic_get(QUEUE, auto_ack=False)
        if method is not None:
            assert body == expected, body
            return method
        time.sleep(0.2)
    raise AssertionError("No message available on the identity queue")


def check(directory):
    subprocess.run(
        [sys.executable, "prepare.py", "--secrets-dir", str(directory)],
        cwd=ROOT,
        check=True,
    )
    password_files = {
        name: directory / f"{name}.password" for name in (PUBLISHER, CONSUMER)
    }
    passwords = {
        name: path.read_text(encoding="ascii").strip()
        for name, path in password_files.items()
    }
    subprocess.run(
        [sys.executable, "prepare.py", "--secrets-dir", str(directory)],
        cwd=ROOT,
        check=True,
    )
    assert all(
        path.read_text(encoding="ascii").strip() == passwords[name]
        for name, path in password_files.items()
    )
    assert all(
        password not in (directory / "definitions.json").read_text(encoding="utf-8")
        for password in passwords.values()
    )

    project = "datalens-messaging-test-" + uuid.uuid4().hex[:12]
    env = os.environ | {
        "RABBITMQ_PORT": "0",
        "RABBITMQ_DEFINITIONS_FILE": str(directory / "definitions.json"),
    }

    def compose(*args, capture=False, check=True):
        return subprocess.run(
            ["docker", "compose", "-f", "compose.yaml", "-p", project, *args],
            cwd=ROOT,
            env=env,
            check=check,
            text=True,
            capture_output=capture,
            timeout=220,
        ).stdout

    def connect(name, vhost="identity"):
        address = compose("port", "rabbitmq", "5672", capture=True).strip()
        host, port = address.rsplit(":", 1)
        assert host == "127.0.0.1", address
        return pika.BlockingConnection(
            pika.ConnectionParameters(
                host,
                int(port),
                vhost,
                pika.PlainCredentials(name, passwords.get(name, "guest")),
                socket_timeout=5,
                stack_timeout=10,
                blocked_connection_timeout=5,
            )
        )

    try:
        compose("up", "-d", "--wait", "--wait-timeout", "180")
        for name, vhost, error in (
            ("guest", "identity", pika.exceptions.ProbableAuthenticationError),
            (PUBLISHER, "/", pika.exceptions.ProbableAccessDeniedError),
        ):
            try:
                with connect(name, vhost):
                    raise AssertionError(
                        "Broker admitted credentials outside their scope"
                    )
            except error:
                pass

        with connect(PUBLISHER) as publisher, connect(CONSUMER) as consumer:
            publisher.channel().exchange_declare(EXCHANGE, passive=True)
            consumer.channel().queue_declare(QUEUE, passive=True)
            denied(publisher.channel(), lambda channel: channel.basic_get(QUEUE))
            denied(
                publisher.channel(),
                lambda channel: publish(channel, b"denied", key="agent.test"),
            )
            denied(
                publisher.channel(),
                lambda channel: publish(channel, b"denied", exchange=""),
            )
            denied(consumer.channel(), lambda channel: channel.basic_get(DEAD_QUEUE))
            denied(
                consumer.channel(),
                lambda channel: channel.queue_declare("unauthorized"),
            )
            denied(consumer.channel(), lambda channel: publish(channel, b"denied"))
            publish(publisher.channel(), b"durable fixture")
        print("Scoped credentials, routing and publisher confirms passed", flush=True)

        compose("up", "-d", "--force-recreate", "--wait", "--wait-timeout", "180")
        with connect(CONSUMER) as consumer:
            channel = consumer.channel()
            delivery = receive(channel, b"durable fixture")
            channel.basic_nack(delivery.delivery_tag, requeue=True)
            delivery = receive(channel, b"durable fixture")
            assert delivery.redelivered
            channel.basic_ack(delivery.delivery_tag)
        print(
            "Persistent delivery after recreation and manual redelivery passed",
            flush=True,
        )

        with connect(PUBLISHER) as publisher, connect(CONSUMER) as consumer:
            publish(publisher.channel(), b"dead-letter fixture")
            channel = consumer.channel()
            delivery = receive(channel, b"dead-letter fixture")
            channel.basic_reject(delivery.delivery_tag, requeue=False)
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            queues = json.loads(
                compose(
                    "exec",
                    "-T",
                    "rabbitmq",
                    "rabbitmqctl",
                    "-q",
                    "list_queues",
                    "-p",
                    "identity",
                    "name",
                    "messages_ready",
                    "--formatter=json",
                    capture=True,
                )
            )
            if any(
                q["name"] == DEAD_QUEUE and q["messages_ready"] == 1 for q in queues
            ):
                print("Dead-letter routing and consumer isolation passed", flush=True)
                break
            time.sleep(0.5)
        else:
            raise AssertionError("Rejected message did not reach the dead-letter queue")
    finally:
        active_error = sys.exc_info()[0] is not None
        if active_error:
            compose("logs", "--tail", "80", check=False)
        compose("down", "--volumes", check=not active_error)


if __name__ == "__main__":
    with tempfile.TemporaryDirectory(prefix="datalens-broker-") as folder:
        check(Path(folder))
