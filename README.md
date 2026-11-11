# DataLens messaging

RabbitMQ infrastructure for DataLens identity events. Identity owns the outbox
and publisher. The agent owns the consumer and its projection. Kafka carries
agent telemetry through the team's separate infrastructure.

## Local broker

Requirements are Docker Compose and Python 3.12. From this repository:

```powershell
python prepare.py
docker compose up -d --wait
docker compose ps
```

`prepare.py` generates two random passwords under ignored `secrets/` and
writes a hashed definitions file from `topology.json`. It preserves existing
password files. Keep that directory private and give each service only its own
password. Broker administration uses `docker compose exec rabbitmq rabbitmqctl`
through local Docker access. The broker has no remote administrator account
or management UI.

The official RabbitMQ 4.4.0 image is pinned by digest. Host clients use
`127.0.0.1:5672`. To change the host port, copy `.env.example` to `.env` and
set `RABBITMQ_PORT`. Containers on the broker network use `rabbitmq:5672`.
The vhost is `identity`. Use this plaintext AMQP endpoint for local development.
Hosted AMQPS and ingress controls belong to Plan 10.

| User | Access |
| --- | --- |
| `identity-publisher` | Publish routing keys starting with `user.` to `identity.events.v1` |
| `agent-identity-consumer` | Read and acknowledge deliveries from `agent.identity.v1` |

Provisioning owns topology. Clients can passively inspect their own resource.
Publish persistent messages using confirms and mandatory routing. Consume with
manual acknowledgements. Rejecting a delivery without requeue routes it via
`identity.dead.v1` to `agent.identity.dead.v1`. The consumer has no access to
that queue. The source quorum queue uses at-least-once dead-lettering, so a
consumer or operator must tolerate duplicate delivery.

## Persistence and recovery

The named `broker-data` volume retains messages and metadata across
`docker compose down` and container replacement. Both queues have one replica
on this local broker. To recover this instance, stop the broker and snapshot
its volume using your Docker volume tooling. Keep the snapshot and credential
files private. Restore the volume with the same node name and compatible
RabbitMQ version. Boot definitions can seed an empty instance. Message recovery
requires the data volume. Keep the running broker's passwords and definitions
in sync if rotating credentials through RabbitMQ operator tools.

```powershell
docker compose exec rabbitmq rabbitmqctl list_queues -p identity name messages_ready
```

[Definitions](https://www.rabbitmq.com/docs/definitions) ·
[Access control](https://www.rabbitmq.com/docs/access-control) ·
[Quorum queues](https://www.rabbitmq.com/docs/quorum-queues) ·
[Backup and restore](https://www.rabbitmq.com/docs/backup)

## Verification and delivery

```powershell
python -m venv .venv
.venv\Scripts\python.exe -m pip install -r requirements-test.txt
.venv\Scripts\python.exe tests/check_broker.py
```

The test creates its own credentials, Compose project, loopback port and volume.
It checks confirmed publishing, permission denials, redelivery, persistence
after container recreation and dead-letter routing. Cleanup removes that test
project and its volume. The local acceptance suite passed. The GitHub workflow exposes the required
`tests` job. Remote PR CI awaits the repository's base commit.

This is step **3.8.4.4.1**. After verified PR delivery, **3.8.4.4.2** adds the
publisher in `datalens-identity`. Step **3.8.4.5** verifies transport across
repositories. The parent plans live outside this repository.

Use feature branches and PRs to `main`. Once the GitHub repository has a base
commit on `main`, configure the required `tests` check. This local foundation
is on `fix/messaging-foundation`.
