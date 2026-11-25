#!/usr/bin/env python3
"""Campanha Multi-Transporte do Event Publishing Study (Colab CPU high-mem).

Executa em VM Colab dedicada (via `colab run --high-mem --timeout 1800`, com teardown automatico):
  - Provisiona 4 transportes reais no mesmo ambiente:
      1. PostgreSQL 16 (wal_level=logical, synchronous_commit=on) + pg-queue
      2. Apache Kafka 3.9.0 (KRaft single-node, acks=all, transactional.id fencing)
      3. RabbitMQ (durable queue + pipelined publisher confirms)
      4. Redis Streams (XADD/XREADGROUP/XACK + AOF always)
  - Executa 5 blocos empiricos com R=3 repeticoes pareadas (para IC t sobre log(razao), S2):
      C1: Fencing P6 completo (H-A no Kafka com transactional.id vs RabbitMQ sem fencing)
      C2: Fronteira Multi-Transporte (lambda in {25, 100, 300, 600, 1000} ev/s, carga representativa Zipf,
          estrategias outbox-poll(10/100ms), outbox-notify, outbox-cdc, pg-queue x Kafka/RabbitMQ/Redis)
      C3: Ablacao A5 — Contencao por agregado (hot_aggs=1 vs 200 com c=8 escritores -> Previsao P8)
      C4: Ablacao A1/A2 — Lote (b in {1, 50, 200}) x Polling (p in {10, 100, 500}ms) + microconstantes T_q/T_pub/T_mark
      C5: Aplicacao Hibrida Real — 4 fluxos concorrentes no MESMO PostgreSQL (medindo noisy-neighbor!)

Uso:
  colab run --high-mem --timeout 1800 experiments/colab_campaign/run_campaign.py \
      > analysis/colab_campaign/raw_stdout.txt 2> analysis/colab_campaign/raw_stderr.txt
"""

import json
import math
import os
import platform
import queue
import re
import select
import statistics
import struct
import subprocess
import sys
import threading
import time
import uuid
from collections import defaultdict


def log(msg):
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", file=sys.stderr, flush=True)


def sh(cmd, check=True, timeout=180):
    log(f"$ {cmd}")
    res = subprocess.run(cmd, shell=True, text=True, capture_output=True, timeout=timeout)
    if check and res.returncode != 0:
        log(f"STDOUT: {res.stdout[-1500:]}")
        log(f"STDERR: {res.stderr[-1500:]}")
        raise RuntimeError(f"Command failed ({res.returncode}): {cmd}")
    return res


def bootstrap_all():
    t0 = time.time()
    os.environ["DEBIAN_FRONTEND"] = "noninteractive"
    log("Installing PostgreSQL, RabbitMQ, Redis, OpenJDK 21, and Python clients...")
    sh(
        "apt-get update -qq && apt-get install -y -qq "
        "postgresql postgresql-contrib rabbitmq-server redis-server openjdk-21-jre-headless curl"
    )
    sh(f"{sys.executable} -m pip install -q psycopg2-binary pika redis confluent-kafka numpy scipy")

    # 1. PostgreSQL (logical replication + tuned connections)
    pg_ver = sh("ls /etc/postgresql | sort -V | tail -n 1").stdout.strip()
    pg_conf = f"/etc/postgresql/{pg_ver}/main/postgresql.conf"
    pg_hba = f"/etc/postgresql/{pg_ver}/main/pg_hba.conf"
    with open(pg_conf, "a") as f:
        f.write(
            "\n# event-publishing-study campaign\n"
            "wal_level = logical\n"
            "max_replication_slots = 12\n"
            "max_wal_senders = 12\n"
            "max_connections = 250\n"
            "synchronous_commit = on\n"
            "shared_buffers = 512MB\n"
            "work_mem = 16MB\n"
            "checkpoint_timeout = 5min\n"
        )
    with open(pg_hba, "r") as f:
        hba = f.read()
    prepend = (
        "local   all             all                                     trust\n"
        "host    all             all             127.0.0.1/32            trust\n"
        "local   replication     all                                     trust\n"
        "host    replication     all             127.0.0.1/32            trust\n"
    )
    with open(pg_hba, "w") as f:
        f.write(prepend + hba)
    sh("service postgresql restart")
    sh("su - postgres -c \"psql -c \\\"ALTER USER postgres WITH PASSWORD 'postgres' SUPERUSER REPLICATION;\\\"\"")
    sh("su - postgres -c \"psql -c \\\"CREATE DATABASE study;\\\"\"", check=False)

    # 2. RabbitMQ
    sh("service rabbitmq-server start")
    for _ in range(30):
        if sh("rabbitmqctl status", check=False).returncode == 0:
            break
        time.sleep(1)
    else:
        raise RuntimeError("RabbitMQ failed to start")

    # 3. Redis (AOF always for K2 local durability contract)
    sh("redis-server --daemonize yes --appendonly yes --appendfsync always --save '' --port 6379")
    for _ in range(15):
        if sh("redis-cli ping", check=False).stdout.strip() == "PONG":
            break
        time.sleep(0.5)
    else:
        raise RuntimeError("Redis failed to start")

    # 4. Apache Kafka 3.9.0 in KRaft single-node mode
    kafka_dir = "/opt/kafka"
    if not os.path.exists(kafka_dir):
        kafka_url = "https://archive.apache.org/dist/kafka/3.9.0/kafka_2.13-3.9.0.tgz"
        sh(f"curl -fsSL {kafka_url} -o /tmp/kafka.tgz && mkdir -p /opt/kafka && tar -xzf /tmp/kafka.tgz -C /opt/kafka --strip-components=1 && rm /tmp/kafka.tgz", timeout=240)

    kraft_props = f"{kafka_dir}/config/kraft/server.properties"
    with open(kraft_props, "r") as f:
        kp = f.read()
    # Ensure single-node transactional coordinator works (min ISR=1, RF=1 for single-node T1)
    extra_kafka = (
        "\n# study single-node transactional settings\n"
        "offsets.topic.replication.factor=1\n"
        "transaction.state.log.replication.factor=1\n"
        "transaction.state.log.min.isr=1\n"
        "num.partitions=12\n"
        "listeners=PLAINTEXT://127.0.0.1:9092,CONTROLLER://127.0.0.1:9093\n"
        "advertised.listeners=PLAINTEXT://127.0.0.1:9092\n"
    )
    # Remove conflicting listener/rf lines if present
    filtered = "\n".join(
        line for line in kp.splitlines()
        if not line.startswith((
            "listeners=", "advertised.listeners=",
            "offsets.topic.replication.factor=",
            "transaction.state.log.replication.factor=",
            "transaction.state.log.min.isr=",
            "num.partitions=",
        ))
    )
    with open(kraft_props, "w") as f:
        f.write(filtered + extra_kafka)

    cluster_id = sh(f"{kafka_dir}/bin/kafka-storage.sh random-uuid").stdout.strip().splitlines()[-1].strip()
    sh(f"{kafka_dir}/bin/kafka-storage.sh format -t {cluster_id} -c {kraft_props} --ignore-formatted")
    sh(f"KAFKA_HEAP_OPTS='-Xmx1G -Xms512M' {kafka_dir}/bin/kafka-server-start.sh -daemon {kraft_props}")

    # Wait for Kafka readiness
    for _ in range(40):
        r = sh(f"{kafka_dir}/bin/kafka-topics.sh --bootstrap-server 127.0.0.1:9092 --list", check=False)
        if r.returncode == 0:
            break
        time.sleep(1)
    else:
        log(sh(f"tail -n 50 {kafka_dir}/logs/server.log", check=False).stdout)
        raise RuntimeError("Kafka failed to start")

    mem_gb = round(os.sysconf("SC_PAGE_SIZE") * os.sysconf("SC_PHYS_PAGES") / (1024**3), 1)
    log(f"Bootstrap completed in {time.time() - t0:.1f}s (PG {pg_ver}, Kafka 3.9.0 KRaft, RabbitMQ, Redis AOF=always, CPUs={os.cpu_count()}, RAM={mem_gb} GiB)")
    return {"pg_ver": pg_ver, "cpus": os.cpu_count(), "ram_gib": mem_gb}


PG_DSN = "host=127.0.0.1 port=5432 dbname=study user=postgres password=postgres"
ENV_STRUCT = struct.Struct("!16sIQQ")


def pack_envelope(event_uuid: uuid.UUID, agg_id: int, agg_ver: int, t_int_us: int, payload: bytes) -> bytes:
    return ENV_STRUCT.pack(event_uuid.bytes, agg_id, agg_ver, t_int_us) + payload


def unpack_envelope(body: bytes):
    eb, agg_id, agg_ver, t_int_us = ENV_STRUCT.unpack_from(body, 0)
    return uuid.UUID(bytes=eb), agg_id, agg_ver, t_int_us


def setup_schema(n_customers=200):
    import psycopg2

    conn = psycopg2.connect(PG_DSN)
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute(
        """
        DROP TABLE IF EXISTS outbox CASCADE;
        DROP TABLE IF EXISTS orders CASCADE;
        DROP TABLE IF EXISTS customers CASCADE;
        DROP TABLE IF EXISTS outbox_relay_lease CASCADE;

        CREATE TABLE customers (
          id            INT PRIMARY KEY,
          balance_cents BIGINT NOT NULL,
          version       BIGINT NOT NULL DEFAULT 0,
          updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
        );

        CREATE TABLE orders (
          id                UUID PRIMARY KEY,
          customer_id       INT NOT NULL REFERENCES customers(id),
          aggregate_version BIGINT NOT NULL,
          status            TEXT NOT NULL,
          amount_cents      BIGINT NOT NULL,
          event_id          UUID NOT NULL,
          created_at        TIMESTAMPTZ NOT NULL DEFAULT now()
        );
        CREATE INDEX orders_customer_idx ON orders (customer_id);

        CREATE TABLE outbox (
          id                BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
          flow              TEXT NOT NULL DEFAULT 'default',
          event_id          UUID NOT NULL,
          aggregate_id      INT NOT NULL,
          aggregate_version BIGINT NOT NULL,
          partition_key     INT NOT NULL,
          t_int_us          BIGINT NOT NULL,
          payload           BYTEA NOT NULL,
          created_at        TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
          published_at      TIMESTAMPTZ
        );
        CREATE INDEX outbox_pending_idx ON outbox (id) WHERE published_at IS NULL;
        CREATE INDEX outbox_flow_pending_idx ON outbox (flow, id) WHERE published_at IS NULL;

        CREATE TABLE outbox_relay_lease (
          partition_key INT PRIMARY KEY,
          owner         TEXT NOT NULL,
          epoch         BIGINT NOT NULL,
          expires_at    TIMESTAMPTZ NOT NULL
        );
        """
    )
    cur.executemany(
        "INSERT INTO customers (id, balance_cents, version) VALUES (%s, 10000000, 0)",
        [(i,) for i in range(n_customers)],
    )
    cur.executemany(
        "INSERT INTO outbox_relay_lease (partition_key, owner, epoch, expires_at) VALUES (%s, 'none', 0, now() - interval '1 hour')",
        [(p,) for p in range(16)],
    )
    cur.close()
    conn.close()


def reset_pg(slot_name="study_cdc_slot"):
    import psycopg2

    conn = psycopg2.connect(PG_DSN)
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute("TRUNCATE TABLE outbox, orders RESTART IDENTITY;")
    cur.execute("UPDATE customers SET balance_cents = 10000000, version = 0;")
    cur.execute("UPDATE outbox_relay_lease SET owner = 'none', epoch = 0, expires_at = now() - interval '1 hour';")
    cur.execute("SELECT slot_name FROM pg_replication_slots WHERE slot_name = %s", (slot_name,))
    if cur.fetchone():
        cur.execute("SELECT pg_drop_replication_slot(%s)", (slot_name,))
    cur.close()
    conn.close()


# =============================================================================
# Transport Adapters (Kafka, RabbitMQ, Redis Streams)
# =============================================================================
def create_kafka_topic(topic: str, partitions: int = 12):
    from confluent_kafka.admin import AdminClient, NewTopic

    admin = AdminClient({"bootstrap.servers": "127.0.0.1:9092"})
    try:
        fs = admin.delete_topics([topic], operation_timeout=10)
        for f in fs.values():
            try:
                f.result(timeout=10)
            except Exception:
                pass
        time.sleep(0.4)
    except Exception:
        pass
    fs = admin.create_topics([NewTopic(topic, num_partitions=partitions, replication_factor=1)])
    for f in fs.values():
        try:
            f.result(timeout=10)
        except Exception:
            pass


def reset_rabbitmq_queue(queue_name="study_events"):
    import pika

    conn = pika.BlockingConnection(pika.ConnectionParameters("127.0.0.1"))
    ch = conn.channel()
    ch.queue_delete(queue=queue_name)
    ch.queue_declare(queue=queue_name, durable=True)
    conn.close()


def reset_redis_stream(stream_name="study_stream", group_name="study_grp"):
    import redis

    r = redis.Redis(host="127.0.0.1", port=6379)
    r.delete(stream_name)
    r.xgroup_create(stream_name, group_name, id="0", mkstream=True)
    r.close()


class BatchPublisher:
    """Unified batched publisher for kafka / rabbitmq / redis."""

    def __init__(self, transport: str, dest: str):
        self.transport = transport
        self.dest = dest
        if transport == "kafka":
            from confluent_kafka import Producer

            self.prod = Producer({
                "bootstrap.servers": "127.0.0.1:9092",
                "acks": "all",
                "linger.ms": 2,
                "batch.num.messages": 200,
                "enable.idempotence": True,
            })
        elif transport == "rabbitmq":
            import pika

            self.conn = pika.BlockingConnection(pika.ConnectionParameters("127.0.0.1"))
            self.ch = self.conn.channel()
            self.ch.confirm_delivery()
            self.props = pika.BasicProperties(delivery_mode=2)
        elif transport == "redis":
            import redis

            self.r = redis.Redis(host="127.0.0.1", port=6379)
        else:
            raise ValueError(transport)

    def publish_batch(self, items):
        """items: list of (agg_id: int, body: bytes). Blocks until durable ack for entire batch."""
        if not items:
            return
        if self.transport == "kafka":
            for agg_id, body in items:
                self.prod.produce(self.dest, key=str(agg_id).encode(), value=body)
            self.prod.flush(timeout=10.0)
        elif self.transport == "rabbitmq":
            # In pika BlockingConnection, confirm_delivery confirms each publish.
            # Keep it honest: real publisher confirms per message in batch.
            for _, body in items:
                self.ch.basic_publish("", self.dest, body, properties=self.props)
        elif self.transport == "redis":
            pipe = self.r.pipeline(transaction=False)
            for _, body in items:
                pipe.xadd(self.dest, {"b": body})
            pipe.execute()

    def close(self):
        if self.transport == "kafka":
            self.prod.flush(timeout=5.0)
        elif self.transport == "rabbitmq":
            self.conn.close()
        elif self.transport == "redis":
            self.r.close()


def start_consumer(transport: str, dest: str, stop_event, delivered_map, deliv_lock, l_e2e_ms):
    """Starts a consumer thread with C-proc (ack after recording) and returns the Thread."""
    if transport == "kafka":
        def run_kafka():
            from confluent_kafka import Consumer

            c = Consumer({
                "bootstrap.servers": "127.0.0.1:9092",
                "group.id": f"grp-{uuid.uuid4().hex[:8]}",
                "auto.offset.reset": "earliest",
                "enable.auto.commit": False,
                "fetch.wait.max.ms": 5,
            })
            c.subscribe([dest])
            while not stop_event.is_set():
                msgs = c.consume(num_messages=200, timeout=0.01)
                if not msgs:
                    continue
                t_recv_us = int(time.time() * 1e6)
                with deliv_lock:
                    for m in msgs:
                        if m is None or m.error():
                            continue
                        eid, agg_id, agg_ver, t_int_us = unpack_envelope(m.value())
                        is_first = len(delivered_map[eid]) == 0
                        delivered_map[eid].append((t_recv_us, agg_id, agg_ver))
                        if is_first:
                            l_e2e_ms.append((t_recv_us - t_int_us) / 1000.0)
                c.commit(asynchronous=True)
            c.close()

        t = threading.Thread(target=run_kafka, daemon=True)
        t.start()
        return t

    if transport == "rabbitmq":
        def run_rmq():
            import pika

            conn = pika.BlockingConnection(pika.ConnectionParameters("127.0.0.1"))
            ch = conn.channel()
            ch.basic_qos(prefetch_count=200)
            while not stop_event.is_set():
                got = 0
                last_tag = None
                while got < 100:
                    method, props, body = ch.basic_get(queue=dest, auto_ack=False)
                    if method is None:
                        break
                    t_recv_us = int(time.time() * 1e6)
                    eid, agg_id, agg_ver, t_int_us = unpack_envelope(body)
                    with deliv_lock:
                        is_first = len(delivered_map[eid]) == 0
                        delivered_map[eid].append((t_recv_us, agg_id, agg_ver))
                        if is_first:
                            l_e2e_ms.append((t_recv_us - t_int_us) / 1000.0)
                    last_tag = method.delivery_tag
                    got += 1
                if last_tag is not None:
                    ch.basic_ack(delivery_tag=last_tag, multiple=True)
                else:
                    time.sleep(0.002)
            conn.close()

        t = threading.Thread(target=run_rmq, daemon=True)
        t.start()
        return t

    if transport == "redis":
        def run_redis():
            import redis

            r = redis.Redis(host="127.0.0.1", port=6379)
            while not stop_event.is_set():
                res = r.xreadgroup("study_grp", "c1", {dest: ">"}, count=200, block=10)
                if not res:
                    continue
                t_recv_us = int(time.time() * 1e6)
                ack_ids = []
                with deliv_lock:
                    for _, entries in res:
                        for entry_id, fields in entries:
                            body = fields[b"b"]
                            eid, agg_id, agg_ver, t_int_us = unpack_envelope(body)
                            is_first = len(delivered_map[eid]) == 0
                            delivered_map[eid].append((t_recv_us, agg_id, agg_ver))
                            if is_first:
                                l_e2e_ms.append((t_recv_us - t_int_us) / 1000.0)
                            ack_ids.append(entry_id)
                if ack_ids:
                    r.xack(dest, "study_grp", *ack_ids)
            r.close()

        t = threading.Thread(target=run_redis, daemon=True)
        t.start()
        return t

    raise ValueError(transport)


# =============================================================================
# C1: Full P6 Fencing Verification (Kafka transactional.id vs RabbitMQ)
# =============================================================================
def run_c1_fencing_kafka_vs_rabbitmq():
    from confluent_kafka import Consumer, KafkaException, Producer
    import pika
    import psycopg2

    log("=== C1: Full P6 Fencing Verification (Kafka transactional.id vs RabbitMQ) ===")
    res = {}

    # --- Kafka WITH transactional.id fencing ---
    topic = "fencing_ha_topic"
    create_kafka_topic(topic, partitions=1)
    reset_pg()
    conn = psycopg2.connect(PG_DSN)
    cur = conn.cursor()
    e1, e2 = uuid.uuid4(), uuid.uuid4()
    t_us = int(time.time() * 1e6)
    cur.execute(
        "INSERT INTO outbox (event_id, aggregate_id, aggregate_version, partition_key, t_int_us, payload) "
        "VALUES (%s, 1, 1, 1, %s, %s), (%s, 1, 2, 1, %s, %s)",
        (str(e1), t_us, b"v1", str(e2), t_us, b"v2"),
    )
    cur.execute("UPDATE outbox_relay_lease SET owner='R1', epoch=1, expires_at=now() + interval '5s' WHERE partition_key=1")
    conn.commit()

    # R1 initializes transactional producer for partition 1 and begins tx
    p_r1 = Producer({"bootstrap.servers": "127.0.0.1:9092", "transactional.id": "relay-part-1", "acks": "all"})
    p_r1.init_transactions()
    p_r1.begin_transaction()
    cur.execute("SELECT id, event_id, aggregate_id, aggregate_version, t_int_us, payload FROM outbox WHERE partition_key=1 ORDER BY id LIMIT 1")
    r1_row = cur.fetchone()
    # R1 pauses...

    # Lease expires -> R2 takes over with SAME transactional.id -> fences R1!
    cur.execute("UPDATE outbox_relay_lease SET owner='R2', epoch=2, expires_at=now() + interval '30s' WHERE partition_key=1")
    cur.execute("SELECT id, event_id, aggregate_id, aggregate_version, t_int_us, payload FROM outbox WHERE partition_key=1 ORDER BY id")
    r2_rows = cur.fetchall()

    p_r2 = Producer({"bootstrap.servers": "127.0.0.1:9092", "transactional.id": "relay-part-1", "acks": "all"})
    p_r2.init_transactions()
    p_r2.begin_transaction()
    for row in r2_rows:
        body = pack_envelope(uuid.UUID(row[1]), row[2], row[3], row[4], bytes(row[5]))
        p_r2.produce(topic, key=b"1", value=body)
    p_r2.commit_transaction()
    cur.execute("UPDATE outbox SET published_at = clock_timestamp() WHERE id = ANY(%s)", ([r[0] for r in r2_rows],))
    conn.commit()

    # R1 wakes up (zombie) and tries to publish & commit v1!
    zombie_fenced_by_broker = False
    zombie_err = None
    try:
        body = pack_envelope(uuid.UUID(r1_row[1]), r1_row[2], r1_row[3], r1_row[4], bytes(r1_row[5]))
        p_r1.produce(topic, key=b"1", value=body)
        p_r1.commit_transaction()
    except KafkaException as ex:
        zombie_fenced_by_broker = True
        zombie_err = str(ex)

    # Read committed messages from Kafka
    c = Consumer({
        "bootstrap.servers": "127.0.0.1:9092",
        "group.id": f"fencing-check-{uuid.uuid4().hex[:6]}",
        "auto.offset.reset": "earliest",
        "isolation.level": "read_committed",
    })
    c.subscribe([topic])
    kafka_versions = []
    deadline = time.time() + 4.0
    while time.time() < deadline:
        m = c.poll(0.2)
        if m is not None and not m.error():
            _, _, v, _ = unpack_envelope(m.value())
            kafka_versions.append(v)
            if len(kafka_versions) >= 2:
                # wait a tiny bit to be sure no 3rd message arrives
                time.sleep(0.3)
                m2 = c.poll(0.2)
                if m2 is None:
                    break
    c.close()
    conn.close()

    res["kafka_two_layer_fencing"] = {
        "versions_delivered": kafka_versions,
        "zombie_fenced_by_broker": zombie_fenced_by_broker,
        "broker_error": zombie_err,
        "o1_inversions": sum(1 for i in range(1, len(kafka_versions)) if kafka_versions[i] < kafka_versions[i - 1]),
    }

    # --- RabbitMQ WITH DB epoch fencing only (no broker fencing available) ---
    reset_pg()
    reset_rabbitmq_queue("fencing_rmq")
    conn = psycopg2.connect(PG_DSN)
    cur = conn.cursor()
    e1, e2 = uuid.uuid4(), uuid.uuid4()
    cur.execute(
        "INSERT INTO outbox (event_id, aggregate_id, aggregate_version, partition_key, t_int_us, payload) "
        "VALUES (%s, 1, 1, 1, %s, %s), (%s, 1, 2, 1, %s, %s)",
        (str(e1), t_us, b"v1", str(e2), t_us, b"v2"),
    )
    cur.execute("UPDATE outbox_relay_lease SET owner='R1', epoch=1, expires_at=now() + interval '5s' WHERE partition_key=1")
    conn.commit()
    cur.execute("SELECT id, event_id, aggregate_id, aggregate_version, t_int_us, payload FROM outbox WHERE partition_key=1 ORDER BY id LIMIT 1")
    r1_row = cur.fetchone()

    cur.execute("UPDATE outbox_relay_lease SET owner='R2', epoch=2, expires_at=now() + interval '30s' WHERE partition_key=1")
    cur.execute("SELECT id, event_id, aggregate_id, aggregate_version, t_int_us, payload FROM outbox WHERE partition_key=1 ORDER BY id")
    r2_rows = cur.fetchall()
    rmq = pika.BlockingConnection(pika.ConnectionParameters("127.0.0.1"))
    ch = rmq.channel()
    ch.confirm_delivery()
    for row in r2_rows:
        ch.basic_publish("", "fencing_rmq", pack_envelope(uuid.UUID(row[1]), row[2], row[3], row[4], bytes(row[5])), properties=pika.BasicProperties(delivery_mode=2))
    cur.execute("UPDATE outbox SET published_at = clock_timestamp() WHERE id = ANY(%s)", ([r[0] for r in r2_rows],))
    conn.commit()

    # Zombie R1 publishes v1 (RabbitMQ accepts it!)
    ch.basic_publish("", "fencing_rmq", pack_envelope(uuid.UUID(r1_row[1]), r1_row[2], r1_row[3], r1_row[4], bytes(r1_row[5])), properties=pika.BasicProperties(delivery_mode=2))
    cur.execute(
        "UPDATE outbox SET published_at = clock_timestamp() WHERE id = %s "
        "AND EXISTS (SELECT 1 FROM outbox_relay_lease WHERE partition_key=1 AND owner='R1' AND epoch=1 AND expires_at > clock_timestamp())",
        (r1_row[0],),
    )
    db_mark_blocked = cur.rowcount == 0
    conn.commit()

    rmq_versions = []
    time.sleep(0.2)
    while True:
        m, _, body = ch.basic_get(queue="fencing_rmq", auto_ack=True)
        if m is None:
            break
        _, _, v, _ = unpack_envelope(body)
        rmq_versions.append(v)
    rmq.close()
    conn.close()

    res["rabbitmq_db_fencing_only"] = {
        "versions_delivered": rmq_versions,
        "db_mark_blocked": db_mark_blocked,
        "zombie_fenced_by_broker": False,
        "o1_inversions": sum(1 for i in range(1, len(rmq_versions)) if rmq_versions[i] < rmq_versions[i - 1]),
    }

    log(f"C1 done: {json.dumps(res)}")
    return res


# =============================================================================
# Core Open-Loop Runner across ALL 4 Transports
# =============================================================================
CDC_INSERT_RE = re.compile(
    r"table public\.outbox: INSERT: id\[bigint\]:\d+ "
    r"flow\[text\]:'([^']+)' "
    r"event_id\[uuid\]:'([^']+)' "
    r"aggregate_id\[integer\]:(\d+) "
    r"aggregate_version\[bigint\]:(\d+) "
    r"partition_key\[integer\]:\d+ "
    r"t_int_us\[bigint\]:(\d+) "
    r"payload\[bytea\]:'\\x([0-9a-fA-F]*)'"
)


def run_trial(
    transport: str,          # kafka | rabbitmq | redis | pg-queue
    strategy: str,           # outbox-poll | outbox-notify | outbox-cdc | pg-queue
    target_rate: float,
    duration_s: float = 6.0,
    poll_ms: float = 100.0,
    batch_size: int = 100,
    writers: int = 4,
    n_hot_customers: int = 200,
    seed: int = 42,
    measure_micro: bool = False,
):
    import numpy as np
    import psycopg2
    import psycopg2.extras

    reset_pg("study_cdc_slot")
    dest = f"t_{transport}_{uuid.uuid4().hex[:6]}"
    if transport == "kafka":
        create_kafka_topic(dest, partitions=12)
    elif transport == "rabbitmq":
        reset_rabbitmq_queue(dest)
    elif transport == "redis":
        reset_redis_stream(dest)

    if strategy == "outbox-cdc":
        c = psycopg2.connect(PG_DSN)
        c.autocommit = True
        cur = c.cursor()
        cur.execute("SELECT pg_create_logical_replication_slot('study_cdc_slot', 'test_decoding')")
        cur.close()
        c.close()

    stop_event = threading.Event()
    l_req_ms = []
    l_req_lock = threading.Lock()
    expected_events = {}
    exp_lock = threading.Lock()
    delivered_map = defaultdict(list)
    deliv_lock = threading.Lock()
    l_e2e_ms = []
    micro_tq, micro_tpub, micro_tmark = [], [], []

    # Consumer
    consumer_t = None
    if transport != "pg-queue":
        consumer_t = start_consumer(transport, dest, stop_event, delivered_map, deliv_lock, l_e2e_ms)

    # Relay / pg-queue worker
    relay_t = None
    if strategy == "pg-queue":
        def pgq():
            conn = psycopg2.connect(PG_DSN)
            conn.autocommit = False
            cur = conn.cursor()
            while not stop_event.is_set():
                cur.execute(
                    "WITH picked AS ("
                    "  SELECT id, event_id, aggregate_id, aggregate_version, t_int_us "
                    "  FROM outbox ORDER BY id LIMIT %s FOR UPDATE SKIP LOCKED"
                    ") DELETE FROM outbox USING picked WHERE outbox.id = picked.id "
                    "RETURNING picked.event_id, picked.aggregate_id, picked.aggregate_version, picked.t_int_us",
                    (batch_size,),
                )
                rows = cur.fetchall()
                conn.commit()
                if not rows:
                    time.sleep(0.005)
                    continue
                t_recv_us = int(time.time() * 1e6)
                with deliv_lock:
                    for r in rows:
                        eid = uuid.UUID(r[0])
                        is_first = len(delivered_map[eid]) == 0
                        delivered_map[eid].append((t_recv_us, r[1], r[2]))
                        if is_first:
                            l_e2e_ms.append((t_recv_us - r[3]) / 1000.0)
            conn.close()

        relay_t = threading.Thread(target=pgq, daemon=True)
        relay_t.start()

    elif strategy in ("outbox-poll", "outbox-notify"):
        def relay_poll():
            conn = psycopg2.connect(PG_DSN)
            conn.autocommit = False
            cur = conn.cursor()
            listen_conn = None
            if strategy == "outbox-notify":
                listen_conn = psycopg2.connect(PG_DSN)
                listen_conn.autocommit = True
                listen_conn.cursor().execute("LISTEN outbox_ch;")
            pub = BatchPublisher(transport, dest)
            sleep_s = poll_ms / 1000.0

            while not stop_event.is_set():
                t0 = time.perf_counter()
                cur.execute(
                    "SELECT id, event_id, aggregate_id, aggregate_version, t_int_us, payload "
                    "FROM outbox WHERE published_at IS NULL ORDER BY id LIMIT %s FOR UPDATE SKIP LOCKED",
                    (batch_size,),
                )
                rows = cur.fetchall()
                t1 = time.perf_counter()
                if not rows:
                    conn.commit()
                    if strategy == "outbox-notify":
                        if select.select([listen_conn], [], [], 0.2) != ([], [], []):
                            listen_conn.poll()
                            listen_conn.notifies.clear()
                    else:
                        time.sleep(sleep_s)
                    continue

                items = [
                    (r[2], pack_envelope(uuid.UUID(r[1]), r[2], r[3], r[4], bytes(r[5])))
                    for r in rows
                ]
                ids = [r[0] for r in rows]
                pub.publish_batch(items)
                t2 = time.perf_counter()
                cur.execute("UPDATE outbox SET published_at = clock_timestamp() WHERE id = ANY(%s)", (ids,))
                conn.commit()
                t3 = time.perf_counter()
                if measure_micro:
                    micro_tq.append((t1 - t0) * 1000.0)
                    micro_tpub.append((t2 - t1) * 1000.0)
                    micro_tmark.append((t3 - t2) * 1000.0)

            pub.close()
            if listen_conn:
                listen_conn.close()
            conn.close()

        relay_t = threading.Thread(target=relay_poll, daemon=True)
        relay_t.start()

    elif strategy == "outbox-cdc":
        def relay_cdc():
            rconn = psycopg2.connect(PG_DSN, connection_factory=psycopg2.extras.LogicalReplicationConnection)
            rcur = rconn.cursor()
            rcur.start_replication(slot_name="study_cdc_slot", decode=True)
            pub = BatchPublisher(transport, dest)

            while not stop_event.is_set():
                # Drain up to batch_size immediately available WAL messages
                batch_items = []
                last_msg = None
                while len(batch_items) < batch_size:
                    msg = rcur.read_message()
                    if msg is None:
                        break
                    last_msg = msg
                    m = CDC_INSERT_RE.match(msg.payload)
                    if m:
                        eid = uuid.UUID(m.group(2))
                        agg_id = int(m.group(3))
                        agg_ver = int(m.group(4))
                        t_int_us = int(m.group(5))
                        raw_p = bytes.fromhex(m.group(6))
                        batch_items.append((agg_id, pack_envelope(eid, agg_id, agg_ver, t_int_us, raw_p)))
                if batch_items:
                    pub.publish_batch(batch_items)
                if last_msg is not None:
                    last_msg.cursor.send_feedback(flush_lsn=last_msg.data_start)
                else:
                    select.select([rcur], [], [], 0.003)

            pub.close()
            rconn.close()

        relay_t = threading.Thread(target=relay_cdc, daemon=True)
        relay_t.start()

    time.sleep(0.3)

    # Zipf distribution over n_hot_customers
    rng = np.random.default_rng(seed)
    ranks = np.arange(1, n_hot_customers + 1, dtype=np.float64)
    probs = (1.0 / ranks)
    probs /= probs.sum()
    var_payloads = [os.urandom(sz) for sz in (512, 1024, 2048, 4096)]

    task_q = queue.Queue(maxsize=30000)
    errors = [0]

    def writer():
        conn = psycopg2.connect(PG_DSN)
        conn.autocommit = False
        cur = conn.cursor()
        while True:
            item = task_q.get()
            if item is None:
                break
            t_int_us, agg_id, payload = item
            eid = uuid.uuid4()
            oid = uuid.uuid4()
            try:
                cur.execute("SELECT balance_cents, version FROM customers WHERE id = %s FOR UPDATE", (agg_id,))
                bal, ver = cur.fetchone()
                nv = ver + 1
                cur.execute("UPDATE customers SET balance_cents = %s, version = %s WHERE id = %s", (bal - 1, nv, agg_id))
                cur.execute(
                    "INSERT INTO orders (id, customer_id, aggregate_version, status, amount_cents, event_id) VALUES (%s, %s, %s, 'OK', 1, %s)",
                    (str(oid), agg_id, nv, str(eid)),
                )
                cur.execute(
                    "INSERT INTO outbox (flow, event_id, aggregate_id, aggregate_version, partition_key, t_int_us, payload) "
                    "VALUES ('default', %s, %s, %s, %s, %s, %s)",
                    (str(eid), agg_id, nv, agg_id % 16, t_int_us, psycopg2.Binary(payload)),
                )
                if strategy == "outbox-notify":
                    cur.execute("NOTIFY outbox_ch, '1'")
                conn.commit()
                t_done_us = int(time.time() * 1e6)
                with l_req_lock:
                    l_req_ms.append((t_done_us - t_int_us) / 1000.0)
                with exp_lock:
                    expected_events[eid] = (agg_id, nv, t_int_us)
            except Exception:
                conn.rollback()
                errors[0] += 1
        conn.close()

    wthreads = [threading.Thread(target=writer, daemon=True) for _ in range(writers)]
    for wt in wthreads:
        wt.start()

    total_events = int(target_rate * duration_s)
    interval_s = 1.0 / target_rate
    t_start = time.perf_counter()
    wall_start_us = int(time.time() * 1e6)

    for i in range(total_events):
        sched = i * interval_s
        now = time.perf_counter() - t_start
        if sched > now:
            time.sleep(sched - now)
        t_int_us = wall_start_us + int(sched * 1e6)
        agg_id = int(rng.choice(n_hot_customers, p=probs))
        task_q.put((t_int_us, agg_id, var_payloads[i % len(var_payloads)]))

    for _ in wthreads:
        task_q.put(None)
    for wt in wthreads:
        wt.join(timeout=20)

    # Drain up to 5s
    drain_end = time.time() + 5.0
    while time.time() < drain_end:
        with deliv_lock:
            if len(delivered_map) >= len(expected_events):
                break
        time.sleep(0.05)
    time.sleep(0.15)

    stop_event.set()
    if relay_t:
        relay_t.join(timeout=5)
    if consumer_t:
        consumer_t.join(timeout=5)

    def pct(arr, q):
        return round(float(np.percentile(arr, q)), 3) if arr else None

    expected_n = len(expected_events)
    delivered_n = len(delivered_map)
    censored = max(0, expected_n - delivered_n)
    order_viol = 0
    by_agg = defaultdict(list)
    for eid, recs in delivered_map.items():
        by_agg[recs[0][1]].append((recs[0][0], recs[0][2]))
    for seq in by_agg.values():
        seq.sort(key=lambda x: x[0])
        for j in range(1, len(seq)):
            if seq[j][1] < seq[j - 1][1]:
                order_viol += 1

    out = {
        "transport": transport,
        "strategy": strategy,
        "rate": target_rate,
        "poll_ms": poll_ms if strategy == "outbox-poll" else None,
        "batch_size": batch_size,
        "writers": writers,
        "n_hot_customers": n_hot_customers,
        "expected": expected_n,
        "delivered": delivered_n,
        "censored": censored,
        "order_violations_O1": order_viol,
        "errors": errors[0],
        "l_req_p50_ms": pct(l_req_ms, 50),
        "l_req_p99_ms": pct(l_req_ms, 99),
        "l_e2e_p50_ms": pct(l_e2e_ms, 50),
        "l_e2e_p99_ms": pct(l_e2e_ms, 99),
    }
    if measure_micro and micro_tq:
        out["micro_constants_ms"] = {
            "T_q_median": round(statistics.median(micro_tq), 3),
            "T_pub_median": round(statistics.median(micro_tpub), 3),
            "T_mark_median": round(statistics.median(micro_tmark), 3),
            "batches": len(micro_tq),
        }
    return out


# =============================================================================
# C5: Hybrid Application Simulation (4 concurrent flows on SAME PostgreSQL!)
# =============================================================================
def run_c5_hybrid_app(duration_s=10.0):
    import numpy as np
    import psycopg2
    import psycopg2.extras

    log("=== C5: Hybrid Application (4 concurrent flows on SAME PostgreSQL) ===")
    reset_pg("hybrid_cdc_slot")
    create_kafka_topic("hybrid_billing", partitions=6)
    create_kafka_topic("hybrid_audit", partitions=6)
    reset_rabbitmq_queue("hybrid_webhooks")

    c = psycopg2.connect(PG_DSN)
    c.autocommit = True
    c.cursor().execute("SELECT pg_create_logical_replication_slot('hybrid_cdc_slot', 'test_decoding')")
    c.close()

    stop_event = threading.Event()
    flows = {
        "billing_cdc_kafka": {"rate": 100, "exp": {}, "deliv": defaultdict(list), "e2e": [], "req": []},
        "audit_poll_kafka": {"rate": 250, "exp": {}, "deliv": defaultdict(list), "e2e": [], "req": []},
        "webhooks_poll_rmq": {"rate": 40, "exp": {}, "deliv": defaultdict(list), "e2e": [], "req": []},
        "emails_pg_queue": {"rate": 30, "exp": {}, "deliv": defaultdict(list), "e2e": [], "req": []},
    }
    locks = {k: threading.Lock() for k in flows}

    # Start consumers
    ct1 = start_consumer("kafka", "hybrid_billing", stop_event, flows["billing_cdc_kafka"]["deliv"], locks["billing_cdc_kafka"], flows["billing_cdc_kafka"]["e2e"])
    ct2 = start_consumer("kafka", "hybrid_audit", stop_event, flows["audit_poll_kafka"]["deliv"], locks["audit_poll_kafka"], flows["audit_poll_kafka"]["e2e"])
    ct3 = start_consumer("rabbitmq", "hybrid_webhooks", stop_event, flows["webhooks_poll_rmq"]["deliv"], locks["webhooks_poll_rmq"], flows["webhooks_poll_rmq"]["e2e"])

    # Relay 1: CDC for billing_cdc_kafka
    def r_cdc():
        rconn = psycopg2.connect(PG_DSN, connection_factory=psycopg2.extras.LogicalReplicationConnection)
        rcur = rconn.cursor()
        rcur.start_replication(slot_name="hybrid_cdc_slot", decode=True)
        pub = BatchPublisher("kafka", "hybrid_billing")
        while not stop_event.is_set():
            batch = []
            last = None
            while len(batch) < 100:
                msg = rcur.read_message()
                if msg is None:
                    break
                last = msg
                m = CDC_INSERT_RE.match(msg.payload)
                if m and m.group(1) == "billing_cdc_kafka":
                    batch.append((int(m.group(3)), pack_envelope(uuid.UUID(m.group(2)), int(m.group(3)), int(m.group(4)), int(m.group(5)), bytes.fromhex(m.group(6)))))
            if batch:
                pub.publish_batch(batch)
            if last:
                last.cursor.send_feedback(flush_lsn=last.data_start)
            else:
                select.select([rcur], [], [], 0.003)
        pub.close()
        rconn.close()

    # Relay 2: Poll(100ms) for audit_poll_kafka
    def r_audit():
        conn = psycopg2.connect(PG_DSN)
        conn.autocommit = False
        cur = conn.cursor()
        pub = BatchPublisher("kafka", "hybrid_audit")
        while not stop_event.is_set():
            cur.execute("SELECT id, event_id, aggregate_id, aggregate_version, t_int_us, payload FROM outbox WHERE flow='audit_poll_kafka' AND published_at IS NULL ORDER BY id LIMIT 100 FOR UPDATE SKIP LOCKED")
            rows = cur.fetchall()
            if not rows:
                conn.commit()
                time.sleep(0.1)
                continue
            pub.publish_batch([(r[2], pack_envelope(uuid.UUID(r[1]), r[2], r[3], r[4], bytes(r[5]))) for r in rows])
            cur.execute("UPDATE outbox SET published_at=clock_timestamp() WHERE id=ANY(%s)", ([r[0] for r in rows],))
            conn.commit()
        pub.close()
        conn.close()

    # Relay 3: Poll(100ms) for webhooks_poll_rmq
    def r_wh():
        conn = psycopg2.connect(PG_DSN)
        conn.autocommit = False
        cur = conn.cursor()
        pub = BatchPublisher("rabbitmq", "hybrid_webhooks")
        while not stop_event.is_set():
            cur.execute("SELECT id, event_id, aggregate_id, aggregate_version, t_int_us, payload FROM outbox WHERE flow='webhooks_poll_rmq' AND published_at IS NULL ORDER BY id LIMIT 50 FOR UPDATE SKIP LOCKED")
            rows = cur.fetchall()
            if not rows:
                conn.commit()
                time.sleep(0.1)
                continue
            pub.publish_batch([(r[2], pack_envelope(uuid.UUID(r[1]), r[2], r[3], r[4], bytes(r[5]))) for r in rows])
            cur.execute("UPDATE outbox SET published_at=clock_timestamp() WHERE id=ANY(%s)", ([r[0] for r in rows],))
            conn.commit()
        pub.close()
        conn.close()

    # Relay 4: pg-queue for emails_pg_queue
    def r_email():
        conn = psycopg2.connect(PG_DSN)
        conn.autocommit = False
        cur = conn.cursor()
        while not stop_event.is_set():
            cur.execute(
                "WITH p AS (SELECT id, event_id, aggregate_id, aggregate_version, t_int_us FROM outbox WHERE flow='emails_pg_queue' ORDER BY id LIMIT 50 FOR UPDATE SKIP LOCKED) "
                "DELETE FROM outbox USING p WHERE outbox.id = p.id RETURNING p.event_id, p.aggregate_id, p.aggregate_version, p.t_int_us"
            )
            rows = cur.fetchall()
            conn.commit()
            if not rows:
                time.sleep(0.01)
                continue
            t_recv_us = int(time.time() * 1e6)
            with locks["emails_pg_queue"]:
                for r in rows:
                    eid = uuid.UUID(r[0])
                    is_first = len(flows["emails_pg_queue"]["deliv"][eid]) == 0
                    flows["emails_pg_queue"]["deliv"][eid].append((t_recv_us, r[1], r[2]))
                    if is_first:
                        flows["emails_pg_queue"]["e2e"].append((t_recv_us - r[3]) / 1000.0)
        conn.close()

    rthreads = [threading.Thread(target=fn, daemon=True) for fn in (r_cdc, r_audit, r_wh, r_email)]
    for t in rthreads:
        t.start()
    time.sleep(0.4)

    # Launch 4 concurrent open-loop producers
    def produce_flow(fname, rate):
        conn = psycopg2.connect(PG_DSN)
        conn.autocommit = False
        cur = conn.cursor()
        payload = os.urandom(512)
        n = int(rate * duration_s)
        dt = 1.0 / rate
        t0 = time.perf_counter()
        w0 = int(time.time() * 1e6)
        for i in range(n):
            sched = i * dt
            now = time.perf_counter() - t0
            if sched > now:
                time.sleep(sched - now)
            t_int_us = w0 + int(sched * 1e6)
            agg_id = i % 200
            eid = uuid.uuid4()
            cur.execute("SELECT balance_cents, version FROM customers WHERE id=%s FOR UPDATE", (agg_id,))
            bal, ver = cur.fetchone()
            cur.execute("UPDATE customers SET balance_cents=%s, version=%s WHERE id=%s", (bal - 1, ver + 1, agg_id))
            cur.execute(
                "INSERT INTO outbox (flow, event_id, aggregate_id, aggregate_version, partition_key, t_int_us, payload) "
                "VALUES (%s, %s, %s, %s, %s, %s, %s)",
                (fname, str(eid), agg_id, ver + 1, agg_id % 16, t_int_us, psycopg2.Binary(payload)),
            )
            conn.commit()
            t_done_us = int(time.time() * 1e6)
            with locks[fname]:
                flows[fname]["req"].append((t_done_us - t_int_us) / 1000.0)
                flows[fname]["exp"][eid] = (agg_id, ver + 1)
        conn.close()

    pthreads = [threading.Thread(target=produce_flow, args=(k, v["rate"]), daemon=True) for k, v in flows.items()]
    for t in pthreads:
        t.start()
    for t in pthreads:
        t.join(timeout=30)

    time.sleep(2.5)
    stop_event.set()
    for t in rthreads + [ct1, ct2, ct3]:
        t.join(timeout=5)

    summary = {}
    for k, v in flows.items():
        summary[k] = {
            "target_rate": v["rate"],
            "expected": len(v["exp"]),
            "delivered": len(v["deliv"]),
            "censored": max(0, len(v["exp"]) - len(v["deliv"])),
            "l_req_p50_ms": round(float(np.percentile(v["req"], 50)), 2) if v["req"] else None,
            "l_req_p99_ms": round(float(np.percentile(v["req"], 99)), 2) if v["req"] else None,
            "l_e2e_p50_ms": round(float(np.percentile(v["e2e"], 50)), 2) if v["e2e"] else None,
            "l_e2e_p99_ms": round(float(np.percentile(v["e2e"], 99)), 2) if v["e2e"] else None,
        }
        log(f"  C5 {k}: deliv={summary[k]['delivered']}/{summary[k]['expected']} L_e2e p50={summary[k]['l_e2e_p50_ms']}ms p99={summary[k]['l_e2e_p99_ms']}ms")
    return summary


def main():
    t_all = time.time()
    env_info = bootstrap_all()
    setup_schema(n_customers=200)

    results = {
        "meta": {
            **env_info,
            "platform": platform.platform(),
            "scope": "Colab high-mem CPU VM (4 real transports: Kafka 3.9 KRaft, RabbitMQ, Redis Streams AOF=always, PG16 pg-queue)",
            "not_verified_here": ["F2_real_page_cache", "T3_multi_host_quorum", "PG2_sync_standby", "cross_az_network"],
        }
    }

    # C1: Full P6 Fencing (Kafka vs RabbitMQ)
    results["C1_fencing_P6"] = run_c1_fencing_kafka_vs_rabbitmq()

    # C2: Multi-transport Frontier (R=3 blocks for paired t-CI!)
    log("=== C2: Multi-Transport Frontier (R=3 paired blocks) ===")
    c2_rows = []
    c2_configs = [
        ("kafka", "outbox-poll", 10),
        ("kafka", "outbox-poll", 100),
        ("kafka", "outbox-notify", 0),
        ("kafka", "outbox-cdc", 0),
        ("rabbitmq", "outbox-poll", 10),
        ("rabbitmq", "outbox-poll", 100),
        ("rabbitmq", "outbox-cdc", 0),
        ("redis", "outbox-poll", 10),
        ("redis", "outbox-poll", 100),
        ("redis", "outbox-cdc", 0),
        ("pg-queue", "pg-queue", 0),
    ]
    for block in range(1, 4):
        for rate in (25, 100, 300, 600):
            for transport, strat, p_ms in c2_configs:
                # Skip slow redundant combos at 600 for rabbitmq (already saturated at 300)
                if transport == "rabbitmq" and rate == 600 and p_ms == 100:
                    continue
                r = run_trial(
                    transport=transport,
                    strategy=strat,
                    target_rate=rate,
                    duration_s=5.0,
                    poll_ms=p_ms,
                    batch_size=100,
                    writers=4,
                    seed=42 + block,
                )
                r["block"] = block
                c2_rows.append(r)
                log(
                    f"  C2 b={block} rate={rate} {transport}/{strat}(p={p_ms}): "
                    f"L_e2e p50={r['l_e2e_p50_ms']}ms p99={r['l_e2e_p99_ms']}ms cens={r['censored']} O1={r['order_violations_O1']}"
                )
    results["C2_multi_transport_frontier"] = c2_rows

    # C3: Ablation A5 — Aggregate Contention (P8) on Kafka
    log("=== C3: Ablation A5 — Aggregate Contention (1 hot aggregate vs 200, c=8) ===")
    c3_rows = []
    for n_hot in (200, 1):
        for strat, p_ms in [("outbox-poll", 10), ("outbox-poll", 100), ("outbox-cdc", 0)]:
            r = run_trial(
                transport="kafka",
                strategy=strat,
                target_rate=200,
                duration_s=6.0,
                poll_ms=p_ms,
                batch_size=100,
                writers=8,
                n_hot_customers=n_hot,
                seed=99,
            )
            c3_rows.append(r)
            log(f"  C3 hot={n_hot} kafka/{strat}(p={p_ms}): L_req p99={r['l_req_p99_ms']}ms L_e2e p99={r['l_e2e_p99_ms']}ms cens={r['censored']}")
    results["C3_ablation_A5_contention"] = c3_rows

    # C4: Ablation A1/A2 — Batch x Poll + Microconstants for Mechanistic Model
    log("=== C4: Ablation A1/A2 — Batch x Poll + Microconstants ===")
    c4_rows = []
    for transport in ("kafka", "redis", "rabbitmq"):
        for b in (1, 50, 200):
            for p_ms in (10, 100, 500):
                if b == 1 and p_ms == 500:
                    continue
                r = run_trial(
                    transport=transport,
                    strategy="outbox-poll",
                    target_rate=100,
                    duration_s=5.0,
                    poll_ms=p_ms,
                    batch_size=b,
                    writers=4,
                    seed=77,
                    measure_micro=True,
                )
                c4_rows.append(r)
                mc = r.get("micro_constants_ms", {})
                log(f"  C4 {transport} b={b} p={p_ms}: L_e2e p99={r['l_e2e_p99_ms']}ms micro={mc}")
    results["C4_ablation_A1_A2_micro"] = c4_rows

    # C5: Hybrid Application (4 concurrent flows on SAME PG)
    results["C5_hybrid_app"] = run_c5_hybrid_app(duration_s=10.0)

    results["meta"]["total_runtime_s"] = round(time.time() - t_all, 1)
    print("===CAMPAIGN_JSON_START===")
    print(json.dumps(results, indent=2))
    print("===CAMPAIGN_JSON_END===")
    log(f"Full multi-transport campaign completed in {results['meta']['total_runtime_s']}s")


if __name__ == "__main__":
    main()
