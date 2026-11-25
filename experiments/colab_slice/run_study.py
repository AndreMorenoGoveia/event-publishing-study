#!/usr/bin/env python3
"""Fatia vertical enxuta do Event Publishing Study para execucao economica via Colab CLI.

Executa em VM somente CPU do Colab (via `colab run`, com teardown automatico ao terminar):
  1. Provisiona PostgreSQL real (wal_level=logical) + RabbitMQ real (durable + confirms).
  2. Executa A-F1: Historias minimas deterministicas (H-A sem/com fencing, H-B, H-C, H-D, H-J).
  3. Executa A-S1: Perfil Empresa Pequena (W0: 10 e 50 ev/s) — tx-only, dw-after, pg-queue,
     outbox-poll (100ms/1000ms), outbox-notify, outbox-cdc.
  4. Executa A-E1: Perfil Grande Empresa (25, 100, 300, 600 ev/s, Zipf + payload variavel, c=4)
     — fronteira polling (10ms/100ms) x NOTIFY x CDC x pg-queue.
  5. Executa A-E2: Contencao do LISTEN/NOTIFY sob concorrencia (c in {4, 16, 32}) — previsao P3.

Uso:
  colab run --timeout 900 experiments/colab_slice/run_study.py > analysis/colab_slice/raw_output.txt
"""

import json
import math
import os
import platform
import queue
import random
import re
import struct
import subprocess
import sys
import threading
import time
import uuid
from collections import defaultdict


def log(msg):
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", file=sys.stderr, flush=True)


def sh(cmd, check=True):
    log(f"$ {cmd}")
    res = subprocess.run(cmd, shell=True, text=True, capture_output=True)
    if check and res.returncode != 0:
        log(f"STDOUT: {res.stdout[-1500:]}")
        log(f"STDERR: {res.stderr[-1500:]}")
        raise RuntimeError(f"Command failed ({res.returncode}): {cmd}")
    return res


def bootstrap_environment():
    t0 = time.time()
    log("Installing system packages (postgresql, rabbitmq-server) and Python deps...")
    os.environ["DEBIAN_FRONTEND"] = "noninteractive"
    sh("apt-get update -qq && apt-get install -y -qq postgresql postgresql-contrib rabbitmq-server")
    sh(f"{sys.executable} -m pip install -q psycopg2-binary pika numpy")

    # Configure PostgreSQL for logical replication + high connections
    pg_ver = sh("ls /etc/postgresql | sort -V | tail -n 1").stdout.strip()
    pg_conf = f"/etc/postgresql/{pg_ver}/main/postgresql.conf"
    pg_hba = f"/etc/postgresql/{pg_ver}/main/pg_hba.conf"

    with open(pg_conf, "a") as f:
        f.write(
            "\n# event-publishing-study overrides\n"
            "wal_level = logical\n"
            "max_replication_slots = 8\n"
            "max_wal_senders = 8\n"
            "max_connections = 200\n"
            "synchronous_commit = on\n"
            "shared_buffers = 256MB\n"
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

    # Start RabbitMQ
    sh("service rabbitmq-server start")
    for _ in range(30):
        r = sh("rabbitmqctl status", check=False)
        if r.returncode == 0:
            break
        time.sleep(1)
    else:
        raise RuntimeError("RabbitMQ failed to start")

    log(f"Bootstrap completed in {time.time() - t0:.1f}s (PG {pg_ver}, CPUs={os.cpu_count()})")
    return pg_ver


PG_DSN = "host=127.0.0.1 port=5432 dbname=study user=postgres password=postgres"


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
        CREATE INDEX outbox_part_pending_idx ON outbox (partition_key, id) WHERE published_at IS NULL;

        CREATE TABLE outbox_relay_lease (
          partition_key INT PRIMARY KEY,
          owner         TEXT NOT NULL,
          epoch         BIGINT NOT NULL,
          expires_at    TIMESTAMPTZ NOT NULL
        );
        """
    )
    # Seed customers
    cur.executemany(
        "INSERT INTO customers (id, balance_cents, version) VALUES (%s, 1000000, 0)",
        [(i,) for i in range(n_customers)],
    )
    # Seed leases for 16 partitions
    cur.executemany(
        "INSERT INTO outbox_relay_lease (partition_key, owner, epoch, expires_at) VALUES (%s, 'none', 0, now() - interval '1 hour')",
        [(p,) for p in range(16)],
    )
    # Drop any old logical replication slot
    cur.execute("SELECT slot_name FROM pg_replication_slots WHERE slot_name = 'study_cdc_slot'")
    if cur.fetchone():
        cur.execute("SELECT pg_drop_replication_slot('study_cdc_slot')")
    cur.close()
    conn.close()


def reset_tables():
    import psycopg2

    conn = psycopg2.connect(PG_DSN)
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute("TRUNCATE TABLE outbox, orders RESTART IDENTITY;")
    cur.execute("UPDATE customers SET balance_cents = 1000000, version = 0;")
    cur.execute("UPDATE outbox_relay_lease SET owner = 'none', epoch = 0, expires_at = now() - interval '1 hour';")
    cur.execute("SELECT slot_name FROM pg_replication_slots WHERE slot_name = 'study_cdc_slot'")
    if cur.fetchone():
        cur.execute("SELECT pg_drop_replication_slot('study_cdc_slot')")
    cur.close()
    conn.close()


def reset_queue(queue_name="study_events"):
    import pika

    conn = pika.BlockingConnection(pika.ConnectionParameters("127.0.0.1"))
    ch = conn.channel()
    ch.queue_delete(queue=queue_name)
    ch.queue_declare(queue=queue_name, durable=True)
    conn.close()


# Compact binary envelope: event_id(16B) + agg_id(4B) + agg_ver(8B) + t_int_us(8B) + payload
ENV_STRUCT = struct.Struct("!16sIQQ")


def pack_envelope(event_uuid: uuid.UUID, agg_id: int, agg_ver: int, t_int_us: int, payload: bytes) -> bytes:
    return ENV_STRUCT.pack(event_uuid.bytes, agg_id, agg_ver, t_int_us) + payload


def unpack_envelope(body: bytes):
    eb, agg_id, agg_ver, t_int_us = ENV_STRUCT.unpack_from(body, 0)
    return uuid.UUID(bytes=eb), agg_id, agg_ver, t_int_us


# =============================================================================
# BLOCK 1: A-F1 Deterministic Correctness Stories (doc 12 §11)
# =============================================================================
def run_fault_stories():
    import pika
    import psycopg2

    log("=== Running Block 1: A-F1 Deterministic Fault Stories ===")
    results = {}

    # Helper to consume all currently available messages from RabbitMQ
    def drain_rabbit(queue_name="study_events", wait_s=0.3):
        time.sleep(wait_s)
        conn = pika.BlockingConnection(pika.ConnectionParameters("127.0.0.1"))
        ch = conn.channel()
        msgs = []
        while True:
            method, props, body = ch.basic_get(queue=queue_name, auto_ack=True)
            if method is None:
                break
            msgs.append(unpack_envelope(body))
        conn.close()
        return msgs

    # --- H-A: Lease zombie without fencing vs with DB epoch fencing (and RabbitMQ lacking broker fencing) ---
    for with_db_fencing in (False, True):
        reset_tables()
        reset_queue()
        conn = psycopg2.connect(PG_DSN)
        conn.autocommit = False
        cur = conn.cursor()
        # Insert 2 ordered events for aggregate 1 (v=1, v=2)
        e1, e2 = uuid.uuid4(), uuid.uuid4()
        t_us = int(time.time() * 1e6)
        cur.execute(
            "INSERT INTO outbox (event_id, aggregate_id, aggregate_version, partition_key, t_int_us, payload) "
            "VALUES (%s, 1, 1, 1, %s, %s), (%s, 1, 2, 1, %s, %s)",
            (str(e1), t_us, b"v1", str(e2), t_us, b"v2"),
        )
        # R1 acquires lease epoch=1
        cur.execute(
            "UPDATE outbox_relay_lease SET owner='R1', epoch=1, expires_at=now() + interval '5 seconds' WHERE partition_key=1"
        )
        conn.commit()

        # R1 reads v1 and pauses (simulating GC pause / network stall)
        cur.execute("SELECT id, event_id, aggregate_id, aggregate_version, t_int_us, payload FROM outbox WHERE partition_key=1 AND published_at IS NULL ORDER BY id LIMIT 1")
        r1_row = cur.fetchone()

        # Lease expires; R2 acquires lease epoch=2, reads v1 and v2, publishes both to RabbitMQ, marks published
        cur.execute(
            "UPDATE outbox_relay_lease SET owner='R2', epoch=2, expires_at=now() + interval '30 seconds' WHERE partition_key=1"
        )
        cur.execute("SELECT id, event_id, aggregate_id, aggregate_version, t_int_us, payload FROM outbox WHERE partition_key=1 AND published_at IS NULL ORDER BY id")
        r2_rows = cur.fetchall()

        rmq = pika.BlockingConnection(pika.ConnectionParameters("127.0.0.1"))
        ch = rmq.channel()
        ch.confirm_delivery()
        for row in r2_rows:
            body = pack_envelope(uuid.UUID(row[1]), row[2], row[3], row[4], bytes(row[5]))
            ch.basic_publish("", "study_events", body, properties=pika.BasicProperties(delivery_mode=2))
        cur.execute(
            "UPDATE outbox SET published_at = clock_timestamp() WHERE id = ANY(%s)",
            ([r[0] for r in r2_rows],),
        )
        conn.commit()

        # R1 wakes up (zombie with stale epoch=1) and publishes its stale r1_row (v1) AFTER R2 already published v1, v2!
        # Note: RabbitMQ has NO broker-side transactional producer fencing!
        zombie_body = pack_envelope(uuid.UUID(r1_row[1]), r1_row[2], r1_row[3], r1_row[4], bytes(r1_row[5]))
        ch.basic_publish("", "study_events", zombie_body, properties=pika.BasicProperties(delivery_mode=2))

        if with_db_fencing:
            cur.execute(
                "UPDATE outbox SET published_at = clock_timestamp() WHERE id = %s "
                "AND EXISTS (SELECT 1 FROM outbox_relay_lease WHERE partition_key=1 AND owner='R1' AND epoch=1 AND expires_at > clock_timestamp())",
                (r1_row[0],),
            )
            db_mark_blocked = cur.rowcount == 0
        else:
            cur.execute("UPDATE outbox SET published_at = clock_timestamp() WHERE id = %s", (r1_row[0],))
            db_mark_blocked = False
        conn.commit()
        rmq.close()
        conn.close()

        msgs = drain_rabbit()
        versions_delivered = [m[2] for m in msgs]
        # Check redelivery / out-of-order after v2
        inversions_all = sum(1 for i in range(1, len(versions_delivered)) if versions_delivered[i] < versions_delivered[i - 1])
        key = "H-A_with_db_fencing_rabbitmq" if with_db_fencing else "H-A_no_fencing_rabbitmq"
        results[key] = {
            "versions_sequence": versions_delivered,
            "db_mark_blocked": db_mark_blocked,
            "redelivery_inversions": inversions_all,
            "broker_fencing_available": False,
            "confirms_P6_db_fencing_alone_insufficient": inversions_all > 0,
        }

    # --- H-B: Crash between broker ack and DB mark (outbox duplicate window) ---
    reset_tables()
    reset_queue()
    conn = psycopg2.connect(PG_DSN)
    cur = conn.cursor()
    eid = uuid.uuid4()
    t_us = int(time.time() * 1e6)
    cur.execute(
        "INSERT INTO outbox (event_id, aggregate_id, aggregate_version, partition_key, t_int_us, payload) VALUES (%s, 1, 1, 1, %s, %s)",
        (str(eid), t_us, b"hb"),
    )
    conn.commit()
    rmq = pika.BlockingConnection(pika.ConnectionParameters("127.0.0.1"))
    ch = rmq.channel()
    ch.confirm_delivery()
    # Attempt 1: publish succeeds, FAULT POINT crashes before UPDATE published_at
    ch.basic_publish("", "study_events", pack_envelope(eid, 1, 1, t_us, b"hb"), properties=pika.BasicProperties(delivery_mode=2))
    # Attempt 2 (relay restart): row still unpublished -> publishes again and marks
    cur.execute("SELECT id, event_id FROM outbox WHERE published_at IS NULL")
    pending = cur.fetchall()
    for row in pending:
        ch.basic_publish("", "study_events", pack_envelope(uuid.UUID(row[1]), 1, 1, t_us, b"hb"), properties=pika.BasicProperties(delivery_mode=2))
        cur.execute("UPDATE outbox SET published_at = clock_timestamp() WHERE id = %s", (row[0],))
    conn.commit()
    rmq.close()
    conn.close()
    msgs = drain_rabbit()
    results["H-B_outbox_crash_post_ack"] = {
        "expected": 1,
        "delivered_unique": len({m[0] for m in msgs}),
        "total_deliveries": len(msgs),
        "lost": 1 - len({m[0] for m in msgs}),
        "duplicates": len(msgs) - len({m[0] for m in msgs}),
    }

    # --- H-C: dw-after crash between COMMIT and publish -> confirmed loss ---
    reset_tables()
    reset_queue()
    conn = psycopg2.connect(PG_DSN)
    cur = conn.cursor()
    eid = uuid.uuid4()
    cur.execute("INSERT INTO orders (id, customer_id, aggregate_version, status, amount_cents, event_id) VALUES (%s, 1, 1, 'OK', 100, %s)", (str(uuid.uuid4()), str(eid)))
    conn.commit()
    # FAULT POINT F6: crash before publish
    conn.close()
    msgs = drain_rabbit()
    results["H-C_dw_after_crash"] = {
        "expected_in_db": 1,
        "delivered_unique": len({m[0] for m in msgs}),
        "lost_confirmed": 1 - len({m[0] for m in msgs}),
    }

    # --- H-D: dw-before publish succeeds, DB ROLLBACK -> ghost event ---
    reset_tables()
    reset_queue()
    conn = psycopg2.connect(PG_DSN)
    cur = conn.cursor()
    eid = uuid.uuid4()
    t_us = int(time.time() * 1e6)
    rmq = pika.BlockingConnection(pika.ConnectionParameters("127.0.0.1"))
    ch = rmq.channel()
    ch.confirm_delivery()
    ch.basic_publish("", "study_events", pack_envelope(eid, 1, 1, t_us, b"hd"), properties=pika.BasicProperties(delivery_mode=2))
    # FAULT POINT F6: DB commit fails -> ROLLBACK
    cur.execute("INSERT INTO orders (id, customer_id, aggregate_version, status, amount_cents, event_id) VALUES (%s, 1, 1, 'OK', 100, %s)", (str(uuid.uuid4()), str(eid)))
    conn.rollback()
    rmq.close()
    conn.close()
    msgs = drain_rabbit()
    results["H-D_dw_before_rollback"] = {
        "expected_in_db": 0,
        "delivered_unique": len({m[0] for m in msgs}),
        "ghost_events": len({m[0] for m in msgs}),
    }

    # --- H-J: cleanup=early vs cleanup=after_ack when broker fails before ack ---
    hj = {}
    for mode in ("early", "after_ack"):
        reset_tables()
        reset_queue()
        conn = psycopg2.connect(PG_DSN)
        cur = conn.cursor()
        eid = uuid.uuid4()
        t_us = int(time.time() * 1e6)
        cur.execute(
            "INSERT INTO outbox (event_id, aggregate_id, aggregate_version, partition_key, t_int_us, payload) VALUES (%s, 1, 1, 1, %s, %s)",
            (str(eid), t_us, b"hj"),
        )
        conn.commit()
        if mode == "early":
            cur.execute("DELETE FROM outbox WHERE event_id = %s", (str(eid),))
            conn.commit()
            # Simulated broker unavailable -> publish raises error, row already deleted!
            broker_failed = True
        else:
            # Publish fails before ack -> row NOT deleted; on retry after recovery it publishes!
            broker_failed = True
            rmq = pika.BlockingConnection(pika.ConnectionParameters("127.0.0.1"))
            ch = rmq.channel()
            ch.confirm_delivery()
            cur.execute("SELECT id, event_id FROM outbox WHERE published_at IS NULL")
            for row in cur.fetchall():
                ch.basic_publish("", "study_events", pack_envelope(uuid.UUID(row[1]), 1, 1, t_us, b"hj"), properties=pika.BasicProperties(delivery_mode=2))
                cur.execute("DELETE FROM outbox WHERE id = %s", (row[0],))
            conn.commit()
            rmq.close()
        conn.close()
        msgs = drain_rabbit()
        hj[mode] = {"expected": 1, "delivered": len({m[0] for m in msgs}), "lost_confirmed": 1 - len({m[0] for m in msgs})}
    results["H-J_cleanup_timing"] = hj

    log(f"Block 1 done: {json.dumps(results)}")
    return results


# =============================================================================
# Core Open-Loop Benchmark Runner (faithful to doc 03 & doc 02)
# =============================================================================
def run_open_loop_trial(
    strategy: str,
    target_rate: float,
    duration_s: float = 10.0,
    poll_interval_ms: float = 100.0,
    batch_size: int = 100,
    writers: int = 4,
    workload: str = "control",
    n_customers: int = 200,
):
    """Run a single open-loop trial and return metrics (L_req, L_e2e, throughput, integrity, order)."""
    import numpy as np
    import pika
    import psycopg2
    import psycopg2.extras

    reset_tables()
    use_rabbit = strategy not in ("tx-only", "pg-queue")
    if use_rabbit:
        reset_queue()

    # Prepare CDC slot BEFORE producers start if strategy == outbox-cdc
    if strategy == "outbox-cdc":
        c = psycopg2.connect(PG_DSN)
        c.autocommit = True
        cur = c.cursor()
        cur.execute("SELECT pg_create_logical_replication_slot('study_cdc_slot', 'test_decoding')")
        cur.close()
        c.close()

    stop_event = threading.Event()
    producer_done = threading.Event()

    # Shared recording structures
    l_req_ms = []
    l_req_lock = threading.Lock()
    expected_events = {}  # event_id -> (agg_id, agg_ver, t_int_us)
    exp_lock = threading.Lock()

    delivered_map = defaultdict(list)  # event_id -> list of (t_recv_us, agg_id, agg_ver)
    deliv_lock = threading.Lock()
    l_e2e_ms = []

    # Pre-generate Zipf weights if workload == representative
    rng = np.random.default_rng(42)
    if workload == "representative":
        ranks = np.arange(1, n_customers + 1, dtype=np.float64)
        probs = (1.0 / ranks)
        probs /= probs.sum()
    else:
        probs = None

    # --- Consumer thread (RabbitMQ) ---
    consumer_thread = None
    if use_rabbit:
        def rmq_consumer():
            conn = pika.BlockingConnection(pika.ConnectionParameters("127.0.0.1"))
            ch = conn.channel()
            ch.basic_qos(prefetch_count=200)
            while not stop_event.is_set():
                method, props, body = ch.basic_get(queue="study_events", auto_ack=False)
                if method is None:
                    time.sleep(0.001)
                    continue
                t_recv_us = int(time.time() * 1e6)
                eid, agg_id, agg_ver, t_int_us = unpack_envelope(body)
                ch.basic_ack(delivery_tag=method.delivery_tag)
                with deliv_lock:
                    is_first = len(delivered_map[eid]) == 0
                    delivered_map[eid].append((t_recv_us, agg_id, agg_ver))
                    if is_first:
                        l_e2e_ms.append((t_recv_us - t_int_us) / 1000.0)
            conn.close()

        consumer_thread = threading.Thread(target=rmq_consumer, daemon=True)
        consumer_thread.start()

    # --- Relay / Queue-Consumer thread ---
    relay_thread = None
    if strategy == "pg-queue":
        def pg_queue_consumer():
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

        relay_thread = threading.Thread(target=pg_queue_consumer, daemon=True)
        relay_thread.start()

    elif strategy in ("outbox-poll", "outbox-notify"):
        def outbox_relay():
            import select

            conn = psycopg2.connect(PG_DSN)
            conn.autocommit = False
            cur = conn.cursor()

            listen_conn = None
            if strategy == "outbox-notify":
                listen_conn = psycopg2.connect(PG_DSN)
                listen_conn.autocommit = True
                lcur = listen_conn.cursor()
                lcur.execute("LISTEN outbox_ch;")

            rmq = pika.BlockingConnection(pika.ConnectionParameters("127.0.0.1"))
            ch = rmq.channel()
            ch.confirm_delivery()

            sleep_s = poll_interval_ms / 1000.0
            while not stop_event.is_set():
                cur.execute(
                    "SELECT id, event_id, aggregate_id, aggregate_version, t_int_us, payload "
                    "FROM outbox WHERE published_at IS NULL ORDER BY id LIMIT %s FOR UPDATE SKIP LOCKED",
                    (batch_size,),
                )
                rows = cur.fetchall()
                if not rows:
                    conn.commit()
                    if strategy == "outbox-notify":
                        # Wait for NOTIFY or 1s safety timeout
                        if select.select([listen_conn], [], [], min(1.0, 0.2)) != ([], [], []):
                            listen_conn.poll()
                            listen_conn.notifies.clear()
                    else:
                        time.sleep(sleep_s)
                    continue

                ids = []
                for row in rows:
                    body = pack_envelope(uuid.UUID(row[1]), row[2], row[3], row[4], bytes(row[5]))
                    ch.basic_publish("", "study_events", body, properties=pika.BasicProperties(delivery_mode=2))
                    ids.append(row[0])
                cur.execute("UPDATE outbox SET published_at = clock_timestamp() WHERE id = ANY(%s)", (ids,))
                conn.commit()
                # If batch was full, loop immediately without sleeping (M1 saturated regime)

            rmq.close()
            if listen_conn:
                listen_conn.close()
            conn.close()

        relay_thread = threading.Thread(target=outbox_relay, daemon=True)
        relay_thread.start()

    elif strategy == "outbox-cdc":
        # Real PostgreSQL Logical Replication consumer (test_decoding) -> RabbitMQ
        insert_re = re.compile(
            r"table public\.outbox: INSERT: id\[bigint\]:\d+ "
            r"event_id\[uuid\]:'([^']+)' "
            r"aggregate_id\[integer\]:(\d+) "
            r"aggregate_version\[bigint\]:(\d+) "
            r"partition_key\[integer\]:\d+ "
            r"t_int_us\[bigint\]:(\d+) "
            r"payload\[bytea\]:'\\x([0-9a-fA-F]*)'"
        )

        def cdc_relay():
            import select

            rconn = psycopg2.connect(PG_DSN, connection_factory=psycopg2.extras.LogicalReplicationConnection)
            rcur = rconn.cursor()
            rcur.start_replication(slot_name="study_cdc_slot", decode=True)

            rmq = pika.BlockingConnection(pika.ConnectionParameters("127.0.0.1"))
            ch = rmq.channel()
            ch.confirm_delivery()

            while not stop_event.is_set():
                msg = rcur.read_message()
                if msg is None:
                    select.select([rcur], [], [], 0.005)
                    continue
                m = insert_re.match(msg.payload)
                if m:
                    eid = uuid.UUID(m.group(1))
                    agg_id = int(m.group(2))
                    agg_ver = int(m.group(3))
                    t_int_us = int(m.group(4))
                    raw_payload = bytes.fromhex(m.group(5))
                    body = pack_envelope(eid, agg_id, agg_ver, t_int_us, raw_payload)
                    ch.basic_publish("", "study_events", body, properties=pika.BasicProperties(delivery_mode=2))
                msg.cursor.send_feedback(flush_lsn=msg.data_start)

            rmq.close()
            rconn.close()

        relay_thread = threading.Thread(target=cdc_relay, daemon=True)
        relay_thread.start()

    # Give relay/consumer 0.2s to settle
    time.sleep(0.2)

    # --- Open-loop generator + writer workers ---
    task_q = queue.Queue(maxsize=20000)
    errors = [0]

    def writer_worker():
        conn = psycopg2.connect(PG_DSN)
        conn.autocommit = False
        cur = conn.cursor()
        rmq = None
        ch = None
        if strategy == "dw-after":
            rmq = pika.BlockingConnection(pika.ConnectionParameters("127.0.0.1"))
            ch = rmq.channel()
            ch.confirm_delivery()

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
                new_ver = ver + 1
                cur.execute("UPDATE customers SET balance_cents = %s, version = %s WHERE id = %s", (bal - 1, new_ver, agg_id))
                cur.execute(
                    "INSERT INTO orders (id, customer_id, aggregate_version, status, amount_cents, event_id) "
                    "VALUES (%s, %s, %s, 'OK', 1, %s)",
                    (str(oid), agg_id, new_ver, str(eid)),
                )
                if strategy in ("outbox-poll", "outbox-notify", "outbox-cdc", "pg-queue"):
                    cur.execute(
                        "INSERT INTO outbox (event_id, aggregate_id, aggregate_version, partition_key, t_int_us, payload) "
                        "VALUES (%s, %s, %s, %s, %s, %s)",
                        (str(eid), agg_id, new_ver, agg_id % 16, t_int_us, psycopg2.Binary(payload)),
                    )
                if strategy == "outbox-notify":
                    cur.execute("NOTIFY outbox_ch, '1'")
                conn.commit()

                if strategy == "dw-after":
                    body = pack_envelope(eid, agg_id, new_ver, t_int_us, payload)
                    ch.basic_publish("", "study_events", body, properties=pika.BasicProperties(delivery_mode=2))

                t_done_us = int(time.time() * 1e6)
                with l_req_lock:
                    l_req_ms.append((t_done_us - t_int_us) / 1000.0)
                with exp_lock:
                    expected_events[eid] = (agg_id, new_ver, t_int_us)
            except Exception:
                conn.rollback()
                errors[0] += 1

        if rmq:
            rmq.close()
        conn.close()

    worker_threads = [threading.Thread(target=writer_worker, daemon=True) for _ in range(writers)]
    for wt in worker_threads:
        wt.start()

    # Open-loop arrival schedule
    total_events = int(target_rate * duration_s)
    interval_s = 1.0 / target_rate
    t_start = time.perf_counter()
    wall_start_us = int(time.time() * 1e6)

    # Pre-create payload bytes (incompressible-ish random bytes)
    fixed_payload = os.urandom(1024)
    var_payloads = [os.urandom(sz) for sz in (512, 1024, 2048, 4096, 8192)]

    for i in range(total_events):
        scheduled_offset = i * interval_s
        now_offset = time.perf_counter() - t_start
        if scheduled_offset > now_offset:
            time.sleep(scheduled_offset - now_offset)
        t_int_us = wall_start_us + int(scheduled_offset * 1e6)
        if workload == "representative":
            agg_id = int(rng.choice(n_customers, p=probs))
            payload = var_payloads[i % len(var_payloads)]
        else:
            agg_id = i % n_customers
            payload = fixed_payload
        task_q.put((t_int_us, agg_id, payload))

    for _ in worker_threads:
        task_q.put(None)
    for wt in worker_threads:
        wt.join(timeout=30)
    producer_done.set()

    # Drain until quiescence (all expected events delivered or max 6s drain)
    if strategy != "tx-only":
        drain_deadline = time.time() + 6.0
        while time.time() < drain_deadline:
            with deliv_lock:
                if len(delivered_map) >= len(expected_events):
                    break
            time.sleep(0.05)
        time.sleep(0.2)

    stop_event.set()
    if relay_thread:
        relay_thread.join(timeout=5)
    if consumer_thread:
        consumer_thread.join(timeout=5)

    # Compute metrics
    def pct(arr, q):
        return round(float(np.percentile(arr, q)), 3) if arr else None

    expected_n = len(expected_events)
    delivered_unique = len(delivered_map) if strategy != "tx-only" else expected_n
    censored = max(0, expected_n - delivered_unique) if strategy != "tx-only" else 0
    duplicates = sum(max(0, len(v) - 1) for v in delivered_map.values()) if strategy != "tx-only" else 0

    # Verify O1 order per aggregate (first deliveries ordered by t_recv_us)
    order_violations = 0
    if strategy != "tx-only":
        by_agg = defaultdict(list)
        for eid, recs in delivered_map.items():
            t_first, agg_id, agg_ver = recs[0]
            by_agg[agg_id].append((t_first, agg_ver))
        for agg_id, seq in by_agg.items():
            seq.sort(key=lambda x: x[0])
            for j in range(1, len(seq)):
                if seq[j][1] < seq[j - 1][1]:
                    order_violations += 1

    return {
        "strategy": strategy,
        "target_rate": target_rate,
        "poll_ms": poll_interval_ms if strategy == "outbox-poll" else None,
        "writers": writers,
        "workload": workload,
        "expected": expected_n,
        "delivered_unique": delivered_unique,
        "censored": censored,
        "duplicates": duplicates,
        "order_violations_O1": order_violations,
        "errors": errors[0],
        "l_req_p50_ms": pct(l_req_ms, 50),
        "l_req_p99_ms": pct(l_req_ms, 99),
        "l_e2e_p50_ms": pct(l_e2e_ms, 50) if strategy != "tx-only" else None,
        "l_e2e_p99_ms": pct(l_e2e_ms, 99) if strategy != "tx-only" else None,
    }


# =============================================================================
# BLOCK 4: A-E2 NOTIFY Contention under Concurrency (Ablation A7 / P3)
# =============================================================================
def run_notify_contention_ablation():
    import psycopg2

    log("=== Running Block 4: A-E2 NOTIFY Contention Ablation (P3) ===")
    rows = []
    for c in (4, 16, 32):
        for mode in ("outbox-poll", "outbox-notify"):
            reset_tables()
            stop_at = time.perf_counter() + 5.0
            counts = [0] * c

            def worker(idx):
                conn = psycopg2.connect(PG_DSN)
                conn.autocommit = False
                cur = conn.cursor()
                payload = b"x" * 256
                local_n = 0
                while time.perf_counter() < stop_at:
                    agg_id = (idx * 7 + local_n) % 200
                    eid = str(uuid.uuid4())
                    oid = str(uuid.uuid4())
                    cur.execute("SELECT balance_cents, version FROM customers WHERE id = %s FOR UPDATE", (agg_id,))
                    bal, ver = cur.fetchone()
                    cur.execute("UPDATE customers SET balance_cents = %s, version = %s WHERE id = %s", (bal - 1, ver + 1, agg_id))
                    cur.execute(
                        "INSERT INTO orders (id, customer_id, aggregate_version, status, amount_cents, event_id) VALUES (%s, %s, %s, 'OK', 1, %s)",
                        (oid, agg_id, ver + 1, eid),
                    )
                    cur.execute(
                        "INSERT INTO outbox (event_id, aggregate_id, aggregate_version, partition_key, t_int_us, payload) VALUES (%s, %s, %s, %s, 0, %s)",
                        (eid, agg_id, ver + 1, agg_id % 16, psycopg2.Binary(payload)),
                    )
                    if mode == "outbox-notify":
                        cur.execute("NOTIFY outbox_ch, '1'")
                    conn.commit()
                    local_n += 1
                counts[idx] = local_n
                conn.close()

            t0 = time.perf_counter()
            threads = [threading.Thread(target=worker, args=(i,), daemon=True) for i in range(c)]
            for t in threads:
                t.start()
            for t in threads:
                t.join(timeout=15)
            elapsed = time.perf_counter() - t0
            tps = round(sum(counts) / elapsed, 1)
            rows.append({"concurrency": c, "mode": mode, "tx_total": sum(counts), "elapsed_s": round(elapsed, 2), "tps": tps})
            log(f"  A-E2 c={c} {mode}: {tps} tx/s")
    return rows


def main():
    t_all = time.time()
    pg_ver = bootstrap_environment()
    setup_schema(n_customers=200)

    out = {
        "meta": {
            "platform": platform.platform(),
            "cpus": os.cpu_count(),
            "python": platform.python_version(),
            "postgres_version": pg_ver,
            "scope": "single-host Colab CPU VM (T1/PG1 exploratory vertical slice)",
            "not_verified_here": ["F2_real_page_cache", "T3_multi_host_quorum", "PG2_sync_standby", "K3_K4_network_replication"],
        }
    }

    # Block 1: Deterministic fault stories (A-F1)
    out["A_F1_fault_stories"] = run_fault_stories()

    # Block 2: Small company profile (A-S1: W0 = 10 and 50 ev/s)
    log("=== Running Block 2: A-S1 Small Company Profile (10 & 50 ev/s) ===")
    a_s1 = []
    for rate in (10, 50):
        for strat, p_ms in [
            ("tx-only", 0),
            ("dw-after", 0),
            ("pg-queue", 0),
            ("outbox-poll", 100),
            ("outbox-poll", 1000),
            ("outbox-notify", 0),
            ("outbox-cdc", 0),
        ]:
            r = run_open_loop_trial(strat, target_rate=rate, duration_s=8.0, poll_interval_ms=p_ms, writers=2, workload="control")
            a_s1.append(r)
            log(f"  A-S1 rate={rate} {strat}(p={p_ms}): L_req p99={r['l_req_p99_ms']}ms, L_e2e p99={r['l_e2e_p99_ms']}ms, deliv={r['delivered_unique']}/{r['expected']}")
    out["A_S1_small_company"] = a_s1

    # Block 3: Enterprise profile frontier (A-E1: 25, 100, 300, 600 ev/s, representative workload)
    log("=== Running Block 3: A-E1 Enterprise Frontier (Representative Workload) ===")
    a_e1 = []
    for rate in (25, 100, 300, 600):
        for strat, p_ms in [
            ("outbox-poll", 10),
            ("outbox-poll", 100),
            ("outbox-notify", 0),
            ("outbox-cdc", 0),
            ("pg-queue", 0),
        ]:
            r = run_open_loop_trial(strat, target_rate=rate, duration_s=8.0, poll_interval_ms=p_ms, writers=4, workload="representative")
            a_e1.append(r)
            log(f"  A-E1 rate={rate} {strat}(p={p_ms}): L_req p99={r['l_req_p99_ms']}ms, L_e2e p99={r['l_e2e_p99_ms']}ms, cens={r['censored']}, O1_viol={r['order_violations_O1']}")
    out["A_E1_enterprise_frontier"] = a_e1

    # Block 4: NOTIFY contention ablation (A-E2 / P3)
    out["A_E2_notify_contention"] = run_notify_contention_ablation()

    out["meta"]["total_runtime_s"] = round(time.time() - t_all, 1)
    print("===STUDY_RESULTS_JSON_START===")
    print(json.dumps(out, indent=2))
    print("===STUDY_RESULTS_JSON_END===")
    log(f"All blocks completed in {out['meta']['total_runtime_s']}s")


if __name__ == "__main__":
    main()
