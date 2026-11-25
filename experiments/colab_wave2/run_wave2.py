#!/usr/bin/env python3
"""Campanha Onda 2 no Colab (--high-mem, teardown automatico):
  W2-A: Validacao preditiva selada (N17 / Pilar N3) — 12 configuracoes ineditas V01-V12
  W2-B: Fronteira Kafka alta taxa (500 -> 3000 ev/s) — achando lambda* real (P1)
  W2-C: NATS JetStream + Ablacao A3 fsync no mesmo sistema (Kafka e NATS: page cache x fsync, P4/M8)
  W2-D: R=10 blocos pareados nos pontos decisivos (t_{0.975,9}=2.262 -> sair do inconclusivo, S2)
  W2-E: Cenarios de producao para o seletor: G04 (payload 1-64 KiB), G08 (fan-out 1-4 grupos), G18 (replay 5000 ev)
"""

import asyncio
import json
import os
import platform
import queue
import re
import select
import struct
import subprocess
import sys
import threading
import time
import uuid
from collections import defaultdict


def log(msg):
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", file=sys.stderr, flush=True)


def run_coro(coro):
    """Run an asyncio coroutine safely even inside IPython/Colab's running event loop."""
    box = {}
    def _runner():
        try:
            box["val"] = asyncio.run(coro)
        except Exception as e:
            box["err"] = e
    t = threading.Thread(target=_runner, daemon=True)
    t.start()
    t.join()
    if "err" in box:
        raise box["err"]
    return box.get("val")


def sh(cmd, check=True, timeout=240):
    res = subprocess.run(cmd, shell=True, text=True, capture_output=True, timeout=timeout)
    if check and res.returncode != 0:
        log(f"FAIL: {cmd}\nSTDOUT: {res.stdout[-800:]}\nSTDERR: {res.stderr[-800:]}")
        raise RuntimeError(f"Command failed ({res.returncode}): {cmd}")
    return res


PG_DSN = "host=127.0.0.1 port=5432 dbname=study user=postgres password=postgres"
ENV_STRUCT = struct.Struct("!16sIQQ")
CDC_INSERT_RE = re.compile(
    r"table public\.outbox: INSERT: id\[bigint\]:\d+ "
    r"event_id\[uuid\]:'([^']+)' "
    r"aggregate_id\[integer\]:(\d+) "
    r"aggregate_version\[bigint\]:(\d+) "
    r"partition_key\[integer\]:\d+ "
    r"t_int_us\[bigint\]:(\d+) "
    r"payload\[bytea\]:'\\x([0-9a-fA-F]*)'"
)


def pack_env(eid, agg_id, agg_ver, t_int_us, payload):
    return ENV_STRUCT.pack(eid.bytes, agg_id, agg_ver, t_int_us) + payload


def unpack_env(body):
    eb, agg_id, agg_ver, t_int_us = ENV_STRUCT.unpack_from(body, 0)
    return uuid.UUID(bytes=eb), agg_id, agg_ver, t_int_us


def bootstrap_all():
    t0 = time.time()
    os.environ["DEBIAN_FRONTEND"] = "noninteractive"
    log("Installing PG16, Kafka 3.9, NATS JetStream, RabbitMQ, Redis, Python clients...")
    sh("apt-get update -qq && apt-get install -y -qq postgresql postgresql-contrib rabbitmq-server redis-server openjdk-21-jre-headless curl unzip")
    sh(f"{sys.executable} -m pip install -q psycopg2-binary confluent-kafka pika redis nats-py numpy")

    # 1. PostgreSQL
    pg_ver = sh("ls /etc/postgresql | sort -V | tail -n 1").stdout.strip()
    with open(f"/etc/postgresql/{pg_ver}/main/postgresql.conf", "a") as f:
        f.write(
            "\nwal_level = logical\nmax_replication_slots = 12\nmax_wal_senders = 12\n"
            "max_connections = 250\nsynchronous_commit = on\nshared_buffers = 1GB\n"
            "work_mem = 16MB\ncheckpoint_timeout = 10min\n"
        )
    hba_p = f"/etc/postgresql/{pg_ver}/main/pg_hba.conf"
    old_hba = open(hba_p).read()
    open(hba_p, "w").write(
        "local all all trust\nhost all all 127.0.0.1/32 trust\n"
        "local replication all trust\nhost replication all 127.0.0.1/32 trust\n" + old_hba
    )
    sh("service postgresql restart")
    sh("su - postgres -c \"psql -c \\\"ALTER USER postgres WITH PASSWORD 'postgres' SUPERUSER REPLICATION;\\\"\"")
    sh("su - postgres -c \"psql -c \\\"CREATE DATABASE study;\\\"\"", check=False)

    # 2. RabbitMQ
    sh("service rabbitmq-server start")
    for _ in range(30):
        if sh("rabbitmqctl status", check=False).returncode == 0:
            break
        time.sleep(1)

    # 3. Redis (AOF always)
    sh("redis-server --daemonize yes --appendonly yes --appendfsync always --save '' --port 6379")
    for _ in range(15):
        if sh("redis-cli ping", check=False).stdout.strip() == "PONG":
            break
        time.sleep(0.5)

    # 4. NATS Server with JetStream (2 instances: port 4222 page-cache default; port 4223 sync_interval=always!)
    sh(
        "curl -fsSL https://github.com/nats-io/nats-server/releases/download/v2.10.22/nats-server-v2.10.22-linux-amd64.tar.gz -o /tmp/nats.tgz "
        "&& tar -xzf /tmp/nats.tgz -C /tmp && mv /tmp/nats-server-v2.10.22-linux-amd64/nats-server /usr/local/bin/nats-server "
        "&& chmod +x /usr/local/bin/nats-server && rm -rf /tmp/nats.tgz /tmp/nats-server-*"
    )
    open("/tmp/nats_fast.conf", "w").write("port: 4222\njetstream { store_dir: \"/tmp/nats_js_fast\" }\n")
    open("/tmp/nats_fsync.conf", "w").write("port: 4223\njetstream { store_dir: \"/tmp/nats_js_fsync\", sync_interval: \"always\" }\n")
    sh("nats-server -c /tmp/nats_fast.conf > /tmp/nats_fast.log 2>&1 &")
    sh("nats-server -c /tmp/nats_fsync.conf > /tmp/nats_fsync.log 2>&1 &")
    time.sleep(1.5)

    # 5. Kafka 3.9.0 KRaft (group.initial.rebalance.delay.ms=0!)
    sh(
        "curl -fsSL https://archive.apache.org/dist/kafka/3.9.0/kafka_2.13-3.9.0.tgz -o /tmp/k.tgz "
        "&& mkdir -p /opt/kafka && tar -xzf /tmp/k.tgz -C /opt/kafka --strip-components=1 && rm /tmp/k.tgz"
    )
    kp = "/opt/kafka/config/kraft/server.properties"
    lines = [
        l for l in open(kp).read().splitlines()
        if not l.startswith((
            "listeners=", "advertised.listeners=", "offsets.topic.replication.factor=",
            "transaction.state.log.", "num.partitions=", "group.initial.rebalance.delay.ms=",
            "message.max.bytes=", "replica.fetch.max.bytes=",
        ))
    ]
    lines.extend([
        "offsets.topic.replication.factor=1",
        "transaction.state.log.replication.factor=1",
        "transaction.state.log.min.isr=1",
        "group.initial.rebalance.delay.ms=0",
        "num.partitions=12",
        "message.max.bytes=2097152",
        "listeners=PLAINTEXT://127.0.0.1:9092,CONTROLLER://127.0.0.1:9093",
        "advertised.listeners=PLAINTEXT://127.0.0.1:9092",
    ])
    open(kp, "w").write("\n".join(lines) + "\n")
    cid = sh("/opt/kafka/bin/kafka-storage.sh random-uuid").stdout.strip().splitlines()[-1].strip()
    sh(f"/opt/kafka/bin/kafka-storage.sh format -t {cid} -c {kp} --ignore-formatted")
    sh("KAFKA_HEAP_OPTS='-Xmx2G -Xms1G' /opt/kafka/bin/kafka-server-start.sh -daemon /opt/kafka/config/kraft/server.properties")
    for _ in range(40):
        if sh("/opt/kafka/bin/kafka-topics.sh --bootstrap-server 127.0.0.1:9092 --list", check=False).returncode == 0:
            break
        time.sleep(1)

    mem_gb = round(os.sysconf("SC_PAGE_SIZE") * os.sysconf("SC_PHYS_PAGES") / (1024**3), 1)
    log(f"Bootstrap completed in {time.time() - t0:.1f}s (PG {pg_ver}, Kafka 3.9, NATS JS fast+fsync, RabbitMQ, Redis, CPUs={os.cpu_count()}, RAM={mem_gb} GiB)")
    return {"pg_ver": pg_ver, "cpus": os.cpu_count(), "ram_gib": mem_gb}


def setup_schema(n_customers=200):
    import psycopg2
    conn = psycopg2.connect(PG_DSN)
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute("""
        DROP TABLE IF EXISTS outbox, orders, customers CASCADE;
        CREATE TABLE customers (id INT PRIMARY KEY, balance_cents BIGINT NOT NULL, version BIGINT NOT NULL DEFAULT 0);
        CREATE TABLE orders (id UUID PRIMARY KEY, customer_id INT NOT NULL, aggregate_version BIGINT NOT NULL, event_id UUID NOT NULL);
        CREATE TABLE outbox (
          id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
          event_id UUID NOT NULL, aggregate_id INT NOT NULL, aggregate_version BIGINT NOT NULL,
          partition_key INT NOT NULL, t_int_us BIGINT NOT NULL, payload BYTEA NOT NULL, published_at TIMESTAMPTZ
        );
        CREATE INDEX outbox_pending_idx ON outbox (id) WHERE published_at IS NULL;
    """)
    cur.executemany("INSERT INTO customers VALUES (%s, 100000000, 0)", [(i,) for i in range(n_customers)])
    conn.close()


def reset_pg():
    import psycopg2
    conn = psycopg2.connect(PG_DSN)
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute("TRUNCATE TABLE outbox, orders RESTART IDENTITY; UPDATE customers SET balance_cents=100000000, version=0;")
    cur.execute("SELECT slot_name FROM pg_replication_slots WHERE slot_name='study_cdc_slot'")
    if cur.fetchone():
        cur.execute("SELECT pg_drop_replication_slot('study_cdc_slot')")
    conn.close()


# =============================================================================
# Unified Transport Helpers (Kafka, Kafka-fsync, NATS-JS, NATS-JS-fsync, RabbitMQ, Redis)
# =============================================================================
def prepare_dest(transport: str, dest: str):
    if transport in ("kafka", "kafka-fsync"):
        from confluent_kafka.admin import AdminClient, NewTopic
        admin = AdminClient({"bootstrap.servers": "127.0.0.1:9092"})
        cfg = {"flush.messages": "1", "flush.ms": "0"} if transport == "kafka-fsync" else {}
        fs = admin.create_topics([NewTopic(dest, num_partitions=12, replication_factor=1, config=cfg)])
        for f in fs.values():
            try:
                f.result(timeout=10)
            except Exception:
                pass
    elif transport == "rabbitmq":
        import pika
        conn = pika.BlockingConnection(pika.ConnectionParameters("127.0.0.1"))
        ch = conn.channel()
        ch.queue_delete(queue=dest)
        ch.queue_declare(queue=dest, durable=True)
        conn.close()
    elif transport == "redis":
        import redis
        r = redis.Redis(host="127.0.0.1", port=6379)
        r.delete(dest)
        r.xgroup_create(dest, "study_grp", id="0", mkstream=True)
        r.close()
    elif transport in ("nats", "nats-fsync"):
        port = 4223 if transport == "nats-fsync" else 4222
        async def _mk():
            import nats
            nc = await nats.connect(f"nats://127.0.0.1:{port}")
            js = nc.jetstream()
            try:
                await js.delete_stream(dest)
            except Exception:
                pass
            await js.add_stream(name=dest, subjects=[f"{dest}.*"], storage="file")
            await nc.close()
        run_coro(_mk())


class UnifiedPublisher:
    def __init__(self, transport: str, dest: str):
        self.transport = transport
        self.dest = dest
        if transport in ("kafka", "kafka-fsync"):
            from confluent_kafka import Producer
            self.prod = Producer({
                "bootstrap.servers": "127.0.0.1:9092",
                "acks": "all",
                "linger.ms": 1,
                "batch.num.messages": 500,
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
        elif transport in ("nats", "nats-fsync"):
            import nats
            self.port = 4223 if transport == "nats-fsync" else 4222
            self.loop = asyncio.new_event_loop()
            self.nc = self.loop.run_until_complete(nats.connect(f"nats://127.0.0.1:{self.port}"))
            self.js = self.nc.jetstream()

    def publish_batch(self, items):
        if not items:
            return
        if self.transport in ("kafka", "kafka-fsync"):
            for aid, body in items:
                self.prod.produce(self.dest, key=str(aid).encode(), value=body)
            self.prod.flush(timeout=15.0)
        elif self.transport == "rabbitmq":
            for _, body in items:
                self.ch.basic_publish("", self.dest, body, properties=self.props)
        elif self.transport == "redis":
            pipe = self.r.pipeline(transaction=False)
            for _, body in items:
                pipe.xadd(self.dest, {"b": body})
            pipe.execute()
        elif self.transport in ("nats", "nats-fsync"):
            async def _pub():
                # Concurrent JetStream acks within the batch
                coros = [self.js.publish(f"{self.dest}.{aid % 12}", body) for aid, body in items]
                await asyncio.gather(*coros)
            self.loop.run_until_complete(_pub())

    def close(self):
        if self.transport in ("kafka", "kafka-fsync"):
            self.prod.flush(timeout=5.0)
        elif self.transport == "rabbitmq":
            self.conn.close()
        elif self.transport == "redis":
            self.r.close()
        elif self.transport in ("nats", "nats-fsync"):
            self.loop.run_until_complete(self.nc.close())
            self.loop.close()


def start_consumers(transport, dest, n_groups, stop_ev, deliv_maps, deliv_lock, e2e_lists):
    """Starts n_groups independent consumer groups; returns (threads, ready_events)."""
    threads, readys = [], []
    for g in range(n_groups):
        ready = threading.Event()
        readys.append(ready)

        if transport in ("kafka", "kafka-fsync"):
            def make_k(gidx, rev):
                def run_k():
                    from confluent_kafka import Consumer
                    c = Consumer({
                        "bootstrap.servers": "127.0.0.1:9092",
                        "group.id": f"g{gidx}-{uuid.uuid4().hex[:6]}",
                        "auto.offset.reset": "earliest",
                        "enable.auto.commit": False,
                        "fetch.wait.max.ms": 2,
                    })
                    c.subscribe([dest], on_assign=lambda _c, _p: rev.set())
                    while not stop_ev.is_set():
                        msgs = c.consume(num_messages=300, timeout=0.005)
                        if not msgs:
                            continue
                        t_us = int(time.time() * 1e6)
                        with deliv_lock:
                            dm = deliv_maps[gidx]
                            el = e2e_lists[gidx]
                            for m in msgs:
                                if m is None or m.error():
                                    continue
                                eid, aid, av, ti = unpack_env(m.value())
                                first = len(dm[eid]) == 0
                                dm[eid].append((t_us, aid, av))
                                if first:
                                    el.append((t_us - ti) / 1000.0)
                        c.commit(asynchronous=True)
                    c.close()
                return run_k
            t = threading.Thread(target=make_k(g, ready), daemon=True)
            t.start()
            threads.append(t)

        elif transport in ("nats", "nats-fsync"):
            port = 4223 if transport == "nats-fsync" else 4222
            def make_n(gidx, rev, p):
                def run_n():
                    async def _loop():
                        import nats
                        nc = await nats.connect(f"nats://127.0.0.1:{p}")
                        js = nc.jetstream()
                        psub = await js.pull_subscribe(f"{dest}.*", durable=f"dur_{gidx}_{uuid.uuid4().hex[:5]}", stream=dest)
                        rev.set()
                        while not stop_ev.is_set():
                            try:
                                msgs = await psub.fetch(200, timeout=0.02)
                            except Exception:
                                continue
                            t_us = int(time.time() * 1e6)
                            with deliv_lock:
                                dm = deliv_maps[gidx]
                                el = e2e_lists[gidx]
                                for m in msgs:
                                    eid, aid, av, ti = unpack_env(m.data)
                                    first = len(dm[eid]) == 0
                                    dm[eid].append((t_us, aid, av))
                                    if first:
                                        el.append((t_us - ti) / 1000.0)
                            for m in msgs:
                                await m.ack()
                        await nc.close()
                    lp = asyncio.new_event_loop()
                    lp.run_until_complete(_loop())
                    lp.close()
                return run_n
            t = threading.Thread(target=make_n(g, ready, port), daemon=True)
            t.start()
            threads.append(t)

        elif transport == "rabbitmq":
            def make_r(gidx, rev):
                def run_r():
                    import pika
                    conn = pika.BlockingConnection(pika.ConnectionParameters("127.0.0.1"))
                    ch = conn.channel()
                    ch.basic_qos(prefetch_count=200)
                    rev.set()
                    while not stop_ev.is_set():
                        got, last_tag = 0, None
                        while got < 100:
                            m, _, body = ch.basic_get(queue=dest, auto_ack=False)
                            if m is None:
                                break
                            t_us = int(time.time() * 1e6)
                            eid, aid, av, ti = unpack_env(body)
                            with deliv_lock:
                                dm = deliv_maps[gidx]
                                el = e2e_lists[gidx]
                                first = len(dm[eid]) == 0
                                dm[eid].append((t_us, aid, av))
                                if first:
                                    el.append((t_us - ti) / 1000.0)
                            last_tag = m.delivery_tag
                            got += 1
                        if last_tag is not None:
                            ch.basic_ack(delivery_tag=last_tag, multiple=True)
                        else:
                            time.sleep(0.002)
                    conn.close()
                return run_r
            t = threading.Thread(target=make_r(g, ready), daemon=True)
            t.start()
            threads.append(t)

        elif transport == "redis":
            def make_rd(gidx, rev):
                def run_rd():
                    import redis
                    r = redis.Redis(host="127.0.0.1", port=6379)
                    rev.set()
                    while not stop_ev.is_set():
                        res = r.xreadgroup("study_grp", f"c{gidx}", {dest: ">"}, count=200, block=10)
                        if not res:
                            continue
                        t_us = int(time.time() * 1e6)
                        acks = []
                        with deliv_lock:
                            dm = deliv_maps[gidx]
                            el = e2e_lists[gidx]
                            for _, entries in res:
                                for eid_s, fields in entries:
                                    eid, aid, av, ti = unpack_env(fields[b"b"])
                                    first = len(dm[eid]) == 0
                                    dm[eid].append((t_us, aid, av))
                                    if first:
                                        el.append((t_us - ti) / 1000.0)
                                    acks.append(eid_s)
                        if acks:
                            r.xack(dest, "study_grp", *acks)
                    r.close()
                return run_rd
            t = threading.Thread(target=make_rd(g, ready), daemon=True)
            t.start()
            threads.append(t)

    for rev in readys:
        rev.wait(timeout=10.0)
    return threads


def run_trial(
    transport: str,
    strategy: str,
    rate: float,
    duration_s: float = 4.0,
    poll_ms: float = 100.0,
    batch_size: int = 100,
    writers: int = 4,
    payload_bytes: int = 1024,
    n_groups: int = 1,
    seed: int = 42,
):
    import numpy as np
    import psycopg2
    import psycopg2.extras

    reset_pg()
    dest = f"w2_{transport.replace('-', '')}_{uuid.uuid4().hex[:6]}"
    if transport != "pg-queue":
        prepare_dest(transport, dest)

    if strategy == "outbox-cdc":
        c = psycopg2.connect(PG_DSN)
        c.autocommit = True
        c.cursor().execute("SELECT pg_create_logical_replication_slot('study_cdc_slot', 'test_decoding')")
        c.close()

    stop_ev = threading.Event()
    deliv_lock = threading.Lock()
    deliv_maps = [defaultdict(list) for _ in range(n_groups)]
    e2e_lists = [[] for _ in range(n_groups)]
    l_req = []
    exp = {}

    cthreads = []
    if transport != "pg-queue":
        cthreads = start_consumers(transport, dest, n_groups, stop_ev, deliv_maps, deliv_lock, e2e_lists)

    # Relay / pg-queue
    if strategy == "pg-queue":
        def pgq():
            conn = psycopg2.connect(PG_DSN)
            conn.autocommit = False
            cur = conn.cursor()
            while not stop_ev.is_set():
                cur.execute(
                    "WITH p AS (SELECT id, event_id, aggregate_id, aggregate_version, t_int_us FROM outbox ORDER BY id LIMIT %s FOR UPDATE SKIP LOCKED) "
                    "DELETE FROM outbox USING p WHERE outbox.id = p.id RETURNING p.event_id, p.aggregate_id, p.aggregate_version, p.t_int_us",
                    (batch_size,),
                )
                rows = cur.fetchall()
                conn.commit()
                if not rows:
                    time.sleep(0.004)
                    continue
                t_us = int(time.time() * 1e6)
                with deliv_lock:
                    for r in rows:
                        eid = uuid.UUID(r[0])
                        first = len(deliv_maps[0][eid]) == 0
                        deliv_maps[0][eid].append((t_us, r[1], r[2]))
                        if first:
                            e2e_lists[0].append((t_us - r[3]) / 1000.0)
            conn.close()
        rt = threading.Thread(target=pgq, daemon=True)
        rt.start()
    elif strategy == "outbox-cdc":
        def r_cdc():
            rc = psycopg2.connect(PG_DSN, connection_factory=psycopg2.extras.LogicalReplicationConnection)
            rcur = rc.cursor()
            rcur.start_replication(slot_name="study_cdc_slot", decode=True)
            pub = UnifiedPublisher(transport, dest)
            while not stop_ev.is_set():
                batch, last = [], None
                while len(batch) < batch_size:
                    msg = rcur.read_message()
                    if msg is None:
                        break
                    last = msg
                    m = CDC_INSERT_RE.match(msg.payload)
                    if m:
                        batch.append((int(m.group(2)), pack_env(uuid.UUID(m.group(1)), int(m.group(2)), int(m.group(3)), int(m.group(4)), bytes.fromhex(m.group(5)))))
                if batch:
                    pub.publish_batch(batch)
                if last:
                    last.cursor.send_feedback(flush_lsn=last.data_start)
                else:
                    select.select([rcur], [], [], 0.002)
            pub.close()
            rc.close()
        rt = threading.Thread(target=r_cdc, daemon=True)
        rt.start()
    else:
        def r_poll():
            conn = psycopg2.connect(PG_DSN)
            conn.autocommit = False
            cur = conn.cursor()
            pub = UnifiedPublisher(transport, dest)
            sleep_s = poll_ms / 1000.0
            while not stop_ev.is_set():
                cur.execute("SELECT id, event_id, aggregate_id, aggregate_version, t_int_us, payload FROM outbox WHERE published_at IS NULL ORDER BY id LIMIT %s FOR UPDATE SKIP LOCKED", (batch_size,))
                rows = cur.fetchall()
                if not rows:
                    conn.commit()
                    time.sleep(sleep_s)
                    continue
                pub.publish_batch([(r[2], pack_env(uuid.UUID(r[1]), r[2], r[3], r[4], bytes(r[5]))) for r in rows])
                cur.execute("UPDATE outbox SET published_at=clock_timestamp() WHERE id=ANY(%s)", ([r[0] for r in rows],))
                conn.commit()
            pub.close()
            conn.close()
        rt = threading.Thread(target=r_poll, daemon=True)
        rt.start()

    time.sleep(0.15)

    # Producers
    rng = np.random.default_rng(seed)
    ranks = np.arange(1, 201, dtype=np.float64)
    probs = (1.0 / ranks) / (1.0 / ranks).sum()
    fixed_pl = os.urandom(payload_bytes)
    q = queue.Queue(maxsize=40000)

    def writer():
        conn = psycopg2.connect(PG_DSN)
        conn.autocommit = False
        cur = conn.cursor()
        while True:
            item = q.get()
            if item is None:
                break
            ti, aid = item
            eid, oid = uuid.uuid4(), uuid.uuid4()
            cur.execute("SELECT balance_cents, version FROM customers WHERE id=%s FOR UPDATE", (aid,))
            bal, ver = cur.fetchone()
            nv = ver + 1
            cur.execute("UPDATE customers SET balance_cents=%s, version=%s WHERE id=%s", (bal - 1, nv, aid))
            cur.execute("INSERT INTO orders VALUES (%s,%s,%s,%s)", (str(oid), aid, nv, str(eid)))
            cur.execute("INSERT INTO outbox (event_id, aggregate_id, aggregate_version, partition_key, t_int_us, payload) VALUES (%s,%s,%s,%s,%s,%s)", (str(eid), aid, nv, aid % 16, ti, psycopg2.Binary(fixed_pl)))
            conn.commit()
            td = int(time.time() * 1e6)
            with deliv_lock:
                l_req.append((td - ti) / 1000.0)
                exp[eid] = (aid, nv)
        conn.close()

    wts = [threading.Thread(target=writer, daemon=True) for _ in range(writers)]
    for t in wts:
        t.start()

    n_ev = int(rate * duration_s)
    dt = 1.0 / rate
    t0 = time.perf_counter()
    w0 = int(time.time() * 1e6)
    for i in range(n_ev):
        sched = i * dt
        now = time.perf_counter() - t0
        if sched > now:
            time.sleep(sched - now)
        q.put((w0 + int(sched * 1e6), int(rng.choice(200, p=probs))))

    for _ in wts:
        q.put(None)
    for t in wts:
        t.join(timeout=25)

    drain_end = time.time() + 4.5
    while time.time() < drain_end:
        with deliv_lock:
            if all(len(dm) >= len(exp) for dm in deliv_maps):
                break
        time.sleep(0.03)

    stop_ev.set()
    rt.join(timeout=5)
    for t in cthreads:
        t.join(timeout=5)

    def pct(a, p):
        return round(float(np.percentile(a, p)), 2) if a else None

    all_e2e = [x for el in e2e_lists for x in el]
    min_deliv = min(len(dm) for dm in deliv_maps)
    return {
        "transport": transport,
        "strategy": strategy,
        "rate": rate,
        "poll_ms": poll_ms if strategy == "outbox-poll" else None,
        "batch_size": batch_size,
        "payload_bytes": payload_bytes,
        "n_groups": n_groups,
        "expected": len(exp),
        "delivered_min_group": min_deliv,
        "censored": max(0, len(exp) - min_deliv),
        "l_req_p99_ms": pct(l_req, 99),
        "l_e2e_p50_ms": pct(all_e2e, 50),
        "l_e2e_p99_ms": pct(all_e2e, 99),
    }


def run_g18_replay(n_events=5000):
    """G18: Replay 5,000 retained events by a brand-new consumer group on S-log brokers."""
    log(f"=== W2-E G18: Replay {n_events} retained events (Kafka vs NATS JetStream vs Redis Streams) ===")
    pl = os.urandom(1024)
    res = {}

    # 1. Kafka
    topic = "g18_replay_kafka"
    prepare_dest("kafka", topic)
    pub = UnifiedPublisher("kafka", topic)
    t_us = int(time.time() * 1e6)
    items = [(i % 200, pack_env(uuid.uuid4(), i % 200, i, t_us, pl)) for i in range(n_events)]
    pub.publish_batch(items)
    pub.close()

    from confluent_kafka import Consumer
    c = Consumer({"bootstrap.servers": "127.0.0.1:9092", "group.id": f"replay-{uuid.uuid4().hex[:6]}", "auto.offset.reset": "earliest", "enable.auto.commit": False})
    assigned = threading.Event()
    c.subscribe([topic], on_assign=lambda _c, _p: assigned.set())
    while not assigned.is_set():
        c.poll(0.05)
    t0 = time.perf_counter()
    got = 0
    while got < n_events and time.perf_counter() - t0 < 15.0:
        msgs = c.consume(num_messages=500, timeout=0.05)
        got += sum(1 for m in msgs if m is not None and not m.error())
    el = time.perf_counter() - t0
    c.close()
    res["kafka"] = {"events": got, "replay_s": round(el, 3), "replay_eps": round(got / max(0.001, el), 1)}

    # 2. NATS JetStream
    stream = "g18_replay_nats"
    prepare_dest("nats", stream)
    pub = UnifiedPublisher("nats", stream)
    for off in range(0, n_events, 250):
        pub.publish_batch(items[off:off + 250])
    pub.close()

    async def _nats_replay():
        import nats
        nc = await nats.connect("nats://127.0.0.1:4222")
        js = nc.jetstream()
        psub = await js.pull_subscribe(f"{stream}.*", durable=f"rep_{uuid.uuid4().hex[:5]}", stream=stream)
        t0_n = time.perf_counter()
        g = 0
        while g < n_events and time.perf_counter() - t0_n < 15.0:
            try:
                msgs = await psub.fetch(500, timeout=0.1)
            except Exception:
                continue
            g += len(msgs)
            for m in msgs:
                await m.ack()
        el_n = time.perf_counter() - t0_n
        await nc.close()
        return g, el_n

    g_n, el_n = run_coro(_nats_replay())
    res["nats_jetstream"] = {"events": g_n, "replay_s": round(el_n, 3), "replay_eps": round(g_n / max(0.001, el_n), 1)}

    # 3. Redis Streams
    rstream = "g18_replay_redis"
    prepare_dest("redis", rstream)
    pub = UnifiedPublisher("redis", rstream)
    for off in range(0, n_events, 500):
        pub.publish_batch(items[off:off + 500])
    pub.close()

    import redis
    r = redis.Redis(host="127.0.0.1", port=6379)
    r.xgroup_create(rstream, "replay_grp", id="0", mkstream=False)
    t0_r = time.perf_counter()
    g_r = 0
    while g_r < n_events and time.perf_counter() - t0_r < 15.0:
        out = r.xreadgroup("replay_grp", "c1", {rstream: ">"}, count=500, block=50)
        if not out:
            continue
        ids = [eid_s for _, entries in out for eid_s, _ in entries]
        g_r += len(ids)
        if ids:
            r.xack(rstream, "replay_grp", *ids)
    el_r = time.perf_counter() - t0_r
    r.close()
    res["redis_streams"] = {"events": g_r, "replay_s": round(el_r, 3), "replay_eps": round(g_r / max(0.001, el_r), 1)}
    res["rabbitmq_and_pg_queue"] = {"supported": False, "reason": "S-fila: consumo remove a mensagem (doc 12 §6)"}
    log(f"G18 replay done: {res}")
    return res


UNSEEN_SEALED = [
    {"id": "V01", "transport": "kafka", "strategy": "outbox-poll", "rate": 50, "poll_ms": 25, "batch_size": 50},
    {"id": "V02", "transport": "kafka", "strategy": "outbox-poll", "rate": 50, "poll_ms": 250, "batch_size": 50},
    {"id": "V03", "transport": "kafka", "strategy": "outbox-poll", "rate": 150, "poll_ms": 250, "batch_size": 100},
    {"id": "V04", "transport": "kafka", "strategy": "outbox-poll", "rate": 450, "poll_ms": 250, "batch_size": 50},
    {"id": "V05", "transport": "kafka", "strategy": "outbox-poll", "rate": 900, "poll_ms": 100, "batch_size": 100},
    {"id": "V06", "transport": "kafka", "strategy": "outbox-cdc", "rate": 450, "poll_ms": None, "batch_size": 100},
    {"id": "V07", "transport": "kafka", "strategy": "outbox-cdc", "rate": 900, "poll_ms": None, "batch_size": 100},
    {"id": "V08", "transport": "redis", "strategy": "outbox-poll", "rate": 50, "poll_ms": 250, "batch_size": 50},
    {"id": "V09", "transport": "redis", "strategy": "outbox-poll", "rate": 450, "poll_ms": 25, "batch_size": 100},
    {"id": "V10", "transport": "redis", "strategy": "outbox-poll", "rate": 900, "poll_ms": 25, "batch_size": 150},
    {"id": "V11", "transport": "rabbitmq", "strategy": "outbox-poll", "rate": 50, "poll_ms": 250, "batch_size": 50},
    {"id": "V12", "transport": "rabbitmq", "strategy": "outbox-cdc", "rate": 50, "poll_ms": None, "batch_size": 50},
]


def main():
    t_all = time.time()
    env = bootstrap_all()
    setup_schema(200)
    out = {"meta": {**env, "platform": platform.platform()}}

    # W2-A: Sealed Predictive Validation (N17 / Pilar N3)
    log("=== W2-A: Sealed Predictive Validation (12 unseen configs V01-V12, R=2) ===")
    w2a = []
    for cfg in UNSEEN_SEALED:
        reps = []
        for b in (1, 2):
            r = run_trial(
                transport=cfg["transport"], strategy=cfg["strategy"], rate=cfg["rate"],
                duration_s=4.0, poll_ms=cfg["poll_ms"] or 100, batch_size=cfg["batch_size"],
                writers=6 if cfg["rate"] >= 450 else 4, seed=100 + b,
            )
            r["id"] = cfg["id"]
            r["rep"] = b
            reps.append(r)
        w2a.extend(reps)
        log(f"  {cfg['id']} {cfg['transport']}/{cfg['strategy']} r={cfg['rate']} p={cfg['poll_ms']}: obs p99={[x['l_e2e_p99_ms'] for x in reps]}")
    out["W2_A_sealed_validation"] = w2a

    # W2-B: Kafka High-Rate Frontier (finding real lambda* — P1)
    log("=== W2-B: Kafka High-Rate Frontier (500 -> 3000 ev/s, finding lambda*) ===")
    w2b = []
    for rate in (500, 1000, 1500, 2200, 3000):
        for strat, p_ms in [("outbox-poll", 10), ("outbox-poll", 100), ("outbox-cdc", 0)]:
            r = run_trial("kafka", strat, rate=rate, duration_s=4.5, poll_ms=p_ms, batch_size=200, writers=8, seed=202)
            w2b.append(r)
            log(f"  W2-B rate={rate} kafka/{strat}(p={p_ms}): e2e_p50={r['l_e2e_p50_ms']}ms e2e_p99={r['l_e2e_p99_ms']}ms cens={r['censored']}")
    out["W2_B_kafka_high_rate"] = w2b

    # W2-C: NATS JetStream + Ablation A3 (fsync on SAME system — P4 / M8)
    log("=== W2-C: Ablation A3 fsync on SAME system (Kafka & NATS JetStream) ===")
    w2c = []
    for rate in (100, 500, 1000):
        for tr in ("kafka", "kafka-fsync", "nats", "nats-fsync"):
            r = run_trial(tr, "outbox-poll", rate=rate, duration_s=4.0, poll_ms=10, batch_size=100, writers=6, seed=303)
            w2c.append(r)
            log(f"  W2-C rate={rate} {tr}: e2e_p50={r['l_e2e_p50_ms']}ms e2e_p99={r['l_e2e_p99_ms']}ms cens={r['censored']}")
    out["W2_C_ablation_A3_fsync"] = w2c

    # W2-D: High-precision R=10 paired blocks (S2: narrow CIs!)
    log("=== W2-D: R=10 Paired Blocks at 100 & 300 ev/s ===")
    w2d = []
    w2d_cfgs = [
        ("kafka", "outbox-cdc", 0),
        ("kafka", "outbox-poll", 10),
        ("kafka", "outbox-poll", 100),
        ("nats", "outbox-cdc", 0),
        ("nats", "outbox-poll", 10),
        ("redis", "outbox-poll", 10),
        ("pg-queue", "pg-queue", 0),
    ]
    for block in range(1, 11):
        for rate in (100, 300):
            for tr, strat, p_ms in w2d_cfgs:
                r = run_trial(tr, strat, rate=rate, duration_s=3.5, poll_ms=p_ms, batch_size=100, writers=4, seed=400 + block)
                r["block"] = block
                w2d.append(r)
        log(f"  W2-D block {block}/10 done")
    out["W2_D_R10_paired"] = w2d

    # W2-E: Production scenarios (G04 payload, G08 fan-out, G18 replay)
    log("=== W2-E: G04 Payload Size (1, 8, 32, 64 KiB @ 100 ev/s) ===")
    g04 = []
    for sz_kb in (1, 8, 32, 64):
        for tr in ("kafka", "nats", "redis", "rabbitmq"):
            r = run_trial(tr, "outbox-poll", rate=100, duration_s=4.0, poll_ms=10, batch_size=50, writers=4, payload_bytes=sz_kb * 1024, seed=505)
            g04.append(r)
            log(f"  G04 {tr} {sz_kb}KiB: e2e_p99={r['l_e2e_p99_ms']}ms cens={r['censored']}")
    out["W2_E_G04_payload"] = g04

    log("=== W2-E: G08 Fan-out (1, 2, 4 consumer groups @ 300 ev/s) ===")
    g08 = []
    for ng in (1, 2, 4):
        for tr in ("kafka", "nats"):
            r = run_trial(tr, "outbox-poll", rate=300, duration_s=4.0, poll_ms=10, batch_size=100, writers=6, n_groups=ng, seed=606)
            g08.append(r)
            log(f"  G08 {tr} groups={ng}: e2e_p99={r['l_e2e_p99_ms']}ms cens={r['censored']}")
    out["W2_E_G08_fanout"] = g08

    out["W2_E_G18_replay"] = run_g18_replay(5000)

    out["meta"]["total_runtime_s"] = round(time.time() - t_all, 1)
    print("===WAVE2_JSON_START===")
    print(json.dumps(out, indent=2))
    print("===WAVE2_JSON_END===")
    log(f"Wave 2 complete in {out['meta']['total_runtime_s']}s")


if __name__ == "__main__":
    main()
