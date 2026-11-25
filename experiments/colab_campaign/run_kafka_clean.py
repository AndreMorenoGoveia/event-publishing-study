#!/usr/bin/env python3
"""Rodada rapida focada em Kafka (sem artefato de group.initial.rebalance.delay.ms=3000).

Corrige o warmup do consumidor (group.initial.rebalance.delay.ms=0 + aguarda on_assign antes
de iniciar produtores) para coletar:
  - C1_kafka: fencing P6 com leitura limpa de [1, 2] em read_committed
  - C2_kafka: fronteira R=3 blocos para lambda in {25, 100, 300, 600} ev/s
    (outbox-poll 10ms/100ms, outbox-notify, outbox-cdc)
"""

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


def sh(cmd, check=True, timeout=180):
    res = subprocess.run(cmd, shell=True, text=True, capture_output=True, timeout=timeout)
    if check and res.returncode != 0:
        log(f"FAIL {cmd}\nSTDERR: {res.stderr[-1000:]}")
        raise RuntimeError(cmd)
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


def bootstrap():
    os.environ["DEBIAN_FRONTEND"] = "noninteractive"
    sh("apt-get update -qq && apt-get install -y -qq postgresql postgresql-contrib openjdk-21-jre-headless curl")
    sh(f"{sys.executable} -m pip install -q psycopg2-binary confluent-kafka numpy")

    pg_ver = sh("ls /etc/postgresql | sort -V | tail -n 1").stdout.strip()
    with open(f"/etc/postgresql/{pg_ver}/main/postgresql.conf", "a") as f:
        f.write("\nwal_level = logical\nmax_replication_slots = 8\nmax_wal_senders = 8\nmax_connections = 200\nsynchronous_commit = on\nshared_buffers = 512MB\n")
    hba_path = f"/etc/postgresql/{pg_ver}/main/pg_hba.conf"
    with open(hba_path, "r") as f:
        old = f.read()
    with open(hba_path, "w") as f:
        f.write("local all all trust\nhost all all 127.0.0.1/32 trust\nlocal replication all trust\nhost replication all 127.0.0.1/32 trust\n" + old)
    sh("service postgresql restart")
    sh("su - postgres -c \"psql -c \\\"ALTER USER postgres WITH PASSWORD 'postgres' SUPERUSER REPLICATION;\\\"\"")
    sh("su - postgres -c \"psql -c \\\"CREATE DATABASE study;\\\"\"", check=False)

    kafka_dir = "/opt/kafka"
    sh("curl -fsSL https://archive.apache.org/dist/kafka/3.9.0/kafka_2.13-3.9.0.tgz -o /tmp/k.tgz && mkdir -p /opt/kafka && tar -xzf /tmp/k.tgz -C /opt/kafka --strip-components=1 && rm /tmp/k.tgz", timeout=240)
    kp = f"{kafka_dir}/config/kraft/server.properties"
    lines = [
        l for l in open(kp).read().splitlines()
        if not l.startswith(("listeners=", "advertised.listeners=", "offsets.topic.replication.factor=", "transaction.state.log.", "num.partitions=", "group.initial.rebalance.delay.ms="))
    ]
    lines.extend([
        "offsets.topic.replication.factor=1",
        "transaction.state.log.replication.factor=1",
        "transaction.state.log.min.isr=1",
        "group.initial.rebalance.delay.ms=0",
        "num.partitions=12",
        "listeners=PLAINTEXT://127.0.0.1:9092,CONTROLLER://127.0.0.1:9093",
        "advertised.listeners=PLAINTEXT://127.0.0.1:9092",
    ])
    open(kp, "w").write("\n".join(lines) + "\n")
    cid = sh(f"{kafka_dir}/bin/kafka-storage.sh random-uuid").stdout.strip().splitlines()[-1].strip()
    sh(f"{kafka_dir}/bin/kafka-storage.sh format -t {cid} -c {kp} --ignore-formatted")
    sh(f"KAFKA_HEAP_OPTS='-Xmx1G -Xms512M' {kafka_dir}/bin/kafka-server-start.sh -daemon {kp}")
    for _ in range(40):
        if sh(f"{kafka_dir}/bin/kafka-topics.sh --bootstrap-server 127.0.0.1:9092 --list", check=False).returncode == 0:
            break
        time.sleep(1)
    log("Bootstrap done (PG + Kafka with group.initial.rebalance.delay.ms=0)")


def setup_pg():
    import psycopg2
    conn = psycopg2.connect(PG_DSN)
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute("""
        DROP TABLE IF EXISTS outbox, orders, customers, outbox_relay_lease CASCADE;
        CREATE TABLE customers (id INT PRIMARY KEY, balance_cents BIGINT NOT NULL, version BIGINT NOT NULL DEFAULT 0);
        CREATE TABLE orders (id UUID PRIMARY KEY, customer_id INT NOT NULL, aggregate_version BIGINT NOT NULL, event_id UUID NOT NULL);
        CREATE TABLE outbox (
          id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
          event_id UUID NOT NULL, aggregate_id INT NOT NULL, aggregate_version BIGINT NOT NULL,
          partition_key INT NOT NULL, t_int_us BIGINT NOT NULL, payload BYTEA NOT NULL, published_at TIMESTAMPTZ
        );
        CREATE INDEX outbox_pending_idx ON outbox (id) WHERE published_at IS NULL;
        CREATE TABLE outbox_relay_lease (partition_key INT PRIMARY KEY, owner TEXT NOT NULL, epoch BIGINT NOT NULL, expires_at TIMESTAMPTZ NOT NULL);
    """)
    cur.executemany("INSERT INTO customers VALUES (%s, 10000000, 0)", [(i,) for i in range(200)])
    cur.executemany("INSERT INTO outbox_relay_lease VALUES (%s, 'none', 0, now() - interval '1h')", [(p,) for p in range(16)])
    conn.close()


def reset_pg():
    import psycopg2
    conn = psycopg2.connect(PG_DSN)
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute("TRUNCATE TABLE outbox, orders RESTART IDENTITY; UPDATE customers SET balance_cents=10000000, version=0;")
    cur.execute("SELECT slot_name FROM pg_replication_slots WHERE slot_name='study_cdc_slot'")
    if cur.fetchone():
        cur.execute("SELECT pg_drop_replication_slot('study_cdc_slot')")
    conn.close()


def make_topic(topic, parts=12):
    from confluent_kafka.admin import AdminClient, NewTopic
    admin = AdminClient({"bootstrap.servers": "127.0.0.1:9092"})
    fs = admin.create_topics([NewTopic(topic, num_partitions=parts, replication_factor=1)])
    for f in fs.values():
        try:
            f.result(timeout=10)
        except Exception:
            pass


def run_c1_kafka():
    from confluent_kafka import Consumer, KafkaException, Producer
    import psycopg2
    topic = "c1_fencing_clean"
    make_topic(topic, parts=1)
    reset_pg()
    conn = psycopg2.connect(PG_DSN)
    cur = conn.cursor()
    e1, e2 = uuid.uuid4(), uuid.uuid4()
    t_us = int(time.time() * 1e6)
    cur.execute("INSERT INTO outbox (event_id, aggregate_id, aggregate_version, partition_key, t_int_us, payload) VALUES (%s,1,1,1,%s,%s),(%s,1,2,1,%s,%s)", (str(e1), t_us, b"v1", str(e2), t_us, b"v2"))
    conn.commit()
    cur.execute("SELECT id, event_id, aggregate_id, aggregate_version, t_int_us, payload FROM outbox ORDER BY id")
    rows = cur.fetchall()
    r1_row = rows[0]

    p1 = Producer({"bootstrap.servers": "127.0.0.1:9092", "transactional.id": "relay-p1-clean", "acks": "all"})
    p1.init_transactions()
    p1.begin_transaction()

    p2 = Producer({"bootstrap.servers": "127.0.0.1:9092", "transactional.id": "relay-p1-clean", "acks": "all"})
    p2.init_transactions()
    p2.begin_transaction()
    for r in rows:
        p2.produce(topic, key=b"1", value=pack_env(uuid.UUID(r[1]), r[2], r[3], r[4], bytes(r[5])))
    p2.commit_transaction()

    fenced = False
    err = None
    try:
        p1.produce(topic, key=b"1", value=pack_env(uuid.UUID(r1_row[1]), r1_row[2], r1_row[3], r1_row[4], bytes(r1_row[5])))
        p1.commit_transaction()
    except KafkaException as ex:
        fenced = True
        err = str(ex)

    assigned = threading.Event()
    c = Consumer({"bootstrap.servers": "127.0.0.1:9092", "group.id": f"c1-{uuid.uuid4().hex[:6]}", "auto.offset.reset": "earliest", "isolation.level": "read_committed"})
    c.subscribe([topic], on_assign=lambda cons, parts: assigned.set())
    versions = []
    deadline = time.time() + 6.0
    while time.time() < deadline:
        m = c.poll(0.1)
        if m is not None and not m.error():
            _, _, v, _ = unpack_env(m.value())
            versions.append(v)
        elif assigned.is_set() and len(versions) >= 2:
            break
    c.close()
    conn.close()
    res = {"versions_delivered": versions, "zombie_fenced_by_broker": fenced, "broker_error": err, "o1_inversions": sum(1 for i in range(1, len(versions)) if versions[i] < versions[i-1])}
    log(f"C1 clean Kafka: {res}")
    return res


def run_kafka_trial(strategy, rate, poll_ms=100, block=1):
    from confluent_kafka import Consumer, Producer
    import numpy as np
    import psycopg2
    import psycopg2.extras

    reset_pg()
    topic = f"kc_{strategy}_{rate}_b{block}_{uuid.uuid4().hex[:5]}"
    make_topic(topic, parts=12)
    if strategy == "outbox-cdc":
        c = psycopg2.connect(PG_DSN)
        c.autocommit = True
        c.cursor().execute("SELECT pg_create_logical_replication_slot('study_cdc_slot', 'test_decoding')")
        c.close()

    stop_ev = threading.Event()
    assigned_ev = threading.Event()
    l_req, l_e2e = [], []
    exp, deliv = {}, defaultdict(list)
    lk = threading.Lock()

    def cons():
        c = Consumer({
            "bootstrap.servers": "127.0.0.1:9092",
            "group.id": f"g-{uuid.uuid4().hex[:8]}",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
            "fetch.wait.max.ms": 2,
        })
        c.subscribe([topic], on_assign=lambda _c, _p: assigned_ev.set())
        while not stop_ev.is_set():
            msgs = c.consume(num_messages=200, timeout=0.005)
            if not msgs:
                continue
            t_us = int(time.time() * 1e6)
            with lk:
                for m in msgs:
                    if m is None or m.error():
                        continue
                    eid, aid, av, ti = unpack_env(m.value())
                    first = len(deliv[eid]) == 0
                    deliv[eid].append((t_us, aid, av))
                    if first:
                        l_e2e.append((t_us - ti) / 1000.0)
            c.commit(asynchronous=True)
        c.close()

    ct = threading.Thread(target=cons, daemon=True)
    ct.start()
    assigned_ev.wait(timeout=10.0)  # WAIT UNTIL CONSUMER PARTITION ASSIGNMENT IS COMPLETE!

    def relay():
        prod = Producer({"bootstrap.servers": "127.0.0.1:9092", "acks": "all", "linger.ms": 2, "batch.num.messages": 200, "enable.idempotence": True})
        if strategy == "outbox-cdc":
            rc = psycopg2.connect(PG_DSN, connection_factory=psycopg2.extras.LogicalReplicationConnection)
            rcur = rc.cursor()
            rcur.start_replication(slot_name="study_cdc_slot", decode=True)
            while not stop_ev.is_set():
                batch, last = [], None
                while len(batch) < 100:
                    msg = rcur.read_message()
                    if msg is None:
                        break
                    last = msg
                    m = CDC_INSERT_RE.match(msg.payload)
                    if m:
                        batch.append((int(m.group(2)), pack_env(uuid.UUID(m.group(1)), int(m.group(2)), int(m.group(3)), int(m.group(4)), bytes.fromhex(m.group(5)))))
                if batch:
                    for aid, body in batch:
                        prod.produce(topic, key=str(aid).encode(), value=body)
                    prod.flush(timeout=10.0)
                if last:
                    last.cursor.send_feedback(flush_lsn=last.data_start)
                else:
                    select.select([rcur], [], [], 0.002)
            rc.close()
        else:
            conn = psycopg2.connect(PG_DSN)
            conn.autocommit = False
            cur = conn.cursor()
            lconn = None
            if strategy == "outbox-notify":
                lconn = psycopg2.connect(PG_DSN)
                lconn.autocommit = True
                lconn.cursor().execute("LISTEN outbox_ch;")
            while not stop_ev.is_set():
                cur.execute("SELECT id, event_id, aggregate_id, aggregate_version, t_int_us, payload FROM outbox WHERE published_at IS NULL ORDER BY id LIMIT 100 FOR UPDATE SKIP LOCKED")
                rows = cur.fetchall()
                if not rows:
                    conn.commit()
                    if strategy == "outbox-notify":
                        if select.select([lconn], [], [], 0.2) != ([], [], []):
                            lconn.poll()
                            lconn.notifies.clear()
                    else:
                        time.sleep(poll_ms / 1000.0)
                    continue
                for r in rows:
                    prod.produce(topic, key=str(r[2]).encode(), value=pack_env(uuid.UUID(r[1]), r[2], r[3], r[4], bytes(r[5])))
                prod.flush(timeout=10.0)
                cur.execute("UPDATE outbox SET published_at=clock_timestamp() WHERE id=ANY(%s)", ([r[0] for r in rows],))
                conn.commit()
            if lconn:
                lconn.close()
            conn.close()
        prod.flush(timeout=5.0)

    rt = threading.Thread(target=relay, daemon=True)
    rt.start()
    time.sleep(0.15)

    rng = np.random.default_rng(42 + block)
    ranks = np.arange(1, 201, dtype=np.float64)
    probs = (1.0 / ranks) / (1.0 / ranks).sum()
    payloads = [os.urandom(sz) for sz in (512, 1024, 2048, 4096)]
    q = queue.Queue(maxsize=20000)

    def writer():
        conn = psycopg2.connect(PG_DSN)
        conn.autocommit = False
        cur = conn.cursor()
        while True:
            item = q.get()
            if item is None:
                break
            ti, aid, pl = item
            eid, oid = uuid.uuid4(), uuid.uuid4()
            cur.execute("SELECT balance_cents, version FROM customers WHERE id=%s FOR UPDATE", (aid,))
            bal, ver = cur.fetchone()
            nv = ver + 1
            cur.execute("UPDATE customers SET balance_cents=%s, version=%s WHERE id=%s", (bal - 1, nv, aid))
            cur.execute("INSERT INTO orders VALUES (%s,%s,%s,%s)", (str(oid), aid, nv, str(eid)))
            cur.execute("INSERT INTO outbox (event_id, aggregate_id, aggregate_version, partition_key, t_int_us, payload) VALUES (%s,%s,%s,%s,%s,%s)", (str(eid), aid, nv, aid % 16, ti, psycopg2.Binary(pl)))
            if strategy == "outbox-notify":
                cur.execute("NOTIFY outbox_ch, '1'")
            conn.commit()
            td = int(time.time() * 1e6)
            with lk:
                l_req.append((td - ti) / 1000.0)
                exp[eid] = (aid, nv)
        conn.close()

    wts = [threading.Thread(target=writer, daemon=True) for _ in range(4)]
    for t in wts:
        t.start()

    dur = 5.0
    n = int(rate * dur)
    dt = 1.0 / rate
    t0 = time.perf_counter()
    w0 = int(time.time() * 1e6)
    for i in range(n):
        sched = i * dt
        now = time.perf_counter() - t0
        if sched > now:
            time.sleep(sched - now)
        q.put((w0 + int(sched * 1e6), int(rng.choice(200, p=probs)), payloads[i % 4]))
    for _ in wts:
        q.put(None)
    for t in wts:
        t.join(timeout=20)

    end = time.time() + 4.0
    while time.time() < end:
        with lk:
            if len(deliv) >= len(exp):
                break
        time.sleep(0.03)
    stop_ev.set()
    rt.join(timeout=5)
    ct.join(timeout=5)

    def pct(a, p):
        return round(float(np.percentile(a, p)), 3) if a else None

    o1 = 0
    by_a = defaultdict(list)
    for eid, recs in deliv.items():
        by_a[recs[0][1]].append((recs[0][0], recs[0][2]))
    for seq in by_a.values():
        seq.sort(key=lambda x: x[0])
        for j in range(1, len(seq)):
            if seq[j][1] < seq[j - 1][1]:
                o1 += 1

    return {
        "block": block, "transport": "kafka", "strategy": strategy, "poll_ms": poll_ms if strategy == "outbox-poll" else None,
        "rate": rate, "expected": len(exp), "delivered": len(deliv), "censored": max(0, len(exp) - len(deliv)),
        "order_violations_O1": o1, "l_req_p99_ms": pct(l_req, 99), "l_e2e_p50_ms": pct(l_e2e, 50), "l_e2e_p99_ms": pct(l_e2e, 99),
    }


def main():
    t0 = time.time()
    bootstrap()
    setup_pg()
    out = {"C1_kafka_clean": run_c1_kafka(), "C2_kafka_clean": []}
    for b in (1, 2, 3):
        for rate in (25, 100, 300, 600):
            for strat, p in [("outbox-poll", 10), ("outbox-poll", 100), ("outbox-notify", 0), ("outbox-cdc", 0)]:
                r = run_kafka_trial(strat, rate, p, b)
                out["C2_kafka_clean"].append(r)
                log(f"  b={b} rate={rate} kafka/{strat}(p={p}): e2e_p50={r['l_e2e_p50_ms']}ms e2e_p99={r['l_e2e_p99_ms']}ms req_p99={r['l_req_p99_ms']}ms cens={r['censored']}")
    out["runtime_s"] = round(time.time() - t0, 1)
    print("===KAFKA_CLEAN_JSON_START===")
    print(json.dumps(out, indent=2))
    print("===KAFKA_CLEAN_JSON_END===")


if __name__ == "__main__":
    main()
