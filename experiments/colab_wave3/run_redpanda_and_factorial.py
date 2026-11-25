#!/usr/bin/env python3
"""Onda 3D (VM paralela isolada) — Fatorial Fracionado N12 (doc 13 §4):
  - Fatorial 2^(4-1) resolucao IV (I = ABCD) sobre os 4 fatores do outbox no Kafka:
      A: poll_ms   in {10, 200}
      B: batch     in {10, 200}
      C: relays k  in {1, 2}
      D: cleanup   in {'mark', 'delete'}  (D = A * B * C)
    em dois niveis de carga: lambda = 100 ev/s (subcritico) e lambda = 600 ev/s (alta taxa).
  - Estima efeitos principais (log-linear) livres de confusao com interacoes de 2 fatores!
"""

import json
import math
import os
import platform
import queue
import struct
import subprocess
import sys
import threading
import time
import uuid
from collections import defaultdict


def log(msg):
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", file=sys.stderr, flush=True)


def sh(cmd, check=True, timeout=240):
    res = subprocess.run(cmd, shell=True, text=True, capture_output=True, timeout=timeout)
    if check and res.returncode != 0:
        log(f"FAIL {cmd}\nSTDERR: {res.stderr[-800:]}")
        raise RuntimeError(cmd)
    return res


PG_DSN = "host=127.0.0.1 port=5432 dbname=study user=postgres password=postgres"
ENV = struct.Struct("!16sIQQ")


def bootstrap():
    t0 = time.time()
    os.environ["DEBIAN_FRONTEND"] = "noninteractive"
    sh("apt-get update -qq && apt-get install -y -qq postgresql openjdk-21-jre-headless curl")
    sh(f"{sys.executable} -m pip install -q psycopg2-binary confluent-kafka numpy")

    pg_ver = sh("ls /etc/postgresql | sort -V | tail -n 1").stdout.strip()
    hba = f"/etc/postgresql/{pg_ver}/main/pg_hba.conf"
    open(hba, "w").write("local all all trust\nhost all all 127.0.0.1/32 trust\n" + open(hba).read())
    sh("service postgresql restart")
    sh("su - postgres -c \"psql -c \\\"ALTER USER postgres WITH PASSWORD 'postgres' SUPERUSER;\\\"\"")
    sh("su - postgres -c \"psql -c \\\"CREATE DATABASE study;\\\"\"", check=False)

    sh("curl -fsSL https://downloads.apache.org/kafka/4.4.0/kafka_2.13-4.4.0.tgz -o /tmp/k.tgz && mkdir -p /opt/kafka && tar -xzf /tmp/k.tgz -C /opt/kafka --strip-components=1 && rm /tmp/k.tgz")
    kp = "/opt/kafka/config/server.properties"
    ls = [l for l in open(kp).read().splitlines() if not l.startswith(("listeners=", "advertised.listeners=", "offsets.topic.replication.factor=", "transaction.state.log.", "group.initial.rebalance.delay.ms=", "num.partitions="))]
    ls += ["offsets.topic.replication.factor=1", "transaction.state.log.replication.factor=1", "transaction.state.log.min.isr=1", "group.initial.rebalance.delay.ms=0", "num.partitions=12", "listeners=PLAINTEXT://127.0.0.1:9092,CONTROLLER://127.0.0.1:9093", "advertised.listeners=PLAINTEXT://127.0.0.1:9092"]
    open(kp, "w").write("\n".join(ls) + "\n")
    cid = sh("/opt/kafka/bin/kafka-storage.sh random-uuid").stdout.strip().splitlines()[-1].strip()
    sh(f"/opt/kafka/bin/kafka-storage.sh format --standalone -t {cid} -c {kp} --ignore-formatted")
    sh("KAFKA_HEAP_OPTS='-Xmx1G -Xms512M' /opt/kafka/bin/kafka-server-start.sh -daemon /opt/kafka/config/server.properties")
    for _ in range(40):
        if sh("/opt/kafka/bin/kafka-topics.sh --bootstrap-server 127.0.0.1:9092 --list", check=False).returncode == 0:
            break
        time.sleep(1)

    import psycopg2
    c = psycopg2.connect(PG_DSN)
    c.autocommit = True
    cur = c.cursor()
    cur.execute("""
        DROP TABLE IF EXISTS outbox, customers CASCADE;
        CREATE TABLE customers (id INT PRIMARY KEY, balance_cents BIGINT NOT NULL, version BIGINT NOT NULL DEFAULT 0);
        CREATE TABLE outbox (id BIGSERIAL PRIMARY KEY, event_id UUID NOT NULL, aggregate_id INT NOT NULL, aggregate_version BIGINT NOT NULL, t_int_us BIGINT NOT NULL, payload BYTEA NOT NULL, published_at TIMESTAMPTZ);
        CREATE INDEX outbox_pend_idx ON outbox (id) WHERE published_at IS NULL;
    """)
    cur.executemany("INSERT INTO customers VALUES (%s, 10000000, 0)", [(i,) for i in range(200)])
    c.close()
    log(f"Wave 3D bootstrap done in {time.time() - t0:.1f}s")


def run_factorial_point(rate, poll_ms, batch_size, k_relays, cleanup):
    from confluent_kafka import Consumer, Producer
    from confluent_kafka.admin import AdminClient, NewTopic
    import numpy as np
    import psycopg2

    c = psycopg2.connect(PG_DSN)
    c.autocommit = True
    c.cursor().execute("TRUNCATE outbox RESTART IDENTITY; UPDATE customers SET balance_cents=10000000, version=0;")
    c.close()

    topic = f"n12_{rate}_{poll_ms}_{batch_size}_{k_relays}_{cleanup}_{uuid.uuid4().hex[:4]}"
    AdminClient({"bootstrap.servers": "127.0.0.1:9092"}).create_topics([NewTopic(topic, num_partitions=12, replication_factor=1)])
    time.sleep(0.3)

    stop = threading.Event()
    assigned = threading.Event()
    lk = threading.Lock()
    l_e2e, l_req = [], []
    exp, deliv = {}, defaultdict(list)

    def cons():
        cs = Consumer({"bootstrap.servers": "127.0.0.1:9092", "group.id": f"g-{uuid.uuid4().hex[:6]}", "auto.offset.reset": "earliest", "fetch.wait.max.ms": 2})
        cs.subscribe([topic], on_assign=lambda _c, _p: assigned.set())
        while not stop.is_set():
            msgs = cs.consume(300, timeout=0.005)
            t_us = int(time.time() * 1e6)
            with lk:
                for m in msgs:
                    if m is None or m.error():
                        continue
                    eb, aid, av, ti = ENV.unpack_from(m.value(), 0)
                    eid = uuid.UUID(bytes=eb)
                    first = len(deliv[eid]) == 0
                    deliv[eid].append((t_us, aid, av))
                    if first:
                        l_e2e.append((t_us - ti) / 1000.0)
        cs.close()

    ct = threading.Thread(target=cons, daemon=True)
    ct.start()
    assigned.wait(10)

    def relay():
        conn = psycopg2.connect(PG_DSN)
        conn.autocommit = False
        cur = conn.cursor()
        prod = Producer({"bootstrap.servers": "127.0.0.1:9092", "acks": "all", "linger.ms": 2, "batch.num.messages": 200})
        sl = poll_ms / 1000.0
        while not stop.is_set():
            cur.execute("SELECT id, event_id, aggregate_id, aggregate_version, t_int_us, payload FROM outbox WHERE published_at IS NULL ORDER BY id LIMIT %s FOR UPDATE SKIP LOCKED", (batch_size,))
            rows = cur.fetchall()
            if not rows:
                conn.commit()
                time.sleep(sl)
                continue
            ids = []
            for r in rows:
                prod.produce(topic, key=str(r[2]).encode(), value=ENV.pack(uuid.UUID(r[1]).bytes, r[2], r[3], r[4]) + bytes(r[5]))
                ids.append(r[0])
            prod.flush(10)
            if cleanup == "delete":
                cur.execute("DELETE FROM outbox WHERE id = ANY(%s)", (ids,))
            else:
                cur.execute("UPDATE outbox SET published_at = clock_timestamp() WHERE id = ANY(%s)", (ids,))
            conn.commit()
        conn.close()

    rts = [threading.Thread(target=relay, daemon=True) for _ in range(k_relays)]
    for t in rts:
        t.start()
    time.sleep(0.15)

    q = queue.Queue(maxsize=20000)
    pl = os.urandom(1024)

    def writer():
        conn = psycopg2.connect(PG_DSN)
        conn.autocommit = False
        cur = conn.cursor()
        while True:
            item = q.get()
            if item is None:
                break
            ti, aid = item
            eid = uuid.uuid4()
            cur.execute("SELECT balance_cents, version FROM customers WHERE id=%s FOR UPDATE", (aid,))
            bal, ver = cur.fetchone()
            nv = ver + 1
            cur.execute("UPDATE customers SET balance_cents=%s, version=%s WHERE id=%s", (bal - 1, nv, aid))
            cur.execute("INSERT INTO outbox (event_id, aggregate_id, aggregate_version, t_int_us, payload) VALUES (%s,%s,%s,%s,%s)", (str(eid), aid, nv, ti, psycopg2.Binary(pl)))
            conn.commit()
            td = int(time.time() * 1e6)
            with lk:
                l_req.append((td - ti) / 1000.0)
                exp[eid] = (aid, nv)
        conn.close()

    wts = [threading.Thread(target=writer, daemon=True) for _ in range(6)]
    for t in wts:
        t.start()

    n_ev = int(rate * 4.5)
    dt = 1.0 / rate
    t0 = time.perf_counter()
    w0 = int(time.time() * 1e6)
    for i in range(n_ev):
        sched = i * dt
        rem = sched - (time.perf_counter() - t0)
        if rem > 0:
            time.sleep(rem)
        q.put((w0 + int(sched * 1e6), i % 200))
    for _ in wts:
        q.put(None)
    for t in wts:
        t.join(20)

    end = time.time() + 4.0
    while time.time() < end:
        with lk:
            if len(deliv) >= len(exp):
                break
        time.sleep(0.03)
    stop.set()
    for t in rts:
        t.join(5)
    ct.join(5)

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
        "rate": rate, "poll_ms": poll_ms, "batch_size": batch_size, "k_relays": k_relays, "cleanup": cleanup,
        "expected": len(exp), "delivered": len(deliv), "o1_violations": o1,
        "l_req_p99_ms": round(float(np.percentile(l_req, 99)), 2),
        "l_e2e_p50_ms": round(float(np.percentile(l_e2e, 50)), 2),
        "l_e2e_p99_ms": round(float(np.percentile(l_e2e, 99)), 2),
    }


def main():
    t0 = time.time()
    bootstrap()
    # 2^(4-1) Resolution IV design: D = A * B * C
    design = [
        (-1, -1, -1, -1),
        (+1, -1, -1, +1),
        (-1, +1, -1, +1),
        (+1, +1, -1, -1),
        (-1, -1, +1, +1),
        (+1, -1, +1, -1),
        (-1, +1, +1, -1),
        (+1, +1, +1, +1),
    ]
    rows = []
    effects = {}
    for rate in (100, 600):
        sub = []
        for a, b, c, d_sign in design:
            p_ms = 10 if a == -1 else 200
            bsz = 10 if b == -1 else 200
            k = 1 if c == -1 else 2
            cln = "mark" if d_sign == -1 else "delete"
            r = run_factorial_point(rate, p_ms, bsz, k, cln)
            r["coded"] = {"A_poll": a, "B_batch": b, "C_relays": c, "D_cleanup": d_sign}
            sub.append(r)
            rows.append(r)
            log(f"  N12 r={rate} p={p_ms} b={bsz} k={k} cln={cln}: e2e_p99={r['l_e2e_p99_ms']}ms O1={r['o1_violations']}")
        # Main effects on log(l_e2e_p99_ms): exp(mean(y_hi) - mean(y_lo)) = multiplicative ratio!
        eff = {}
        for fname in ("A_poll", "B_batch", "C_relays", "D_cleanup"):
            hi = [math.log(x["l_e2e_p99_ms"]) for x in sub if x["coded"][fname] == +1]
            lo = [math.log(x["l_e2e_p99_ms"]) for x in sub if x["coded"][fname] == -1]
            eff[fname] = round(math.exp(sum(hi) / len(hi) - sum(lo) / len(lo)), 3)
        effects[str(rate)] = eff
        log(f"  N12 Main Effects (ratio hi/lo) @ {rate} ev/s: {eff}")

    out = {"meta": {"platform": platform.platform(), "cpus": os.cpu_count(), "runtime_s": round(time.time() - t0, 1)}, "N12_runs": rows, "N12_main_effects_ratio": effects}
    print("===WAVE3D_JSON_START===")
    print(json.dumps(out, indent=2))
    print("===WAVE3D_JSON_END===")


if __name__ == "__main__":
    main()
