#!/usr/bin/env python3
"""Onda 3A (VM paralela isolada) — Lacunas Enterprise (N19, N20, N21):
  - N20: Overhead de TLS (NATS JetStream TLS x plaintext) e Serializacao (Binary x JSON x JSON+zlib)
  - N21: Poison Pill / DLQ & Head-of-Line Blocking sob O1 (Kafka O1 bloqueante x RabbitMQ DLQ)
  - N19: Multi-Tenant / Noisy Neighbor (1, 4, 8 servicos concorrentes no mesmo PostgreSQL)
"""

import asyncio
import json
import os
import platform
import queue
import ssl
import struct
import subprocess
import sys
import threading
import time
import uuid
import zlib
from collections import defaultdict


def log(msg):
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", file=sys.stderr, flush=True)


def run_coro(coro):
    box = {}
    def _r():
        try:
            box["v"] = asyncio.run(coro)
        except Exception as e:
            box["e"] = e
    t = threading.Thread(target=_r, daemon=True)
    t.start()
    t.join()
    if "e" in box:
        raise box["e"]
    return box.get("v")


def sh(cmd, check=True, timeout=240):
    res = subprocess.run(cmd, shell=True, text=True, capture_output=True, timeout=timeout)
    if check and res.returncode != 0:
        log(f"FAIL {cmd}\nSTDERR: {res.stderr[-800:]}")
        raise RuntimeError(cmd)
    return res


PG_DSN = "host=127.0.0.1 port=5432 dbname=study user=postgres password=postgres"


def bootstrap():
    t0 = time.time()
    os.environ["DEBIAN_FRONTEND"] = "noninteractive"
    sh("apt-get update -qq && apt-get install -y -qq postgresql rabbitmq-server openjdk-21-jre-headless curl openssl")
    sh(f"{sys.executable} -m pip install -q psycopg2-binary confluent-kafka pika nats-py numpy")

    pg_ver = sh("ls /etc/postgresql | sort -V | tail -n 1").stdout.strip()
    with open(f"/etc/postgresql/{pg_ver}/main/postgresql.conf", "a") as f:
        f.write("\nwal_level = logical\nmax_replication_slots = 16\nmax_wal_senders = 16\nmax_connections = 250\nsynchronous_commit = on\nshared_buffers = 512MB\n")
    hba = f"/etc/postgresql/{pg_ver}/main/pg_hba.conf"
    open(hba, "w").write("local all all trust\nhost all all 127.0.0.1/32 trust\nlocal replication all trust\nhost replication all 127.0.0.1/32 trust\n" + open(hba).read())
    sh("service postgresql restart")
    sh("su - postgres -c \"psql -c \\\"ALTER USER postgres WITH PASSWORD 'postgres' SUPERUSER REPLICATION;\\\"\"")
    sh("su - postgres -c \"psql -c \\\"CREATE DATABASE study;\\\"\"", check=False)

    sh("service rabbitmq-server start")
    for _ in range(30):
        if sh("rabbitmqctl status", check=False).returncode == 0:
            break
        time.sleep(1)

    # NATS JetStream: plaintext (4222) and TLS (4224) with self-signed cert
    sh(
        "curl -fsSL https://github.com/nats-io/nats-server/releases/download/v2.10.22/nats-server-v2.10.22-linux-amd64.tar.gz -o /tmp/n.tgz "
        "&& tar -xzf /tmp/n.tgz -C /tmp && mv /tmp/nats-server-v2.10.22-linux-amd64/nats-server /usr/local/bin/nats-server "
        "&& chmod +x /usr/local/bin/nats-server && rm -rf /tmp/n.tgz /tmp/nats-server-*"
    )
    sh("openssl req -x509 -newkey rsa:2048 -keyout /tmp/nats.key -out /tmp/nats.crt -days 1 -nodes -subj '/CN=127.0.0.1' -addext 'subjectAltName=IP:127.0.0.1'")
    open("/tmp/nats_plain.conf", "w").write("port: 4222\njetstream { store_dir: \"/tmp/n_plain\" }\n")
    open("/tmp/nats_tls.conf", "w").write("port: 4224\njetstream { store_dir: \"/tmp/n_tls\" }\ntls { cert_file: \"/tmp/nats.crt\", key_file: \"/tmp/nats.key\" }\n")
    sh("nats-server -c /tmp/nats_plain.conf > /tmp/np.log 2>&1 &")
    sh("nats-server -c /tmp/nats_tls.conf > /tmp/nt.log 2>&1 &")
    time.sleep(1.5)

    # Kafka 3.9 KRaft
    sh("curl -fsSL https://archive.apache.org/dist/kafka/3.9.0/kafka_2.13-3.9.0.tgz -o /tmp/k.tgz && mkdir -p /opt/kafka && tar -xzf /tmp/k.tgz -C /opt/kafka --strip-components=1 && rm /tmp/k.tgz")
    kp = "/opt/kafka/config/kraft/server.properties"
    ls = [l for l in open(kp).read().splitlines() if not l.startswith(("listeners=", "advertised.listeners=", "offsets.topic.replication.factor=", "transaction.state.log.", "group.initial.rebalance.delay.ms=", "num.partitions="))]
    ls += ["offsets.topic.replication.factor=1", "transaction.state.log.replication.factor=1", "transaction.state.log.min.isr=1", "group.initial.rebalance.delay.ms=0", "num.partitions=6", "listeners=PLAINTEXT://127.0.0.1:9092,CONTROLLER://127.0.0.1:9093", "advertised.listeners=PLAINTEXT://127.0.0.1:9092"]
    open(kp, "w").write("\n".join(ls) + "\n")
    cid = sh("/opt/kafka/bin/kafka-storage.sh random-uuid").stdout.strip().splitlines()[-1].strip()
    sh(f"/opt/kafka/bin/kafka-storage.sh format -t {cid} -c {kp} --ignore-formatted")
    sh("KAFKA_HEAP_OPTS='-Xmx1G -Xms512M' /opt/kafka/bin/kafka-server-start.sh -daemon /opt/kafka/config/kraft/server.properties")
    for _ in range(40):
        if sh("/opt/kafka/bin/kafka-topics.sh --bootstrap-server 127.0.0.1:9092 --list", check=False).returncode == 0:
            break
        time.sleep(1)

    log(f"Wave 3A bootstrap done in {time.time() - t0:.1f}s")


def run_n20_tls_and_serialization():
    """N20: Measure TLS overhead (NATS plain vs TLS) and serialization (Binary vs JSON vs JSON+zlib)."""
    import numpy as np
    log("=== N20: TLS & Serialization Overhead ===")
    results = []

    sample_event = {
        "event_id": str(uuid.uuid4()),
        "aggregate_id": 42,
        "aggregate_version": 1001,
        "event_type": "OrderConfirmed",
        "customer_tier": "enterprise",
        "items": [{"sku": f"SKU-{i:04d}", "qty": i + 1, "price_cents": 1990 * (i + 1)} for i in range(12)],
        "metadata": {"trace_id": uuid.uuid4().hex, "region": "sa-east-1", "service": "billing-api"},
    }
    raw_json = json.dumps(sample_event).encode()
    raw_bin = struct.pack("!16sIQ", uuid.uuid4().bytes, 42, 1001) + raw_json[:256]

    def encode(fmt):
        if fmt == "binary":
            return struct.pack("!16sIQ", uuid.uuid4().bytes, 42, 1001) + b"x" * (len(raw_json) - 28)
        if fmt == "json":
            return json.dumps(sample_event).encode()
        if fmt == "json_zlib":
            return zlib.compress(json.dumps(sample_event).encode(), level=6)

    for use_tls in (False, True):
        for fmt in ("binary", "json", "json_zlib"):
            async def _bench():
                import nats
                url = "nats://127.0.0.1:4224" if use_tls else "nats://127.0.0.1:4222"
                kw = {}
                if use_tls:
                    ctx = ssl.create_default_context(cafile="/tmp/nats.crt")
                    kw["tls"] = ctx
                nc = await nats.connect(url, **kw)
                js = nc.jetstream()
                sname = f"n20_{'tls' if use_tls else 'plain'}_{fmt}"
                try:
                    await js.delete_stream(sname)
                except Exception:
                    pass
                await js.add_stream(name=sname, subjects=[f"{sname}.*"])
                n_ev = 1000
                lats = []
                total_bytes = 0
                t_start = time.perf_counter()
                for i in range(n_ev):
                    t0 = time.perf_counter()
                    body = encode(fmt)
                    total_bytes += len(body)
                    await js.publish(f"{sname}.1", body)
                    lats.append((time.perf_counter() - t0) * 1000.0)
                el = time.perf_counter() - t_start
                await nc.close()
                return {
                    "tls": use_tls,
                    "format": fmt,
                    "bytes_per_event": round(total_bytes / n_ev, 1),
                    "pub_p50_ms": round(float(np.percentile(lats, 50)), 3),
                    "pub_p99_ms": round(float(np.percentile(lats, 99)), 3),
                    "throughput_eps": round(n_ev / el, 1),
                }
            r = run_coro(_bench())
            results.append(r)
            log(f"  N20 tls={use_tls} fmt={fmt:<9}: bytes={r['bytes_per_event']} p50={r['pub_p50_ms']}ms p99={r['pub_p99_ms']}ms tps={r['throughput_eps']}")
    return results


def run_n21_poison_pill_hol():
    """N21: 2% poison pills with 3x 40ms retry — Kafka O1 strict (HOL blocking) vs RabbitMQ DLQ skip."""
    from confluent_kafka import Consumer, Producer
    from confluent_kafka.admin import AdminClient, NewTopic
    import numpy as np
    import pika

    log("=== N21: Poison Pill / Head-of-Line Blocking (Kafka O1 vs RabbitMQ DLQ) ===")
    n_events = 300
    poison_set = {i for i in range(n_events) if i % 50 == 10}  # 6 poison pills (2%)

    # 1. Kafka single partition (strict O1): consumer retries 3x40ms synchronously before DLQ -> blocks healthy events behind it!
    topic = "n21_kafka_hol"
    AdminClient({"bootstrap.servers": "127.0.0.1:9092"}).create_topics([NewTopic(topic, num_partitions=1, replication_factor=1)])
    time.sleep(0.5)
    p = Producer({"bootstrap.servers": "127.0.0.1:9092", "acks": "all"})
    t0_k = time.perf_counter()
    for i in range(n_events):
        p.produce(topic, value=json.dumps({"i": i, "t0": time.perf_counter() - t0_k}).encode())
    p.flush()

    c = Consumer({"bootstrap.servers": "127.0.0.1:9092", "group.id": f"n21-{uuid.uuid4().hex[:5]}", "auto.offset.reset": "earliest"})
    assigned = threading.Event()
    c.subscribe([topic], on_assign=lambda _c, _p: assigned.set())
    while not assigned.is_set():
        c.poll(0.05)

    healthy_lats_k = []
    dlq_k = 0
    seen = 0
    while seen < n_events:
        m = c.poll(0.2)
        if m is None or m.error():
            continue
        ev = json.loads(m.value())
        seen += 1
        if ev["i"] in poison_set:
            # Strict O1: must retry in-place before advancing partition offset!
            for _ in range(3):
                time.sleep(0.04)
            dlq_k += 1
        else:
            time.sleep(0.0005)
            healthy_lats_k.append(((time.perf_counter() - t0_k) - ev["t0"]) * 1000.0)
    c.close()

    # 2. RabbitMQ with 4 concurrent consumers + immediate NACK->DLQ (no strict O1 HOL blocking)
    conn = pika.BlockingConnection(pika.ConnectionParameters("127.0.0.1"))
    ch = conn.channel()
    ch.queue_delete("n21_rmq")
    ch.queue_delete("n21_rmq_dlq")
    ch.queue_declare("n21_rmq_dlq", durable=True)
    ch.queue_declare("n21_rmq", durable=True, arguments={"x-dead-letter-exchange": "", "x-dead-letter-routing-key": "n21_rmq_dlq"})
    t0_r = time.perf_counter()
    for i in range(n_events):
        ch.basic_publish("", "n21_rmq", json.dumps({"i": i, "t0": time.perf_counter() - t0_r}).encode())
    conn.close()

    healthy_lats_r = []
    dlq_r = [0]
    lk = threading.Lock()
    done_r = [0]

    def rmq_worker():
        cn = pika.BlockingConnection(pika.ConnectionParameters("127.0.0.1"))
        cch = cn.channel()
        cch.basic_qos(prefetch_count=10)
        while True:
            with lk:
                if done_r[0] >= n_events:
                    break
            m, _, body = cch.basic_get("n21_rmq", auto_ack=False)
            if m is None:
                time.sleep(0.002)
                continue
            ev = json.loads(body)
            if ev["i"] in poison_set:
                # Immediately reject to DLQ (async retry outside hot path)
                cch.basic_nack(m.delivery_tag, requeue=False)
                with lk:
                    dlq_r[0] += 1
                    done_r[0] += 1
            else:
                time.sleep(0.0005)
                cch.basic_ack(m.delivery_tag)
                with lk:
                    healthy_lats_r.append(((time.perf_counter() - t0_r) - ev["t0"]) * 1000.0)
                    done_r[0] += 1
        cn.close()

    ws = [threading.Thread(target=rmq_worker, daemon=True) for _ in range(4)]
    for w in ws:
        w.start()
    for w in ws:
        w.join(timeout=15)

    res = {
        "poison_pills": len(poison_set),
        "kafka_strict_O1_hol": {
            "healthy_p50_ms": round(float(np.percentile(healthy_lats_k, 50)), 1),
            "healthy_p99_ms": round(float(np.percentile(healthy_lats_k, 99)), 1),
            "dlq_count": dlq_k,
        },
        "rabbitmq_dlq_parallel": {
            "healthy_p50_ms": round(float(np.percentile(healthy_lats_r, 50)), 1),
            "healthy_p99_ms": round(float(np.percentile(healthy_lats_r, 99)), 1),
            "dlq_count": dlq_r[0],
        },
    }
    log(f"  N21 done: {res}")
    return res


def run_n19_multitenant_noisy_neighbor():
    """N19: 1, 4, 8 independent microservices sharing the same PostgreSQL outbox + WAL."""
    import numpy as np
    import psycopg2

    log("=== N19: Multi-Tenant / Noisy Neighbor (1, 4, 8 services on SAME PostgreSQL) ===")
    conn = psycopg2.connect(PG_DSN)
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute("DROP TABLE IF EXISTS mt_outbox CASCADE; CREATE TABLE mt_outbox (id BIGSERIAL PRIMARY KEY, svc INT NOT NULL, payload BYTEA NOT NULL, created_at TIMESTAMPTZ DEFAULT now());")
    conn.close()

    rows = []
    for n_svc in (1, 4, 8):
        conn = psycopg2.connect(PG_DSN)
        conn.autocommit = True
        conn.cursor().execute("TRUNCATE mt_outbox RESTART IDENTITY;")
        conn.close()

        lats_target = []
        lk = threading.Lock()
        stop_at = time.perf_counter() + 6.0
        totals = [0] * n_svc

        def svc_worker(sid):
            c = psycopg2.connect(PG_DSN)
            c.autocommit = False
            cr = c.cursor()
            pl = os.urandom(1024)
            cnt = 0
            while time.perf_counter() < stop_at:
                t0 = time.perf_counter()
                cr.execute("INSERT INTO mt_outbox (svc, payload) VALUES (%s, %s)", (sid, psycopg2.Binary(pl)))
                c.commit()
                dt_ms = (time.perf_counter() - t0) * 1000.0
                cnt += 1
                if sid == 0:
                    with lk:
                        lats_target.append(dt_ms)
                # Target service runs at ~100 ev/s; noisy neighbors run full-tilt!
                if sid == 0:
                    rem = 0.01 - (dt_ms / 1000.0)
                    if rem > 0:
                        time.sleep(rem)
            totals[sid] = cnt
            c.close()

        ts = [threading.Thread(target=svc_worker, args=(i,), daemon=True) for i in range(n_svc)]
        for t in ts:
            t.start()
        for t in ts:
            t.join(timeout=15)

        r = {
            "services": n_svc,
            "target_svc_tx_p50_ms": round(float(np.percentile(lats_target, 50)), 2),
            "target_svc_tx_p99_ms": round(float(np.percentile(lats_target, 99)), 2),
            "total_cluster_tps": round(sum(totals) / 6.0, 1),
        }
        rows.append(r)
        log(f"  N19 services={n_svc}: target_p50={r['target_svc_tx_p50_ms']}ms target_p99={r['target_svc_tx_p99_ms']}ms cluster_tps={r['total_cluster_tps']}")
    return rows


def main():
    t0 = time.time()
    bootstrap()
    out = {
        "meta": {"platform": platform.platform(), "cpus": os.cpu_count()},
        "N20_tls_serialization": run_n20_tls_and_serialization(),
        "N21_poison_pill_hol": run_n21_poison_pill_hol(),
        "N19_multitenant_noisy_neighbor": run_n19_multitenant_noisy_neighbor(),
    }
    out["meta"]["runtime_s"] = round(time.time() - t0, 1)
    print("===WAVE3A_JSON_START===")
    print(json.dumps(out, indent=2))
    print("===WAVE3A_JSON_END===")


if __name__ == "__main__":
    main()
