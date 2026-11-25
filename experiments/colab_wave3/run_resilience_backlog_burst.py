#!/usr/bin/env python3
"""Onda 3C (VM paralela isolada) — Resiliencia, Rajada, Backlog, Rede e Deduplicacao:
  - G11: Rajada 5x (100 -> 500 -> 100 ev/s) em Kafka, NATS JetStream, Redis, RabbitMQ
  - G12: Backlog / Consumidor parado (5.000 eventos acumulados -> tempo e taxa de drenagem)
  - G15: Degradacao de rede (+10 ms e +30 ms de latencia injetada no enlace) -> amplificacao de latencia (C5)
  - G19: Custo de deduplicacao no consumidor (none x dedup-memory x dedup-redis x dedup-pg)
"""

import asyncio
import json
import os
import platform
import socket
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
ENV = struct.Struct("!16sQ")


def bootstrap():
    t0 = time.time()
    os.environ["DEBIAN_FRONTEND"] = "noninteractive"
    sh("apt-get update -qq && apt-get install -y -qq postgresql rabbitmq-server redis-server openjdk-21-jre-headless curl")
    sh(f"{sys.executable} -m pip install -q psycopg2-binary confluent-kafka pika redis nats-py numpy")

    pg_ver = sh("ls /etc/postgresql | sort -V | tail -n 1").stdout.strip()
    hba = f"/etc/postgresql/{pg_ver}/main/pg_hba.conf"
    open(hba, "w").write("local all all trust\nhost all all 127.0.0.1/32 trust\n" + open(hba).read())
    sh("service postgresql restart")
    sh("su - postgres -c \"psql -c \\\"ALTER USER postgres WITH PASSWORD 'postgres' SUPERUSER;\\\"\"")
    sh("su - postgres -c \"psql -c \\\"CREATE DATABASE study;\\\"\"", check=False)

    sh("service rabbitmq-server start")
    for _ in range(30):
        if sh("rabbitmqctl status", check=False).returncode == 0:
            break
        time.sleep(1)

    sh("redis-server --daemonize yes --appendonly yes --appendfsync always --save '' --port 6379")
    for _ in range(15):
        if sh("redis-cli ping", check=False).stdout.strip() == "PONG":
            break
        time.sleep(0.5)

    sh(
        "curl -fsSL https://github.com/nats-io/nats-server/releases/download/v2.10.22/nats-server-v2.10.22-linux-amd64.tar.gz -o /tmp/n.tgz "
        "&& tar -xzf /tmp/n.tgz -C /tmp && mv /tmp/nats-server-v2.10.22-linux-amd64/nats-server /usr/local/bin/nats-server "
        "&& chmod +x /usr/local/bin/nats-server && rm -rf /tmp/n.tgz /tmp/nats-server-*"
    )
    open("/tmp/nats.conf", "w").write("port: 4222\njetstream { store_dir: \"/tmp/n_js\" }\n")
    sh("nats-server -c /tmp/nats.conf > /tmp/n.log 2>&1 &")
    time.sleep(1.5)

    sh("curl -fsSL https://downloads.apache.org/kafka/4.4.0/kafka_2.13-4.4.0.tgz -o /tmp/k.tgz && mkdir -p /opt/kafka && tar -xzf /tmp/k.tgz -C /opt/kafka --strip-components=1 && rm /tmp/k.tgz")
    kp = "/opt/kafka/config/server.properties"
    ls = [l for l in open(kp).read().splitlines() if not l.startswith(("listeners=", "advertised.listeners=", "offsets.topic.replication.factor=", "transaction.state.log.", "group.initial.rebalance.delay.ms=", "num.partitions="))]
    ls += ["offsets.topic.replication.factor=1", "transaction.state.log.replication.factor=1", "transaction.state.log.min.isr=1", "group.initial.rebalance.delay.ms=0", "num.partitions=6", "listeners=PLAINTEXT://127.0.0.1:9092,CONTROLLER://127.0.0.1:9093", "advertised.listeners=PLAINTEXT://127.0.0.1:9092"]
    open(kp, "w").write("\n".join(ls) + "\n")
    cid = sh("/opt/kafka/bin/kafka-storage.sh random-uuid").stdout.strip().splitlines()[-1].strip()
    sh(f"/opt/kafka/bin/kafka-storage.sh format --standalone -t {cid} -c {kp} --ignore-formatted")
    sh("KAFKA_HEAP_OPTS='-Xmx1G -Xms512M' /opt/kafka/bin/kafka-server-start.sh -daemon /opt/kafka/config/server.properties")
    for _ in range(40):
        if sh("/opt/kafka/bin/kafka-topics.sh --bootstrap-server 127.0.0.1:9092 --list", check=False).returncode == 0:
            break
        time.sleep(1)
    log(f"Wave 3C bootstrap done in {time.time() - t0:.1f}s")


def run_g11_burst():
    """G11: 5x Burst (100 ev/s x 2s -> 500 ev/s x 3s -> 100 ev/s x 3s) across 4 brokers."""
    import numpy as np
    log("=== G11: 5x Burst (100 -> 500 -> 100 ev/s) ===")
    pl = os.urandom(1024)
    schedule = []  # (offset_s, phase)
    t = 0.0
    for phase, rate, dur in [("pre", 100, 2.0), ("burst", 500, 3.0), ("post", 100, 3.0)]:
        n = int(rate * dur)
        for i in range(n):
            schedule.append((t + i / rate, phase))
        t += dur

    out = {}
    # 1. Kafka
    from confluent_kafka import Consumer, Producer
    from confluent_kafka.admin import AdminClient, NewTopic
    topic = "g11_kafka"
    AdminClient({"bootstrap.servers": "127.0.0.1:9092"}).create_topics([NewTopic(topic, num_partitions=6, replication_factor=1)])
    time.sleep(0.4)
    stop = threading.Event()
    assigned = threading.Event()
    lats_by_phase = defaultdict(list)
    phase_map = {}
    lk = threading.Lock()

    def k_cons():
        c = Consumer({"bootstrap.servers": "127.0.0.1:9092", "group.id": f"g11-{uuid.uuid4().hex[:5]}", "auto.offset.reset": "earliest", "fetch.wait.max.ms": 2})
        c.subscribe([topic], on_assign=lambda _c, _p: assigned.set())
        while not stop.is_set():
            msgs = c.consume(200, timeout=0.005)
            now_us = int(time.time() * 1e6)
            with lk:
                for m in msgs:
                    if m is None or m.error():
                        continue
                    eb, ti = ENV.unpack_from(m.value(), 0)
                    ph = phase_map.get(eb, "post")
                    lats_by_phase[ph].append((now_us - ti) / 1000.0)
        c.close()

    ct = threading.Thread(target=k_cons, daemon=True)
    ct.start()
    assigned.wait(10)
    prod = Producer({"bootstrap.servers": "127.0.0.1:9092", "acks": "all", "linger.ms": 2})
    t0 = time.perf_counter()
    w0 = int(time.time() * 1e6)
    for off, ph in schedule:
        rem = off - (time.perf_counter() - t0)
        if rem > 0:
            time.sleep(rem)
        eb = uuid.uuid4().bytes
        with lk:
            phase_map[eb] = ph
        prod.produce(topic, value=ENV.pack(eb, w0 + int(off * 1e6)) + pl)
        prod.poll(0)
    prod.flush()
    time.sleep(1.5)
    stop.set()
    ct.join(5)
    out["kafka"] = {ph: round(float(np.percentile(lats_by_phase[ph], 99)), 2) for ph in ("pre", "burst", "post") if lats_by_phase[ph]}

    # 2. NATS JetStream
    async def _nats_burst():
        import nats
        nc = await nats.connect("nats://127.0.0.1:4222")
        js = nc.jetstream()
        await js.add_stream(name="g11_nats", subjects=["g11_nats.*"])
        psub = await js.pull_subscribe("g11_nats.*", durable="g11_d", stream="g11_nats")
        lbp = defaultdict(list)
        pm = {}
        done = asyncio.Event()

        async def cons_task():
            while not done.is_set():
                try:
                    msgs = await psub.fetch(200, timeout=0.02)
                except Exception:
                    continue
                now_us = int(time.time() * 1e6)
                for m in msgs:
                    eb, ti = ENV.unpack_from(m.data, 0)
                    lbp[pm.get(eb, "post")].append((now_us - ti) / 1000.0)
                    await m.ack()

        ctask = asyncio.create_task(cons_task())
        t0_n = time.perf_counter()
        w0_n = int(time.time() * 1e6)
        pending = []
        for off, ph in schedule:
            rem = off - (time.perf_counter() - t0_n)
            if rem > 0:
                await asyncio.sleep(rem)
            eb = uuid.uuid4().bytes
            pm[eb] = ph
            pending.append(asyncio.create_task(js.publish("g11_nats.1", ENV.pack(eb, w0_n + int(off * 1e6)) + pl)))
            if len(pending) >= 50:
                await asyncio.gather(*pending)
                pending.clear()
        if pending:
            await asyncio.gather(*pending)
        await asyncio.sleep(1.2)
        done.set()
        await ctask
        await nc.close()
        return {ph: round(float(np.percentile(lbp[ph], 99)), 2) for ph in ("pre", "burst", "post") if lbp[ph]}

    out["nats_jetstream"] = run_coro(_nats_burst())

    # 3. Redis Streams
    import redis
    r = redis.Redis(host="127.0.0.1", port=6379)
    r.delete("g11_redis")
    r.xgroup_create("g11_redis", "g11g", id="0", mkstream=True)
    stop_r = threading.Event()
    lbp_r = defaultdict(list)
    pm_r = {}
    lk_r = threading.Lock()

    def r_cons():
        rc = redis.Redis(host="127.0.0.1", port=6379)
        while not stop_r.is_set():
            res = rc.xreadgroup("g11g", "c1", {"g11_redis": ">"}, count=200, block=10)
            if not res:
                continue
            now_us = int(time.time() * 1e6)
            ids = []
            with lk_r:
                for _, entries in res:
                    for eid_s, flds in entries:
                        eb, ti = ENV.unpack_from(flds[b"b"], 0)
                        lbp_r[pm_r.get(eb, "post")].append((now_us - ti) / 1000.0)
                        ids.append(eid_s)
            if ids:
                rc.xack("g11_redis", "g11g", *ids)
        rc.close()

    rt = threading.Thread(target=r_cons, daemon=True)
    rt.start()
    t0_r = time.perf_counter()
    w0_r = int(time.time() * 1e6)
    batch_r = []
    for off, ph in schedule:
        rem = off - (time.perf_counter() - t0_r)
        if rem > 0:
            time.sleep(rem)
        eb = uuid.uuid4().bytes
        with lk_r:
            pm_r[eb] = ph
        batch_r.append(ENV.pack(eb, w0_r + int(off * 1e6)) + pl)
        if len(batch_r) >= 20:
            pipe = r.pipeline(transaction=False)
            for b in batch_r:
                pipe.xadd("g11_redis", {"b": b})
            pipe.execute()
            batch_r.clear()
    if batch_r:
        pipe = r.pipeline(transaction=False)
        for b in batch_r:
            pipe.xadd("g11_redis", {"b": b})
        pipe.execute()
    time.sleep(1.2)
    stop_r.set()
    rt.join(5)
    r.close()
    out["redis_streams"] = {ph: round(float(np.percentile(lbp_r[ph], 99)), 2) for ph in ("pre", "burst", "post") if lbp_r[ph]}

    # 4. RabbitMQ
    import pika
    cn = pika.BlockingConnection(pika.ConnectionParameters("127.0.0.1"))
    ch = cn.channel()
    ch.queue_delete("g11_rmq")
    ch.queue_declare("g11_rmq", durable=True)
    ch.confirm_delivery()
    stop_q = threading.Event()
    lbp_q = defaultdict(list)
    pm_q = {}
    lk_q = threading.Lock()

    def q_cons():
        c2 = pika.BlockingConnection(pika.ConnectionParameters("127.0.0.1"))
        ch2 = c2.channel()
        while not stop_q.is_set():
            m, _, body = ch2.basic_get("g11_rmq", auto_ack=True)
            if m is None:
                time.sleep(0.002)
                continue
            now_us = int(time.time() * 1e6)
            eb, ti = ENV.unpack_from(body, 0)
            with lk_q:
                lbp_q[pm_q.get(eb, "post")].append((now_us - ti) / 1000.0)
        c2.close()

    qt = threading.Thread(target=q_cons, daemon=True)
    qt.start()
    t0_q = time.perf_counter()
    w0_q = int(time.time() * 1e6)
    props = pika.BasicProperties(delivery_mode=2)
    for off, ph in schedule:
        rem = off - (time.perf_counter() - t0_q)
        if rem > 0:
            time.sleep(rem)
        eb = uuid.uuid4().bytes
        with lk_q:
            pm_q[eb] = ph
        ch.basic_publish("", "g11_rmq", ENV.pack(eb, w0_q + int(off * 1e6)) + pl, properties=props)
    time.sleep(2.0)
    stop_q.set()
    qt.join(5)
    cn.close()
    out["rabbitmq"] = {ph: round(float(np.percentile(lbp_q[ph], 99)), 2) for ph in ("pre", "burst", "post") if lbp_q[ph]}

    log(f"G11 done: {out}")
    return out


def run_g12_backlog_drain(n_events=5000):
    """G12: Accumulate 5,000 events with consumer stopped, then measure drain time & rate."""
    log(f"=== G12: Backlog Drain ({n_events} events) ===")
    pl = os.urandom(1024)
    res = {}

    # 1. RabbitMQ
    import pika
    cn = pika.BlockingConnection(pika.ConnectionParameters("127.0.0.1"))
    ch = cn.channel()
    ch.queue_delete("g12_rmq")
    ch.queue_declare("g12_rmq", durable=True)
    props = pika.BasicProperties(delivery_mode=2)
    for _ in range(n_events):
        ch.basic_publish("", "g12_rmq", pl, properties=props)
    t0 = time.perf_counter()
    got = 0
    while got < n_events:
        m, _, _ = ch.basic_get("g12_rmq", auto_ack=True)
        if m is None:
            break
        got += 1
    el = time.perf_counter() - t0
    cn.close()
    res["rabbitmq"] = {"events": got, "drain_s": round(el, 3), "drain_eps": round(got / max(0.001, el), 1)}

    # 2. Kafka
    from confluent_kafka import Consumer, Producer
    from confluent_kafka.admin import AdminClient, NewTopic
    AdminClient({"bootstrap.servers": "127.0.0.1:9092"}).create_topics([NewTopic("g12_kafka", num_partitions=6, replication_factor=1)])
    time.sleep(0.4)
    p = Producer({"bootstrap.servers": "127.0.0.1:9092", "acks": "all"})
    for _ in range(n_events):
        p.produce("g12_kafka", value=pl)
    p.flush()
    c = Consumer({"bootstrap.servers": "127.0.0.1:9092", "group.id": f"g12-{uuid.uuid4().hex[:5]}", "auto.offset.reset": "earliest"})
    ass = threading.Event()
    c.subscribe(["g12_kafka"], on_assign=lambda _c, _p: ass.set())
    while not ass.is_set():
        c.poll(0.05)
    t0 = time.perf_counter()
    got = 0
    while got < n_events and time.perf_counter() - t0 < 15.0:
        msgs = c.consume(500, timeout=0.05)
        got += sum(1 for m in msgs if m is not None and not m.error())
    el = time.perf_counter() - t0
    c.close()
    res["kafka"] = {"events": got, "drain_s": round(el, 3), "drain_eps": round(got / max(0.001, el), 1)}

    # 3. Redis Streams
    import redis
    r = redis.Redis(host="127.0.0.1", port=6379)
    r.delete("g12_redis")
    r.xgroup_create("g12_redis", "g12g", id="0", mkstream=True)
    for off in range(0, n_events, 500):
        pipe = r.pipeline(transaction=False)
        for _ in range(min(500, n_events - off)):
            pipe.xadd("g12_redis", {"b": pl})
        pipe.execute()
    t0 = time.perf_counter()
    got = 0
    while got < n_events:
        out = r.xreadgroup("g12g", "c1", {"g12_redis": ">"}, count=500, block=50)
        if not out:
            break
        ids = [eid_s for _, entries in out for eid_s, _ in entries]
        got += len(ids)
        r.xack("g12_redis", "g12g", *ids)
    el = time.perf_counter() - t0
    r.close()
    res["redis_streams"] = {"events": got, "drain_s": round(el, 3), "drain_eps": round(got / max(0.001, el), 1)}

    # 4. NATS JetStream
    async def _nats_drain():
        import nats
        nc = await nats.connect("nats://127.0.0.1:4222")
        js = nc.jetstream()
        await js.add_stream(name="g12_nats", subjects=["g12_nats.*"])
        for off in range(0, n_events, 200):
            await asyncio.gather(*[js.publish("g12_nats.1", pl) for _ in range(min(200, n_events - off))])
        psub = await js.pull_subscribe("g12_nats.*", durable="g12_d", stream="g12_nats")
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
        return {"events": g, "drain_s": round(el_n, 3), "drain_eps": round(g / max(0.001, el_n), 1)}

    res["nats_jetstream"] = run_coro(_nats_drain())
    log(f"G12 done: {res}")
    return res


def run_g15_network_delay():
    """G15: TCP delay proxy (+0, +10, +30 ms RTT) to measure latency amplification (C5) on NATS & Redis."""
    import numpy as np
    import redis

    log("=== G15: Network Delay Amplification (+0, +10, +30 ms RTT) ===")
    results = []

    def start_delay_proxy(target_port, delay_ms):
        srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        srv.bind(("127.0.0.1", 0))
        listen_port = srv.getsockname()[1]
        srv.listen(16)
        stop_ev = threading.Event()
        half_s = (delay_ms / 1000.0) / 2.0

        def pipe_sock(src, dst):
            try:
                while not stop_ev.is_set():
                    data = src.recv(65536)
                    if not data:
                        break
                    if half_s > 0:
                        time.sleep(half_s)
                    dst.sendall(data)
            except Exception:
                pass
            finally:
                try:
                    dst.close()
                except Exception:
                    pass

        def accept_loop():
            srv.settimeout(0.2)
            while not stop_ev.is_set():
                try:
                    cli, _ = srv.accept()
                except socket.timeout:
                    continue
                except Exception:
                    break
                up = socket.create_connection(("127.0.0.1", target_port))
                threading.Thread(target=pipe_sock, args=(cli, up), daemon=True).start()
                threading.Thread(target=pipe_sock, args=(up, cli), daemon=True).start()
            srv.close()

        t = threading.Thread(target=accept_loop, daemon=True)
        t.start()
        return listen_port, stop_ev

    pl = os.urandom(1024)
    for delay_ms in (0, 10, 30):
        # Redis single vs batch-20 over delayed link
        p_port, p_stop = start_delay_proxy(6379, delay_ms)
        r = redis.Redis(host="127.0.0.1", port=p_port)
        lats_1, lats_20 = [], []
        for _ in range(40):
            t0 = time.perf_counter()
            r.xadd("g15_r", {"b": pl})
            lats_1.append((time.perf_counter() - t0) * 1000.0)
        for _ in range(25):
            t0 = time.perf_counter()
            pipe = r.pipeline(transaction=False)
            for _ in range(20):
                pipe.xadd("g15_r", {"b": pl})
            pipe.execute()
            lats_20.append((time.perf_counter() - t0) * 1000.0)
        r.close()
        p_stop.set()

        # RabbitMQ publisher confirms over delayed link
        import pika
        q_port, q_stop = start_delay_proxy(5672, delay_ms)
        cn = pika.BlockingConnection(pika.ConnectionParameters("127.0.0.1", port=q_port))
        ch = cn.channel()
        ch.queue_declare("g15_q", durable=True)
        ch.confirm_delivery()
        props = pika.BasicProperties(delivery_mode=2)
        lats_q = []
        for _ in range(30):
            t0 = time.perf_counter()
            ch.basic_publish("", "g15_q", pl, properties=props)
            lats_q.append((time.perf_counter() - t0) * 1000.0)
        cn.close()
        q_stop.set()

        row = {
            "injected_rtt_ms": delay_ms,
            "redis_single_p50_ms": round(float(np.percentile(lats_1, 50)), 2),
            "redis_batch20_p50_ms": round(float(np.percentile(lats_20, 50)), 2),
            "rabbitmq_confirm_p50_ms": round(float(np.percentile(lats_q, 50)), 2),
        }
        results.append(row)
        log(f"  G15 rtt=+{delay_ms}ms: {row}")
    return results


def run_g19_dedup_cost():
    """G19: Consumer idempotency/dedup cost (none x memory x redis x postgres) on 2,000 events (10% dupes)."""
    import numpy as np
    import psycopg2
    import redis

    log("=== G19: Consumer Deduplication Cost (none x memory x redis x pg) ===")
    base_ids = [str(uuid.uuid4()) for _ in range(1800)]
    events = base_ids + base_ids[:200]  # 2,000 events, 10% duplicates

    res = {}
    # 1. None
    t0 = time.perf_counter()
    for _ in events:
        pass
    res["none"] = {"total_ms": round((time.perf_counter() - t0) * 1000.0, 2), "eps": round(len(events) / max(1e-6, time.perf_counter() - t0), 1), "dupes_caught": 0}

    # 2. In-memory set
    seen = set()
    dupes = 0
    t0 = time.perf_counter()
    for eid in events:
        if eid in seen:
            dupes += 1
        else:
            seen.add(eid)
    el = time.perf_counter() - t0
    res["dedup_memory"] = {"total_ms": round(el * 1000.0, 2), "eps": round(len(events) / max(1e-6, el), 1), "dupes_caught": dupes}

    # 3. Redis SET NX
    r = redis.Redis(host="127.0.0.1", port=6379)
    r.flushdb()
    dupes = 0
    lats = []
    t0 = time.perf_counter()
    for eid in events:
        ts = time.perf_counter()
        ok = r.set(f"d:{eid}", 1, nx=True, ex=3600)
        lats.append((time.perf_counter() - ts) * 1000.0)
        if not ok:
            dupes += 1
    el = time.perf_counter() - t0
    r.close()
    res["dedup_redis"] = {"total_ms": round(el * 1000.0, 2), "eps": round(len(events) / el, 1), "p99_ms": round(float(np.percentile(lats, 99)), 3), "dupes_caught": dupes}

    # 4. PostgreSQL INSERT ON CONFLICT DO NOTHING
    conn = psycopg2.connect(PG_DSN)
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute("DROP TABLE IF EXISTS consumed_events; CREATE TABLE consumed_events (event_id UUID PRIMARY KEY, consumed_at TIMESTAMPTZ DEFAULT now());")
    conn.autocommit = False
    dupes = 0
    lats = []
    t0 = time.perf_counter()
    for eid in events:
        ts = time.perf_counter()
        cur.execute("INSERT INTO consumed_events (event_id) VALUES (%s) ON CONFLICT DO NOTHING", (eid,))
        conn.commit()
        lats.append((time.perf_counter() - ts) * 1000.0)
        if cur.rowcount == 0:
            dupes += 1
    el = time.perf_counter() - t0
    conn.close()
    res["dedup_postgres"] = {"total_ms": round(el * 1000.0, 2), "eps": round(len(events) / el, 1), "p99_ms": round(float(np.percentile(lats, 99)), 3), "dupes_caught": dupes}

    log(f"G19 done: {res}")
    return res


def main():
    t0 = time.time()
    bootstrap()
    out = {
        "meta": {"platform": platform.platform(), "cpus": os.cpu_count()},
        "G11_burst_5x": run_g11_burst(),
        "G12_backlog_drain": run_g12_backlog_drain(5000),
        "G15_network_delay": run_g15_network_delay(),
        "G19_dedup_cost": run_g19_dedup_cost(),
    }
    out["meta"]["runtime_s"] = round(time.time() - t0, 1)
    print("===WAVE3C_JSON_START===")
    print(json.dumps(out, indent=2))
    print("===WAVE3C_JSON_END===")


if __name__ == "__main__":
    main()
