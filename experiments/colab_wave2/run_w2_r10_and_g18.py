#!/usr/bin/env python3
"""Completa os 2 blocos finais da Onda 2 (W2-D R=10 blocos pareados + G18 Replay 5000 eventos).
Usa downloads.apache.org CDN para Kafka e run_coro() para todo codigo asyncio do NATS.
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


def pack_env(eid, aid, av, ti, pl):
    return ENV_STRUCT.pack(eid.bytes, aid, av, ti) + pl


def unpack_env(body):
    eb, aid, av, ti = ENV_STRUCT.unpack_from(body, 0)
    return uuid.UUID(bytes=eb), aid, av, ti


def bootstrap():
    t0 = time.time()
    os.environ["DEBIAN_FRONTEND"] = "noninteractive"
    sh("apt-get update -qq && apt-get install -y -qq postgresql redis-server openjdk-21-jre-headless curl")
    sh(f"{sys.executable} -m pip install -q psycopg2-binary confluent-kafka redis nats-py numpy")

    pg_ver = sh("ls /etc/postgresql | sort -V | tail -n 1").stdout.strip()
    with open(f"/etc/postgresql/{pg_ver}/main/postgresql.conf", "a") as f:
        f.write("\nwal_level = logical\nmax_replication_slots = 8\nmax_wal_senders = 8\nmax_connections = 200\nsynchronous_commit = on\nshared_buffers = 1GB\n")
    hba = f"/etc/postgresql/{pg_ver}/main/pg_hba.conf"
    open(hba, "w").write("local all all trust\nhost all all 127.0.0.1/32 trust\nlocal replication all trust\nhost replication all 127.0.0.1/32 trust\n" + open(hba).read())
    sh("service postgresql restart")
    sh("su - postgres -c \"psql -c \\\"ALTER USER postgres WITH PASSWORD 'postgres' SUPERUSER REPLICATION;\\\"\"")
    sh("su - postgres -c \"psql -c \\\"CREATE DATABASE study;\\\"\"", check=False)

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
        DROP TABLE IF EXISTS outbox, orders, customers CASCADE;
        CREATE TABLE customers (id INT PRIMARY KEY, balance_cents BIGINT NOT NULL, version BIGINT NOT NULL DEFAULT 0);
        CREATE TABLE orders (id UUID PRIMARY KEY, customer_id INT NOT NULL, aggregate_version BIGINT NOT NULL, event_id UUID NOT NULL);
        CREATE TABLE outbox (id BIGSERIAL PRIMARY KEY, event_id UUID NOT NULL, aggregate_id INT NOT NULL, aggregate_version BIGINT NOT NULL, partition_key INT NOT NULL, t_int_us BIGINT NOT NULL, payload BYTEA NOT NULL, published_at TIMESTAMPTZ);
        CREATE INDEX outbox_pending_idx ON outbox (id) WHERE published_at IS NULL;
    """)
    cur.executemany("INSERT INTO customers VALUES (%s, 100000000, 0)", [(i,) for i in range(200)])
    c.close()
    log(f"Bootstrap done in {time.time() - t0:.1f}s")


def reset_pg():
    import psycopg2
    c = psycopg2.connect(PG_DSN)
    c.autocommit = True
    cur = c.cursor()
    cur.execute("TRUNCATE TABLE outbox, orders RESTART IDENTITY; UPDATE customers SET balance_cents=100000000, version=0;")
    cur.execute("SELECT slot_name FROM pg_replication_slots WHERE slot_name='study_cdc_slot'")
    if cur.fetchone():
        cur.execute("SELECT pg_drop_replication_slot('study_cdc_slot')")
    c.close()


def prepare_dest(tr, dest):
    if tr == "kafka":
        from confluent_kafka.admin import AdminClient, NewTopic
        fs = AdminClient({"bootstrap.servers": "127.0.0.1:9092"}).create_topics([NewTopic(dest, num_partitions=12, replication_factor=1)])
        for f in fs.values():
            try:
                f.result(10)
            except Exception:
                pass
    elif tr == "redis":
        import redis
        r = redis.Redis(host="127.0.0.1", port=6379)
        r.delete(dest)
        r.xgroup_create(dest, "study_grp", id="0", mkstream=True)
        r.close()
    elif tr == "nats":
        async def _mk():
            import nats
            nc = await nats.connect("nats://127.0.0.1:4222")
            js = nc.jetstream()
            await js.add_stream(name=dest, subjects=[f"{dest}.*"], storage="file")
            await nc.close()
        run_coro(_mk())


def run_trial(tr, strat, rate, poll_ms=10, block=1):
    import numpy as np
    import psycopg2
    import psycopg2.extras

    reset_pg()
    dest = f"r10_{tr}_{uuid.uuid4().hex[:6]}"
    if tr != "pg-queue":
        prepare_dest(tr, dest)
    if strat == "outbox-cdc":
        c = psycopg2.connect(PG_DSN)
        c.autocommit = True
        c.cursor().execute("SELECT pg_create_logical_replication_slot('study_cdc_slot', 'test_decoding')")
        c.close()

    stop_ev = threading.Event()
    lk = threading.Lock()
    deliv = defaultdict(list)
    e2e, l_req = [], []
    exp = {}

    # Consumer
    ct = None
    if tr != "pg-queue":
        ready = threading.Event()
        if tr == "kafka":
            def c_k():
                from confluent_kafka import Consumer
                c = Consumer({"bootstrap.servers": "127.0.0.1:9092", "group.id": f"g-{uuid.uuid4().hex[:6]}", "auto.offset.reset": "earliest", "enable.auto.commit": False, "fetch.wait.max.ms": 2})
                c.subscribe([dest], on_assign=lambda _c, _p: ready.set())
                while not stop_ev.is_set():
                    msgs = c.consume(300, timeout=0.005)
                    t_us = int(time.time() * 1e6)
                    with lk:
                        for m in msgs:
                            if m is None or m.error():
                                continue
                            eid, aid, av, ti = unpack_env(m.value())
                            if not deliv[eid]:
                                e2e.append((t_us - ti) / 1000.0)
                            deliv[eid].append((t_us, aid, av))
                    if msgs:
                        c.commit(asynchronous=True)
                c.close()
            ct = threading.Thread(target=c_k, daemon=True)
        elif tr == "nats":
            def c_n():
                async def _l():
                    import nats
                    nc = await nats.connect("nats://127.0.0.1:4222")
                    js = nc.jetstream()
                    ps = await js.pull_subscribe(f"{dest}.*", durable=f"d_{uuid.uuid4().hex[:5]}", stream=dest)
                    ready.set()
                    while not stop_ev.is_set():
                        try:
                            msgs = await ps.fetch(200, timeout=0.02)
                        except Exception:
                            continue
                        t_us = int(time.time() * 1e6)
                        with lk:
                            for m in msgs:
                                eid, aid, av, ti = unpack_env(m.data)
                                if not deliv[eid]:
                                    e2e.append((t_us - ti) / 1000.0)
                                deliv[eid].append((t_us, aid, av))
                        for m in msgs:
                            await m.ack()
                    await nc.close()
                asyncio.run(_l())
            ct = threading.Thread(target=c_n, daemon=True)
        elif tr == "redis":
            def c_r():
                import redis
                r = redis.Redis(host="127.0.0.1", port=6379)
                ready.set()
                while not stop_ev.is_set():
                    res = r.xreadgroup("study_grp", "c1", {dest: ">"}, count=200, block=10)
                    if not res:
                        continue
                    t_us = int(time.time() * 1e6)
                    acks = []
                    with lk:
                        for _, entries in res:
                            for es, flds in entries:
                                eid, aid, av, ti = unpack_env(flds[b"b"])
                                if not deliv[eid]:
                                    e2e.append((t_us - ti) / 1000.0)
                                deliv[eid].append((t_us, aid, av))
                                acks.append(es)
                    if acks:
                        r.xack(dest, "study_grp", *acks)
                r.close()
            ct = threading.Thread(target=c_r, daemon=True)
        ct.start()
        ready.wait(10)

    # Relay / pg-queue
    def make_pub():
        if tr == "kafka":
            from confluent_kafka import Producer
            p = Producer({"bootstrap.servers": "127.0.0.1:9092", "acks": "all", "linger.ms": 1, "batch.num.messages": 300})
            def send(items):
                for aid, b in items:
                    p.produce(dest, key=str(aid).encode(), value=b)
                p.flush(10)
            return send, lambda: p.flush(5)
        if tr == "redis":
            import redis
            r = redis.Redis(host="127.0.0.1", port=6379)
            def send(items):
                pipe = r.pipeline(transaction=False)
                for _, b in items:
                    pipe.xadd(dest, {"b": b})
                pipe.execute()
            return send, r.close
        if tr == "nats":
            import nats
            lp = asyncio.new_event_loop()
            nc = lp.run_until_complete(nats.connect("nats://127.0.0.1:4222"))
            js = nc.jetstream()
            def send(items):
                lp.run_until_complete(asyncio.gather(*[js.publish(f"{dest}.{aid % 12}", b) for aid, b in items]))
            def close():
                lp.run_until_complete(nc.close())
                lp.close()
            return send, close

    if strat == "pg-queue":
        def rt_fn():
            c = psycopg2.connect(PG_DSN)
            c.autocommit = False
            cur = c.cursor()
            while not stop_ev.is_set():
                cur.execute("WITH p AS (SELECT id, event_id, aggregate_id, aggregate_version, t_int_us FROM outbox ORDER BY id LIMIT 100 FOR UPDATE SKIP LOCKED) DELETE FROM outbox USING p WHERE outbox.id=p.id RETURNING p.event_id, p.aggregate_id, p.aggregate_version, p.t_int_us")
                rows = cur.fetchall()
                c.commit()
                if not rows:
                    time.sleep(0.004)
                    continue
                t_us = int(time.time() * 1e6)
                with lk:
                    for r in rows:
                        eid = uuid.UUID(r[0])
                        if not deliv[eid]:
                            e2e.append((t_us - r[3]) / 1000.0)
                        deliv[eid].append((t_us, r[1], r[2]))
            c.close()
    elif strat == "outbox-cdc":
        def rt_fn():
            send, close = make_pub()
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
                    send(batch)
                if last:
                    last.cursor.send_feedback(flush_lsn=last.data_start)
                else:
                    select.select([rcur], [], [], 0.002)
            close()
            rc.close()
    else:
        def rt_fn():
            send, close = make_pub()
            c = psycopg2.connect(PG_DSN)
            c.autocommit = False
            cur = c.cursor()
            sl = poll_ms / 1000.0
            while not stop_ev.is_set():
                cur.execute("SELECT id, event_id, aggregate_id, aggregate_version, t_int_us, payload FROM outbox WHERE published_at IS NULL ORDER BY id LIMIT 100 FOR UPDATE SKIP LOCKED")
                rows = cur.fetchall()
                if not rows:
                    c.commit()
                    time.sleep(sl)
                    continue
                send([(r[2], pack_env(uuid.UUID(r[1]), r[2], r[3], r[4], bytes(r[5]))) for r in rows])
                cur.execute("UPDATE outbox SET published_at=clock_timestamp() WHERE id=ANY(%s)", ([r[0] for r in rows],))
                c.commit()
            close()
            c.close()

    rt = threading.Thread(target=rt_fn, daemon=True)
    rt.start()
    time.sleep(0.1)

    rng = np.random.default_rng(400 + block)
    ranks = np.arange(1, 201, dtype=np.float64)
    probs = (1.0 / ranks) / (1.0 / ranks).sum()
    pl = os.urandom(1024)
    q = queue.Queue(maxsize=20000)

    def wr():
        c = psycopg2.connect(PG_DSN)
        c.autocommit = False
        cur = c.cursor()
        while True:
            it = q.get()
            if it is None:
                break
            ti, aid = it
            eid, oid = uuid.uuid4(), uuid.uuid4()
            cur.execute("SELECT balance_cents, version FROM customers WHERE id=%s FOR UPDATE", (aid,))
            bal, ver = cur.fetchone()
            nv = ver + 1
            cur.execute("UPDATE customers SET balance_cents=%s, version=%s WHERE id=%s", (bal - 1, nv, aid))
            cur.execute("INSERT INTO orders VALUES (%s,%s,%s,%s)", (str(oid), aid, nv, str(eid)))
            cur.execute("INSERT INTO outbox (event_id, aggregate_id, aggregate_version, partition_key, t_int_us, payload) VALUES (%s,%s,%s,%s,%s,%s)", (str(eid), aid, nv, aid % 16, ti, psycopg2.Binary(pl)))
            c.commit()
            td = int(time.time() * 1e6)
            with lk:
                l_req.append((td - ti) / 1000.0)
                exp[eid] = (aid, nv)
        c.close()

    wts = [threading.Thread(target=wr, daemon=True) for _ in range(4)]
    for t in wts:
        t.start()

    n_ev = int(rate * 3.5)
    dt = 1.0 / rate
    t0 = time.perf_counter()
    w0 = int(time.time() * 1e6)
    for i in range(n_ev):
        sched = i * dt
        rem = sched - (time.perf_counter() - t0)
        if rem > 0:
            time.sleep(rem)
        q.put((w0 + int(sched * 1e6), int(rng.choice(200, p=probs))))
    for _ in wts:
        q.put(None)
    for t in wts:
        t.join(20)

    end = time.time() + 3.5
    while time.time() < end:
        with lk:
            if len(deliv) >= len(exp):
                break
        time.sleep(0.03)
    stop_ev.set()
    rt.join(5)
    if ct:
        ct.join(5)

    return {
        "block": block, "transport": tr, "strategy": strat, "poll_ms": poll_ms if strat == "outbox-poll" else None,
        "rate": rate, "expected": len(exp), "delivered": len(deliv), "censored": max(0, len(exp) - len(deliv)),
        "l_req_p99_ms": round(float(np.percentile(l_req, 99)), 2) if l_req else None,
        "l_e2e_p50_ms": round(float(np.percentile(e2e, 50)), 2) if e2e else None,
        "l_e2e_p99_ms": round(float(np.percentile(e2e, 99)), 2) if e2e else None,
    }


def run_g18_replay(n_events=5000):
    log(f"=== G18 Replay {n_events} events ===")
    pl = os.urandom(1024)
    res = {}

    # Kafka
    prepare_dest("kafka", "g18_k")
    from confluent_kafka import Consumer, Producer
    p = Producer({"bootstrap.servers": "127.0.0.1:9092", "acks": "all"})
    for i in range(n_events):
        p.produce("g18_k", value=pl)
    p.flush()
    c = Consumer({"bootstrap.servers": "127.0.0.1:9092", "group.id": f"rep-{uuid.uuid4().hex[:5]}", "auto.offset.reset": "earliest"})
    ass = threading.Event()
    c.subscribe(["g18_k"], on_assign=lambda _c, _p: ass.set())
    while not ass.is_set():
        c.poll(0.05)
    t0 = time.perf_counter()
    got = 0
    while got < n_events and time.perf_counter() - t0 < 15.0:
        msgs = c.consume(500, timeout=0.05)
        got += sum(1 for m in msgs if m is not None and not m.error())
    el = time.perf_counter() - t0
    c.close()
    res["kafka"] = {"events": got, "replay_s": round(el, 3), "replay_eps": round(got / max(0.001, el), 1)}

    # NATS JetStream (100% inside run_coro!)
    async def _nats_g18():
        import nats
        nc = await nats.connect("nats://127.0.0.1:4222")
        js = nc.jetstream()
        await js.add_stream(name="g18_n", subjects=["g18_n.*"], storage="file")
        for off in range(0, n_events, 250):
            await asyncio.gather(*[js.publish("g18_n.1", pl) for _ in range(min(250, n_events - off))])
        ps = await js.pull_subscribe("g18_n.*", durable="g18_d", stream="g18_n")
        t0_n = time.perf_counter()
        g = 0
        while g < n_events and time.perf_counter() - t0_n < 15.0:
            try:
                msgs = await ps.fetch(500, timeout=0.1)
            except Exception:
                continue
            g += len(msgs)
            for m in msgs:
                await m.ack()
        el_n = time.perf_counter() - t0_n
        await nc.close()
        return {"events": g, "replay_s": round(el_n, 3), "replay_eps": round(g / max(0.001, el_n), 1)}
    res["nats_jetstream"] = run_coro(_nats_g18())

    # Redis Streams
    import redis
    r = redis.Redis(host="127.0.0.1", port=6379)
    r.delete("g18_r")
    for off in range(0, n_events, 500):
        pipe = r.pipeline(transaction=False)
        for _ in range(min(500, n_events - off)):
            pipe.xadd("g18_r", {"b": pl})
        pipe.execute()
    r.xgroup_create("g18_r", "rep_g", id="0", mkstream=False)
    t0 = time.perf_counter()
    got = 0
    while got < n_events:
        out = r.xreadgroup("rep_g", "c1", {"g18_r": ">"}, count=500, block=50)
        if not out:
            break
        ids = [es for _, entries in out for es, _ in entries]
        got += len(ids)
        r.xack("g18_r", "rep_g", *ids)
    el = time.perf_counter() - t0
    r.close()
    res["redis_streams"] = {"events": got, "replay_s": round(el, 3), "replay_eps": round(got / max(0.001, el), 1)}
    res["rabbitmq_and_pg_queue"] = {"supported": False, "reason": "S-fila: consumo remove a mensagem"}
    return res


def main():
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--start-block", type=int, default=1)
    ap.add_argument("--end-block", type=int, default=5)
    ap.add_argument("--with-g18", action="store_true")
    args, _ = ap.parse_known_args()

    t0 = time.time()
    bootstrap()
    cfgs = [
        ("kafka", "outbox-cdc", 0),
        ("kafka", "outbox-poll", 10),
        ("kafka", "outbox-poll", 100),
        ("nats", "outbox-cdc", 0),
        ("nats", "outbox-poll", 10),
        ("redis", "outbox-poll", 10),
        ("pg-queue", "pg-queue", 0),
    ]
    r10 = []
    for block in range(args.start_block, args.end_block + 1):
        for rate in (100, 300):
            for tr, st, p in cfgs:
                r10.append(run_trial(tr, st, rate, p, block))
        log(f"  R10 block {block}/{args.end_block} done")
    out = {"W2_D_R10_paired": r10, "runtime_s": round(time.time() - t0, 1)}
    if args.with_g18:
        out["W2_E_G18_replay"] = run_g18_replay(5000)
    print("===W2_PART2_JSON_START===")
    print(json.dumps(out, indent=2))
    print("===W2_PART2_JSON_END===")


if __name__ == "__main__":
    main()
