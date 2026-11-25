#!/usr/bin/env python3
"""Onda 3B (VM paralela isolada) — Manutencao PG (N09 / M6) + Empresa Pequena Lambda/ECS (A-S2):
  - N09 (Ablacao A6 / M6): Compara cleanup `mark+delete` vs `delete` vs `partition-drop`
    sob carga continua, medindo bloat (MB), dead tuples e deriva de p99 por janela.
  - A-S2 (Perfil Empresa Pequena): Compara conexao efemera (Lambda sem RDS Proxy: nova conexao por tx)
    vs pool persistente (ECS / Lambda com RDS Proxy) em W0 (2, 10, 50 ev/s).
"""

import json
import os
import platform
import subprocess
import sys
import threading
import time
import uuid


def log(msg):
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", file=sys.stderr, flush=True)


def sh(cmd, check=True):
    res = subprocess.run(cmd, shell=True, text=True, capture_output=True)
    if check and res.returncode != 0:
        log(f"FAIL {cmd}\nSTDERR: {res.stderr[-800:]}")
        raise RuntimeError(cmd)
    return res


PG_DSN = "host=127.0.0.1 port=5432 dbname=study user=postgres password=postgres"


def bootstrap():
    t0 = time.time()
    os.environ["DEBIAN_FRONTEND"] = "noninteractive"
    sh("apt-get update -qq && apt-get install -y -qq postgresql")
    sh(f"{sys.executable} -m pip install -q psycopg2-binary numpy")
    pg_ver = sh("ls /etc/postgresql | sort -V | tail -n 1").stdout.strip()
    hba = f"/etc/postgresql/{pg_ver}/main/pg_hba.conf"
    open(hba, "w").write("local all all trust\nhost all all 127.0.0.1/32 trust\n" + open(hba).read())
    sh("service postgresql restart")
    sh("su - postgres -c \"psql -c \\\"ALTER USER postgres WITH PASSWORD 'postgres' SUPERUSER;\\\"\"")
    sh("su - postgres -c \"psql -c \\\"CREATE DATABASE study;\\\"\"", check=False)
    log(f"Wave 3B bootstrap done in {time.time() - t0:.1f}s")


def run_n09_pg_maintenance():
    """N09 (Ablacao A6 / M6): mark vs delete vs partition-drop + autovacuum on/off."""
    import numpy as np
    import psycopg2

    log("=== N09: PostgreSQL Maintenance & Bloat (mark vs delete vs partition-drop) ===")
    results = []
    payload = os.urandom(1024)

    for mode, autovac in [("mark_only", True), ("delete_after_ack", True), ("delete_after_ack", False), ("partition_drop", True)]:
        conn = psycopg2.connect(PG_DSN)
        conn.autocommit = True
        cur = conn.cursor()
        cur.execute("DROP TABLE IF EXISTS ob_m6 CASCADE;")
        if mode == "partition_drop":
            cur.execute("CREATE TABLE ob_m6 (id BIGSERIAL, bucket INT NOT NULL, payload BYTEA NOT NULL, published_at TIMESTAMPTZ, PRIMARY KEY (bucket, id)) PARTITION BY LIST (bucket);")
            for b in range(6):
                cur.execute(f"CREATE TABLE ob_m6_p{b} PARTITION OF ob_m6 FOR VALUES IN ({b});")
        else:
            av = "true" if autovac else "false"
            cur.execute(f"CREATE TABLE ob_m6 (id BIGSERIAL PRIMARY KEY, bucket INT NOT NULL, payload BYTEA NOT NULL, published_at TIMESTAMPTZ) WITH (autovacuum_enabled = {av});")
            cur.execute("CREATE INDEX ob_m6_pend ON ob_m6 (id) WHERE published_at IS NULL;")

        # Write 30,000 rows across 6 buckets (5,000 per bucket) while relay cleans up
        insert_lats = []
        poll_lats = []
        t0_all = time.perf_counter()
        for bucket in range(6):
            # Producer burst of 5,000 rows in batches of 50
            for _ in range(100):
                t0 = time.perf_counter()
                args = [(bucket, psycopg2.Binary(payload)) for _ in range(50)]
                cur.executemany("INSERT INTO ob_m6 (bucket, payload) VALUES (%s, %s)", args)
                insert_lats.append((time.perf_counter() - t0) * 1000.0)
            # Relay drain + cleanup
            t0p = time.perf_counter()
            if mode == "mark_only":
                cur.execute("UPDATE ob_m6 SET published_at = now() WHERE bucket = %s AND published_at IS NULL", (bucket,))
            elif mode == "delete_after_ack":
                cur.execute("DELETE FROM ob_m6 WHERE bucket = %s", (bucket,))
            elif mode == "partition_drop":
                cur.execute(f"DROP TABLE ob_m6_p{bucket};")
            poll_lats.append((time.perf_counter() - t0p) * 1000.0)

        # Measure final table+index size and dead tuples
        if mode == "partition_drop":
            size_mb = 0.0
            dead_tup = 0
        else:
            cur.execute("SELECT pg_total_relation_size('ob_m6') / (1024.0 * 1024.0);")
            size_mb = round(float(cur.fetchone()[0]), 2)
            cur.execute("SELECT COALESCE(n_dead_tup, 0) FROM pg_stat_user_tables WHERE relname = 'ob_m6';")
            row = cur.fetchone()
            dead_tup = int(row[0]) if row else 0
        conn.close()

        r = {
            "cleanup_mode": mode,
            "autovacuum": autovac,
            "rows_processed": 30000,
            "insert_batch50_p99_ms": round(float(np.percentile(insert_lats, 99)), 2),
            "cleanup_5k_p99_ms": round(float(np.percentile(poll_lats, 99)), 2),
            "final_table_size_mb": size_mb,
            "dead_tuples": dead_tup,
            "total_s": round(time.perf_counter() - t0_all, 2),
        }
        results.append(r)
        log(f"  N09 {mode} autovac={autovac}: size={size_mb}MB dead={dead_tup} cleanup_p99={r['cleanup_5k_p99_ms']}ms")
    return results


def run_as2_lambda_vs_ecs_small():
    """A-S2: Small company — ephemeral connection per tx (Lambda without RDS Proxy) vs persistent pool (ECS/Proxy)."""
    import numpy as np
    import psycopg2

    log("=== A-S2: Small Company (Lambda ephemeral conn vs ECS persistent pool) ===")
    conn = psycopg2.connect(PG_DSN)
    conn.autocommit = True
    conn.cursor().execute("DROP TABLE IF EXISTS small_ob CASCADE; CREATE TABLE small_ob (id BIGSERIAL PRIMARY KEY, eid UUID, payload BYTEA);")
    conn.close()

    rows = []
    pl = os.urandom(1024)
    for mode in ("lambda_ephemeral_conn", "ecs_persistent_pool"):
        for rate in (5, 25, 50):
            n_ev = rate * 4
            dt = 1.0 / rate
            lats = []
            pool_conn = psycopg2.connect(PG_DSN) if mode == "ecs_persistent_pool" else None
            t0_all = time.perf_counter()
            for i in range(n_ev):
                sched = i * dt
                now = time.perf_counter() - t0_all
                if sched > now:
                    time.sleep(sched - now)
                t0 = time.perf_counter()
                if mode == "lambda_ephemeral_conn":
                    c = psycopg2.connect(PG_DSN)
                    cur = c.cursor()
                    cur.execute("INSERT INTO small_ob (eid, payload) VALUES (%s, %s)", (str(uuid.uuid4()), psycopg2.Binary(pl)))
                    c.commit()
                    c.close()
                else:
                    cur = pool_conn.cursor()
                    cur.execute("INSERT INTO small_ob (eid, payload) VALUES (%s, %s)", (str(uuid.uuid4()), psycopg2.Binary(pl)))
                    pool_conn.commit()
                lats.append((time.perf_counter() - t0) * 1000.0)
            if pool_conn:
                pool_conn.close()
            r = {
                "runtime_mode": mode,
                "rate_eps": rate,
                "tx_p50_ms": round(float(np.percentile(lats, 50)), 2),
                "tx_p99_ms": round(float(np.percentile(lats, 99)), 2),
            }
            rows.append(r)
            log(f"  A-S2 {mode:<22} rate={rate:>2}: p50={r['tx_p50_ms']}ms p99={r['tx_p99_ms']}ms")
    return rows


def main():
    t0 = time.time()
    bootstrap()
    out = {
        "meta": {"platform": platform.platform(), "cpus": os.cpu_count()},
        "N09_pg_maintenance_bloat": run_n09_pg_maintenance(),
        "A_S2_lambda_vs_ecs": run_as2_lambda_vs_ecs_small(),
    }
    out["meta"]["runtime_s"] = round(time.time() - t0, 1)
    print("===WAVE3B_JSON_START===")
    print(json.dumps(out, indent=2))
    print("===WAVE3B_JSON_END===")


if __name__ == "__main__":
    main()
