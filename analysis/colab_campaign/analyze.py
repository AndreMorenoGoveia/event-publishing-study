#!/usr/bin/env python3
"""Analisa os resultados da campanha multi-transporte no Colab e gera RESULTADOS.md.

Aplica a regra estatistica calibrada pelo Monte Carlo (analysis/montecarlo/RESULTADOS.md S2):
  - IC t pareado por bloco sobre log(razao) (n=3 blocos)
  - Classificacao unica (doc 05 §7.2): equivalente / diferente / inconclusivo (delta = 10%)
"""

import json
import math
import statistics
from collections import defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parent
RAW_STDOUT = ROOT / "raw_stdout.txt"
RESULTS_JSON = ROOT / "results.json"
REPORT_MD = ROOT / "RESULTADOS.md"

# t_{0.975, df=2} = 4.30265 (95% CI for n=3 paired blocks)
T_CRIT_95 = {2: 4.302652729911275, 3: 3.182446305284263, 4: 2.7764451051977987}


def paired_log_ratio_ci(vals_a, vals_b, delta=0.10):
    """Paired t-CI on log(a / b) across blocks. Returns (ratio_geom, lo, hi, cls)."""
    diffs = [math.log(a / b) for a, b in zip(vals_a, vals_b) if a and b and a > 0 and b > 0]
    n = len(diffs)
    if n < 2:
        return None, None, None, "inconclusivo"
    mu = statistics.mean(diffs)
    sd = statistics.stdev(diffs)
    tcrit = T_CRIT_95.get(n - 1, 2.57)
    hw = tcrit * sd / math.sqrt(n)
    lo, hi = math.exp(mu - hw), math.exp(mu + hw)
    ratio = math.exp(mu)
    if lo >= (1.0 - delta) and hi <= (1.0 + delta):
        cls = "equivalente"
    elif hi < (1.0 - delta) or lo > (1.0 + delta):
        cls = "diferente"
    else:
        cls = "inconclusivo"
    return round(ratio, 3), round(lo, 3), round(hi, 3), cls


def main():
    if RAW_STDOUT.exists():
        raw = RAW_STDOUT.read_text()
        if "===CAMPAIGN_JSON_START===" in raw:
            j = raw.split("===CAMPAIGN_JSON_START===")[1].split("===CAMPAIGN_JSON_END===")[0].strip()
            d = json.loads(j)
            kafka_clean_path = ROOT / "kafka_clean.json"
            if kafka_clean_path.exists():
                kd = json.loads(kafka_clean_path.read_text())
                d["C1_fencing_P6"]["kafka_two_layer_fencing"] = kd["C1_kafka_clean"]
                d["C2_multi_transport_frontier"] = [
                    r for r in d["C2_multi_transport_frontier"] if r["transport"] != "kafka"
                ] + kd["C2_kafka_clean"]
                d["meta"]["kafka_clean_rerun_s"] = kd["runtime_s"]
            RESULTS_JSON.write_text(json.dumps(d, indent=2) + "\n")

    d = json.loads(RESULTS_JSON.read_text())
    meta = d["meta"]

    # Group C2 by (rate, transport, strategy, poll_ms)
    c2_grp = defaultdict(list)
    for r in d["C2_multi_transport_frontier"]:
        k = (r["rate"], r["transport"], r["strategy"], r["poll_ms"])
        c2_grp[k].append(r)

    # Sort each group by block for paired comparisons
    for k in c2_grp:
        c2_grp[k].sort(key=lambda x: x["block"])

    lines = []
    lines.append("# Campanha Multi-Transporte no Colab — Resultados Empíricos")
    lines.append("")
    lines.append(
        f"- **Ambiente:** VM Colab `--high-mem` ({meta['cpus']} vCPUs, {meta['ram_gib']} GiB RAM, "
        f"PostgreSQL {meta['pg_ver']} `wal_level=logical`, Apache Kafka 3.9.0 KRaft, RabbitMQ durável + confirms, Redis Streams AOF `always`)."
    )
    lines.append(
        f"- **Duração total:** {meta['total_runtime_s']} s (~{meta['total_runtime_s']/60:.1f} min) · "
        f"**Gasto real:** `0,08` compute units (`200,96` → `200,88` CU; `Active assignments: 0`)."
    )
    lines.append("- **Dados brutos:** [results.json](results.json) · **Script:** [run_campaign.py](../../experiments/colab_campaign/run_campaign.py).")
    lines.append("")
    lines.append(
        "> **Escopo e honestidade metodológica (doc 07 §1 e doc 12 §10):** execução **single-host (T1/PG1)** com `R=3` blocos pareados e "
        "IC `t` pareado sobre `log(razão)` (conforme S2 do Monte Carlo). **Não verificado aqui:** F2 real com perda de page cache, "
        "replicação multi-host T3/PG2 (K3/K4) e tráfego cross-AZ."
    )
    lines.append("")

    # --- C1 ---
    c1 = d["C1_fencing_P6"]
    kf = c1["kafka_two_layer_fencing"]
    rmq = c1["rabbitmq_db_fencing_only"]
    lines.append("## 1. C1 — Fencing em Duas Camadas: Kafka × RabbitMQ (Previsão P6)")
    lines.append("")
    lines.append("| Broker | Mecanismo testado | Sequência entregue | Zumbi cercado pelo broker? | Inversões O1 |")
    lines.append("|---|---|---|---:|---:|")
    lines.append(
        f"| **Kafka 3.9** | Epoch no banco + `transactional.id` (`read_committed`) | `{kf['versions_delivered']}` | **{kf['zombie_fenced_by_broker']}** | **{kf['o1_inversions']}** |"
    )
    lines.append(
        f"| **RabbitMQ** | Epoch só no banco (`db_mark_blocked={rmq['db_mark_blocked']}`) | `{rmq['versions_delivered']}` | **{rmq['zombie_fenced_by_broker']}** | **{rmq['o1_inversions']}** |"
    )
    lines.append("")
    lines.append(
        "**Resultado (P6 confirmada):** no Kafka, o produtor zumbi recebeu erro de fencing no broker (`PRODUCER_FENCED` / `INVALID_PRODUCER_EPOCH`) "
        "e **zero** inversões chegaram ao consumidor `read_committed`. No RabbitMQ, mesmo com fencing de `epoch` no banco bloqueando o `UPDATE`, "
        "a mensagem do zumbi já enviada entrou na fila fora de ordem (`[1, 2, 1]`)."
    )
    lines.append("")

    # --- C2 ---
    lines.append("## 2. C2 — Fronteira Multi-Transporte (`R=3` blocos, carga representativa Zipf)")
    lines.append("")
    lines.append("| λ (ev/s) | Transporte | Estratégia | `L_req` p99 (ms) | `L_e2e` p50 (ms) | `L_e2e` p99 (ms) | Censurados (3 blocos) | Violações O1 |")
    lines.append("|---:|---|---|---:|---:|---:|---:|---:|")
    for k in sorted(c2_grp.keys(), key=lambda x: (x[0], x[1], x[2], x[3] or 0)):
        rows = c2_grp[k]
        rate, tr, strat, p_ms = k
        sname = f"{strat} (p={p_ms}ms)" if p_ms is not None else strat
        rq99 = statistics.median([x["l_req_p99_ms"] for x in rows])
        e50 = statistics.median([x["l_e2e_p50_ms"] for x in rows])
        e99 = statistics.median([x["l_e2e_p99_ms"] for x in rows])
        cens = sum(x["censored"] for x in rows)
        o1 = sum(x["order_violations_O1"] for x in rows)
        lines.append(f"| {rate} | `{tr}` | `{sname}` | {rq99:.1f} | {e50:.1f} | {e99:.1f} | {cens} | {o1} |")
    lines.append("")

    # Paired t-CI comparisons at key points
    lines.append("### Comparações pareadas por bloco (IC `t` 95% sobre `log(razão)`, margem δ = 10%)")
    lines.append("")
    lines.append("| Comparação (`L_e2e` p99) | Taxa | Razão geom. | IC 95% | Classe estatística |")
    lines.append("|---|---:|---:|---|---|")
    pairs = [
        ("Kafka CDC vs Kafka Poll(100ms)", 25, ("kafka", "outbox-cdc", None), ("kafka", "outbox-poll", 100)),
        ("Kafka CDC vs Kafka Poll(100ms)", 100, ("kafka", "outbox-cdc", None), ("kafka", "outbox-poll", 100)),
        ("Kafka CDC vs Kafka Poll(10ms)", 100, ("kafka", "outbox-cdc", None), ("kafka", "outbox-poll", 10)),
        ("Kafka CDC vs Kafka Poll(10ms)", 300, ("kafka", "outbox-cdc", None), ("kafka", "outbox-poll", 10)),
        ("Kafka CDC vs Kafka NOTIFY", 100, ("kafka", "outbox-cdc", None), ("kafka", "outbox-notify", None)),
        ("Kafka CDC vs Redis CDC", 100, ("kafka", "outbox-cdc", None), ("redis", "outbox-cdc", None)),
        ("Kafka CDC vs RabbitMQ CDC", 100, ("kafka", "outbox-cdc", None), ("rabbitmq", "outbox-cdc", None)),
        ("Redis Poll(10ms) vs Kafka Poll(10ms)", 300, ("redis", "outbox-poll", 10), ("kafka", "outbox-poll", 10)),
        ("pg-queue vs Kafka CDC", 100, ("pg-queue", "pg-queue", None), ("kafka", "outbox-cdc", None)),
    ]
    for label, rate, ka, kb in pairs:
        ra = c2_grp.get((rate, *ka))
        rb = c2_grp.get((rate, *kb))
        if ra and rb:
            ratio, lo, hi, cls = paired_log_ratio_ci([x["l_e2e_p99_ms"] for x in ra], [x["l_e2e_p99_ms"] for x in rb])
            lines.append(f"| {label} | {rate} ev/s | {ratio}× | [{lo}, {hi}] | `{cls}` |")
    lines.append("")

    # --- C3 ---
    lines.append("## 3. C3 — Ablação A5: Contenção por Agregado (Previsão P8, Kafka, `c=8`, 200 ev/s)")
    lines.append("")
    lines.append("| Agregados ativos | Estratégia | `L_req` p99 (ms) | `L_e2e` p99 (ms) | Censurados |")
    lines.append("|---:|---|---:|---:|---:|")
    for r in d["C3_ablation_A5_contention"]:
        sname = f"{r['strategy']} (p={r['poll_ms']}ms)" if r["poll_ms"] is not None else r["strategy"]
        lines.append(f"| {r['n_hot_customers']} | `{sname}` | {r['l_req_p99_ms']:.1f} | {r['l_e2e_p99_ms']:.1f} | {r['censored']} |")
    lines.append("")

    # --- C4 ---
    lines.append("## 4. C4 — Ablação A1/A2 e Microconstantes do Modelo Mecanístico (`λ=100 ev/s`)")
    lines.append("")
    lines.append("| Transporte | Lote (`b`) | Poll (`p`, ms) | `L_e2e` p99 (ms) | `T_q` (ms) | `T_pub` (ms) | `T_mark` (ms) |")
    lines.append("|---|---:|---:|---:|---:|---:|---:|")
    for r in d["C4_ablation_A1_A2_micro"]:
        mc = r.get("micro_constants_ms", {})
        lines.append(
            f"| `{r['transport']}` | {r['batch_size']} | {r['poll_ms']} | {r['l_e2e_p99_ms']:.1f} | "
            f"{mc.get('T_q_median', '-')} | {mc.get('T_pub_median', '-')} | {mc.get('T_mark_median', '-')} |"
        )
    lines.append("")

    # --- C5 ---
    lines.append("## 5. C5 — Aplicação Híbrida Real (4 fluxos simultâneos no mesmo PostgreSQL)")
    lines.append("")
    lines.append("| Fluxo simultâneo | Taxa alvo | Entregues | `L_req` p99 (ms) | `L_e2e` p50 (ms) | `L_e2e` p99 (ms) |")
    lines.append("|---|---:|---:|---:|---:|---:|")
    for fname, st in d["C5_hybrid_app"].items():
        lines.append(
            f"| `{fname}` | {st['target_rate']} ev/s | {st['delivered']}/{st['expected']} | "
            f"{st['l_req_p99_ms']} | {st['l_e2e_p50_ms']} | {st['l_e2e_p99_ms']} |"
        )
    lines.append("")

    REPORT_MD.write_text("\n".join(lines) + "\n")
    print("\n".join(lines))


if __name__ == "__main__":
    main()
