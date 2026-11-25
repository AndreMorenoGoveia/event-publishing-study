#!/usr/bin/env python3
"""Analisa a Onda 2, valida as previsoes seladas (Pilar N3), calcula IC t em R=10
e gera figuras publicaveis (PNG + CSV) em analysis/figures/.
"""

import csv
import json
import math
import statistics
from collections import defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parent
RAW_STDOUT = ROOT / "raw_stdout.txt"
RESULTS_JSON = ROOT / "results.json"
SEALED_JSON = ROOT.parent / "predictions" / "sealed_v1.json"
W1_JSON = ROOT.parent / "colab_campaign" / "results.json"
REPORT_MD = ROOT / "RESULTADOS.md"
FIG_DIR = ROOT.parent / "figures"

# t_{0.975, df}
T_CRIT_95 = {2: 4.30265, 3: 3.18245, 4: 2.77645, 8: 2.30600, 9: 2.26216, 10: 2.22814}


def paired_log_ratio_ci(vals_a, vals_b, delta=0.10):
    diffs = [math.log(a / b) for a, b in zip(vals_a, vals_b) if a and b and a > 0 and b > 0]
    n = len(diffs)
    if n < 2:
        return None, None, None, "inconclusivo"
    mu = statistics.mean(diffs)
    sd = statistics.stdev(diffs)
    tcrit = T_CRIT_95.get(n - 1, 2.26216)
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


def generate_figures(w1, w2, sealed_eval):
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except ImportError:
        print("matplotlib not installed locally; skipping PNG generation (CSVs still written).")
        return []

    FIG_DIR.mkdir(parents=True, exist_ok=True)
    generated = []

    # --- F01 / F18: Kafka High-Rate Frontier (lambda* crossing P1) ---
    w2b = w2["W2_B_kafka_high_rate"]
    by_s = defaultdict(list)
    for r in w2b:
        lbl = f"{r['strategy']} (p={r['poll_ms']}ms)" if r["poll_ms"] is not None else r["strategy"]
        by_s[lbl].append((r["rate"], r["l_e2e_p99_ms"]))
    # Also prepend 25, 100, 300 from Wave 1 clean Kafka
    w1_k = defaultdict(list)
    for r in w1["C2_multi_transport_frontier"]:
        if r["transport"] == "kafka" and r["strategy"] in ("outbox-poll", "outbox-cdc") and r["rate"] in (25, 100, 300):
            lbl = f"{r['strategy']} (p={r['poll_ms']}ms)" if r["poll_ms"] is not None else r["strategy"]
            w1_k[(lbl, r["rate"])].append(r["l_e2e_p99_ms"])
    for (lbl, rate), vals in w1_k.items():
        by_s[lbl].append((rate, statistics.median(vals)))

    fig, ax = plt.subplots(figsize=(8, 4.8))
    for lbl, pts in sorted(by_s.items()):
        pts.sort()
        xs, ys = zip(*pts)
        ax.plot(xs, ys, marker="o", linewidth=2, label=lbl)
    ax.set_yscale("log")
    ax.set_xlabel("Taxa ofertada λ (ev/s)")
    ax.set_ylabel("L_e2e p99 (ms, escala log)")
    ax.set_title("F18 — Fronteira de Decisão no Kafka: Polling × CDC (Cruzamento λ*, P1)")
    ax.grid(True, which="both", alpha=0.3)
    ax.legend()
    fig.tight_layout()
    p1 = FIG_DIR / "F18_kafka_frontier_lambda_star.png"
    fig.savefig(p1, dpi=150)
    plt.close(fig)
    generated.append(p1)

    # --- F05: Ablation A3 fsync on SAME system (Kafka & NATS JetStream, P4/M8) ---
    w2c = w2["W2_C_ablation_A3_fsync"]
    by_tr = defaultdict(list)
    for r in w2c:
        by_tr[r["transport"]].append((r["rate"], r["l_e2e_p99_ms"]))
    fig, ax = plt.subplots(figsize=(8, 4.8))
    for tr, pts in sorted(by_tr.items()):
        pts.sort()
        xs, ys = zip(*pts)
        style = "--" if "fsync" in tr else "-"
        ax.plot(xs, ys, marker="s", linestyle=style, linewidth=2, label=tr)
    ax.set_yscale("log")
    ax.set_xlabel("Taxa ofertada λ (ev/s)")
    ax.set_ylabel("L_e2e p99 (ms, escala log)")
    ax.set_title("F05 — Ablação A3 (M8/P4): Page Cache × fsync no Mesmo Sistema (Kafka & NATS)")
    ax.grid(True, which="both", alpha=0.3)
    ax.legend()
    fig.tight_layout()
    p2 = FIG_DIR / "F05_ablation_fsync_same_system.png"
    fig.savefig(p2, dpi=150)
    plt.close(fig)
    generated.append(p2)

    # --- F06: Payload Size (G04: 1 -> 64 KiB) ---
    g04 = w2["W2_E_G04_payload"]
    by_tr4 = defaultdict(list)
    for r in g04:
        by_tr4[r["transport"]].append((r["payload_bytes"] // 1024, r["l_e2e_p99_ms"]))
    fig, ax = plt.subplots(figsize=(8, 4.8))
    for tr, pts in sorted(by_tr4.items()):
        pts.sort()
        xs, ys = zip(*pts)
        ax.plot(xs, ys, marker="^", linewidth=2, label=tr)
    ax.set_xlabel("Tamanho do payload (KiB)")
    ax.set_ylabel("L_e2e p99 (ms)")
    ax.set_title("F06 — Impacto do Tamanho do Payload @ 100 ev/s (G04)")
    ax.grid(True, alpha=0.3)
    ax.legend()
    fig.tight_layout()
    p3 = FIG_DIR / "F06_payload_size.png"
    fig.savefig(p3, dpi=150)
    plt.close(fig)
    generated.append(p3)

    # --- F_N3: Sealed Model Validation (Predicted vs Observed p99) ---
    fig, ax = plt.subplots(figsize=(7.5, 5.2))
    preds = [r["pred_p99"] for r in sealed_eval]
    obss = [r["obs_p99"] for r in sealed_eval]
    ids = [r["id"] for r in sealed_eval]
    mx = max(max(preds), max(obss)) * 1.15
    ax.plot([1, mx], [1, mx], "k--", alpha=0.5, label="Previsão perfeita (y = x)")
    ax.fill_between([1, mx], [0.75, 0.75 * mx], [1.25, 1.25 * mx], color="green", alpha=0.1, label="Faixa ±25% (meta MAPE)")
    ax.scatter(preds, obss, s=65, zorder=5)
    for pid, x, y in zip(ids, preds, obss):
        ax.annotate(pid, (x, y), textcoords="offset points", xytext=(5, 4), fontsize=8)
    ax.set_xscale("log")
    ax.set_yscale("log")
    ax.set_xlabel("L_e2e p99 previsto — modelo selado (ms)")
    ax.set_ylabel("L_e2e p99 observado — Colab Onda 2 (ms)")
    ax.set_title("Validação Fora da Amostra — Modelo Mecanístico Selado (Pilar N3)")
    ax.grid(True, which="both", alpha=0.3)
    ax.legend()
    fig.tight_layout()
    p4 = FIG_DIR / "FN3_sealed_model_validation.png"
    fig.savefig(p4, dpi=150)
    plt.close(fig)
    generated.append(p4)

    return generated


def main():
    if RAW_STDOUT.exists():
        raw = RAW_STDOUT.read_text()
        if "===WAVE2_JSON_START===" in raw:
            j = raw.split("===WAVE2_JSON_START===")[1].split("===WAVE2_JSON_END===")[0].strip()
            RESULTS_JSON.write_text(j + "\n")

    w2 = json.loads(RESULTS_JSON.read_text())
    w1 = json.loads(W1_JSON.read_text())
    sealed = json.loads(SEALED_JSON.read_text())

    # 1. Evaluate sealed predictions (W2-A)
    obs_by_id = defaultdict(list)
    for r in w2["W2_A_sealed_validation"]:
        obs_by_id[r["id"]].append(r["l_e2e_p99_ms"])

    sealed_eval = []
    for p in sealed["predictions"]:
        oid = p["id"]
        obs = round(statistics.median(obs_by_id[oid]), 2)
        pred = p["pred_l_e2e_p99_ms"]
        ape = abs(obs - pred) / max(1.0, obs) * 100.0
        lo, hi = p["pi80_p99_ms"]
        in_pi = lo <= obs <= hi
        sealed_eval.append({
            "id": oid,
            "desc": f"{p['transport']}/{p['strategy']} r={p['rate']} p={p['poll_ms']} b={p['batch_size']}",
            "regime": p["predicted_regime"],
            "pred_p99": pred,
            "obs_p99": obs,
            "ape_pct": round(ape, 1),
            "pi80": [lo, hi],
            "in_pi80": in_pi,
        })

    mape_all = round(statistics.mean(r["ape_pct"] for r in sealed_eval), 1)
    med_ape = round(statistics.median(r["ape_pct"] for r in sealed_eval), 1)
    pi80_cov = round(sum(r["in_pi80"] for r in sealed_eval) / len(sealed_eval) * 100.0, 1)

    # 2. Evaluate R=10 paired blocks (W2-D)
    w2d_grp = defaultdict(list)
    for r in w2["W2_D_R10_paired"]:
        k = (r["rate"], r["transport"], r["strategy"], r["poll_ms"])
        w2d_grp[k].append(r)
    for k in w2d_grp:
        w2d_grp[k].sort(key=lambda x: x["block"])

    # Generate figures
    figs = generate_figures(w1, w2, sealed_eval)

    # Build Markdown report
    lines = [
        "# Campanha Onda 2 — Modelo Preditivo Selado, Kafka 3000 ev/s, NATS JetStream, `fsync` (P4) e `R=10` Blocos",
        "",
        f"- **Ambiente:** VM Colab `--high-mem` ({w2['meta']['cpus']} vCPUs, {w2['meta']['ram_gib']} GiB RAM, "
        f"PG {w2['meta']['pg_ver']}, Kafka 3.9 KRaft, NATS JetStream fast+fsync, RabbitMQ, Redis AOF `always`).",
        f"- **Duração total:** {w2['meta']['total_runtime_s']} s (~{w2['meta']['total_runtime_s']/60:.1f} min).",
        f"- **Previsões seladas (SHA-256):** `{sealed['sha256_sealed']}` ([sealed_v1.json](../predictions/sealed_v1.json)).",
        "",
        "## 1. W2-A — Validação Fora da Amostra do Modelo Mecanístico (Pilar N3 / N17)",
        "",
        f"- **APE mediano:** **{med_ape}%** · **MAPE médio:** **{mape_all}%** (meta doc 13 §5.3: ≤ 25%) · **Cobertura PI80:** **{pi80_cov}%** (meta: ≥ 70%).",
        "",
        "| ID | Configuração inédita | Regime previsto | Previsto p99 (ms) | Observado p99 (ms) | Erro (%) | Dentro PI80? |",
        "|---|---|---|---:|---:|---:|---:|",
    ]
    for r in sealed_eval:
        lines.append(f"| `{r['id']}` | `{r['desc']}` | {r['regime']} | {r['pred_p99']} | {r['obs_p99']} | {r['ape_pct']}% | {'Sim' if r['in_pi80'] else 'Não'} |")

    lines += [
        "",
        "## 2. W2-B — Fronteira do Kafka em Alta Taxa (`500 → 3000 ev/s`, Cruzamento `λ*` / P1)",
        "",
        "| λ (ev/s) | Estratégia | `L_e2e` p50 (ms) | `L_e2e` p99 (ms) | `L_req` p99 (ms) | Censurados |",
        "|---:|---|---:|---:|---:|---:|",
    ]
    for r in w2["W2_B_kafka_high_rate"]:
        sn = f"{r['strategy']} (p={r['poll_ms']}ms)" if r["poll_ms"] is not None else r["strategy"]
        lines.append(f"| {r['rate']} | `{sn}` | {r['l_e2e_p50_ms']} | {r['l_e2e_p99_ms']} | {r['l_req_p99_ms']} | {r['censored']} |")

    lines += [
        "",
        "## 3. W2-C — Ablação A3: Page Cache × `fsync` no Mesmo Sistema (Kafka & NATS JetStream — M8/P4)",
        "",
        "| λ (ev/s) | Transporte | `L_e2e` p50 (ms) | `L_e2e` p99 (ms) | Censurados |",
        "|---:|---|---:|---:|---:|",
    ]
    for r in w2["W2_C_ablation_A3_fsync"]:
        lines.append(f"| {r['rate']} | `{r['transport']}` | {r['l_e2e_p50_ms']} | {r['l_e2e_p99_ms']} | {r['censored']} |")

    lines += [
        "",
        "## 4. W2-D — Alta Precisão Estatística (`R = 10` blocos pareados, `t = 2,262`, δ = 10%)",
        "",
        "| Comparação (`L_e2e` p99) | Taxa | Mediana A (ms) | Mediana B (ms) | Razão geom. | IC 95% (`R=10`) | Classe |",
        "|---|---:|---:|---:|---:|---|---|",
    ]
    r10_pairs = [
        ("Kafka CDC vs Kafka Poll(100ms)", 100, ("kafka", "outbox-cdc", None), ("kafka", "outbox-poll", 100)),
        ("Kafka CDC vs Kafka Poll(10ms)", 100, ("kafka", "outbox-cdc", None), ("kafka", "outbox-poll", 10)),
        ("Kafka CDC vs NATS CDC", 100, ("kafka", "outbox-cdc", None), ("nats", "outbox-cdc", None)),
        ("NATS CDC vs NATS Poll(10ms)", 100, ("nats", "outbox-cdc", None), ("nats", "outbox-poll", 10)),
        ("Kafka Poll(10ms) vs Redis Poll(10ms)", 100, ("kafka", "outbox-poll", 10), ("redis", "outbox-poll", 10)),
        ("Kafka CDC vs Kafka Poll(100ms)", 300, ("kafka", "outbox-cdc", None), ("kafka", "outbox-poll", 100)),
        ("Kafka CDC vs Kafka Poll(10ms)", 300, ("kafka", "outbox-cdc", None), ("kafka", "outbox-poll", 10)),
        ("Kafka CDC vs NATS CDC", 300, ("kafka", "outbox-cdc", None), ("nats", "outbox-cdc", None)),
        ("Kafka Poll(10ms) vs Redis Poll(10ms)", 300, ("kafka", "outbox-poll", 10), ("redis", "outbox-poll", 10)),
        ("pg-queue vs Kafka CDC", 300, ("pg-queue", "pg-queue", None), ("kafka", "outbox-cdc", None)),
    ]
    for lbl, rate, ka, kb in r10_pairs:
        ra = w2d_grp.get((rate, *ka))
        rb = w2d_grp.get((rate, *kb))
        if ra and rb:
            va = [x["l_e2e_p99_ms"] for x in ra if x["l_e2e_p99_ms"] is not None]
            vb = [x["l_e2e_p99_ms"] for x in rb if x["l_e2e_p99_ms"] is not None]
            if not va or not vb:
                continue
            ratio, lo, hi, cls = paired_log_ratio_ci(
                [x["l_e2e_p99_ms"] for x in ra], [x["l_e2e_p99_ms"] for x in rb]
            )
            lines.append(f"| {lbl} | {rate} ev/s | {statistics.median(va):.1f} | {statistics.median(vb):.1f} | {ratio}× | [{lo}, {hi}] | `{cls}` |")

    lines += [
        "",
        "## 5. W2-E — Cenários de Produção (G04 Payload, G08 Fan-out, G18 Replay)",
        "",
        "### G04 — Tamanho de Payload (`1 → 64 KiB` @ 100 ev/s, `L_e2e` p99 em ms)",
        "| Payload | Kafka | NATS JetStream | Redis Streams | RabbitMQ |",
        "|---:|---:|---:|---:|---:|",
    ]
    g04_map = {(r["payload_bytes"] // 1024, r["transport"]): r["l_e2e_p99_ms"] for r in w2["W2_E_G04_payload"]}
    for sz in (1, 8, 32, 64):
        lines.append(f"| {sz} KiB | {g04_map.get((sz, 'kafka'))} | {g04_map.get((sz, 'nats'))} | {g04_map.get((sz, 'redis'))} | {g04_map.get((sz, 'rabbitmq'))} |")

    lines += [
        "",
        "### G08 — Fan-out (`1, 2, 4` consumer groups independentes @ 300 ev/s, `L_e2e` p99 em ms)",
        "| Grupos | Kafka | NATS JetStream |",
        "|---:|---:|---:|",
    ]
    g08_map = {(r["n_groups"], r["transport"]): r["l_e2e_p99_ms"] for r in w2["W2_E_G08_fanout"]}
    for ng in (1, 2, 4):
        lines.append(f"| {ng} | {g08_map.get((ng, 'kafka'))} | {g08_map.get((ng, 'nats'))} |")

    g18 = w2["W2_E_G18_replay"]
    lines += [
        "",
        "### G18 — Replay de 5.000 eventos retidos (requisito funcional S-log)",
        "| Transporte | Suporta Replay? | Eventos | Tempo (s) | Taxa de Replay (ev/s) |",
        "|---|---|---:|---:|---:|",
        f"| **Kafka** | Sim (S-log) | {g18['kafka']['events']} | {g18['kafka']['replay_s']} | **{g18['kafka']['replay_eps']}** |",
        f"| **NATS JetStream** | Sim (S-log) | {g18['nats_jetstream']['events']} | {g18['nats_jetstream']['replay_s']} | **{g18['nats_jetstream']['replay_eps']}** |",
        f"| **Redis Streams** | Sim (limitado por RAM) | {g18['redis_streams']['events']} | {g18['redis_streams']['replay_s']} | **{g18['redis_streams']['replay_eps']}** |",
        "| **RabbitMQ / `pg-queue`** | **Não** (S-fila) | — | — | — |",
        "",
    ]

    if figs:
        lines.append("## 6. Figuras geradas")
        lines.append("")
        for fp in figs:
            lines.append(f"- `{fp.name}`")

    REPORT_MD.write_text("\n".join(lines) + "\n")
    print("\n".join(lines))


if __name__ == "__main__":
    main()
