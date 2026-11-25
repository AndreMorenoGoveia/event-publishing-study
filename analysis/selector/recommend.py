#!/usr/bin/env python3
"""Seletor v2 de estrategia de publicacao e transporte guiado por metricas.

Calibrado diretamente com os dados empiricos das Ondas 1, 2 e 3 no Colab:
  - analysis/colab_campaign/results.json (Kafka 3.9, RabbitMQ, Redis, PG16 + fencing P6)
  - analysis/colab_wave2/results.json    (Kafka ate 3000 ev/s, NATS JetStream, fsync P4, G04/G08/G18)
  - analysis/colab_wave3/w3a..w3c.json   (TLS/JSON+zlib, Poison Pill HOL, PG Bloat, Lambda/ECS, Burst/Backlog/Dedup)

Uso:
  python3 analysis/selector/recommend.py --demo-hybrid
  python3 analysis/selector/recommend.py --spec meu_app.json
  python3 analysis/selector/recommend.py --rate 500 --slo-ms 50 --concurrency 16 \
      --durability K3 --order O1 --service S-log --runtime distributed --payload-kb 32 --fanout 4
"""

import argparse
import json
import statistics
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import List, Optional

ANALYSIS_DIR = Path(__file__).resolve().parent.parent
W1_JSON = ANALYSIS_DIR / "colab_campaign" / "results.json"
W2_JSON = ANALYSIS_DIR / "colab_wave2" / "results.json"


@dataclass
class FlowSpec:
    name: str
    rate_eps: float            # lambda (eventos/s)
    slo_e2e_p99_ms: float      # SLO de latencia ponta a ponta p99 (ms)
    writer_concurrency: int    # c (conexoes/escritores concorrentes no PG)
    durability: str            # K2 | K3 | K4
    order: str                 # O0 | O1
    service: str               # S-fila | S-log
    runtime: str               # lambda | ecs-single | distributed
    payload_kb: int = 1        # tamanho medio do payload em KiB (G04)
    fanout_groups: int = 1     # numero de consumer groups independentes (G08)
    allow_loss: bool = False   # False = exige E1 (at-least-once)


def load_empirical():
    data = {"w1": {}, "w2_high": {}, "g04": {}, "g08": {}, "g18": {}}
    if W1_JSON.exists():
        w1 = json.loads(W1_JSON.read_text())
        buckets = {}
        for r in w1.get("C2_multi_transport_frontier", []):
            k = (r["transport"], r["strategy"], r.get("poll_ms"), r["rate"])
            buckets.setdefault(k, []).append(r)
        for k, rows in buckets.items():
            data["w1"][k] = {
                "e2e_p50_ms": round(statistics.median(x["l_e2e_p50_ms"] for x in rows), 1),
                "e2e_p99_ms": round(statistics.median(x["l_e2e_p99_ms"] for x in rows), 1),
                "censored": sum(x["censored"] for x in rows),
                "o1_viol": sum(x["order_violations_O1"] for x in rows),
                "blocks": len(rows),
            }
    if W2_JSON.exists():
        w2 = json.loads(W2_JSON.read_text())
        for r in w2.get("W2_B_kafka_high_rate", []):
            data["w2_high"][(r["strategy"], r.get("poll_ms"), r["rate"])] = r
        for r in w2.get("W2_E_G04_payload", []):
            data["g04"][(r["transport"], r["payload_bytes"] // 1024)] = r["l_e2e_p99_ms"]
        for r in w2.get("W2_E_G08_fanout", []):
            data["g08"][(r["transport"], r["n_groups"])] = r["l_e2e_p99_ms"]
        data["g18"] = w2.get("W2_E_G18_replay", {})
    return data


EMP = load_empirical()


def choose_transport(flow: FlowSpec) -> tuple[str, List[str]]:
    notes = []

    if flow.service == "S-log" or flow.fanout_groups > 1:
        if flow.durability == "K4":
            notes.append("Atencao (W2-C/P4): fsync por commit custa 12x no Kafka e >200x no NATS em 500 ev/s; dimensione IOPS/NVMe.")
            return "Kafka (K4) / Redpanda (K4) / NATS JetStream (K4)", notes
        if flow.order == "O1":
            notes.append("Kafka oferece cercamento transacional de produtor (C1/P6: 0 inversoes sob F7).")
            return "Kafka (K3)", notes
        if flow.fanout_groups >= 4 and flow.rate_eps <= 500:
            notes.append("G08: com 4 grupos @ 300 ev/s, NATS JetStream ficou em 36,3 ms vs Kafka 99,2 ms; ambos imunes a payload ate 64 KiB (G04).")
            return "NATS JetStream (K3) ou Kafka (K3)", notes
        return "Kafka (K3) ou NATS JetStream (K3)", notes

    # S-fila
    if flow.runtime in ("lambda", "ecs-single") and flow.order == "O0" and flow.rate_eps <= 100 and flow.durability == "K2":
        notes.append("W0 + S-fila + O0: pg-queue elimina custo fixo de broker externo (L_e2e p99 ~13 ms em C2).")
        return "PostgreSQL (pg-queue)", notes

    if flow.runtime == "lambda":
        notes.append("Em Lambda, filas gerenciadas (SQS/EventBridge) + RDS Proxy (A-S2: reduz latencia PG em 3-4,7x) evitam cluster ocioso.")
        return "AWS SQS / SNS (gerenciado) ou NATS JetStream", notes

    if flow.payload_kb >= 32 or flow.rate_eps > 200:
        notes.append("G04/G11: RabbitMQ saturou >100 ev/s e triplicou p99 em 64 KiB (84,8 ms); prefira NATS JetStream ou Kafka.")
        return "NATS JetStream (K3) ou Kafka (K3)", notes

    if flow.durability == "K4":
        if flow.order == "O1":
            notes.append("C1/P6: RabbitMQ nao cerca produtor zumbi (1 inversao em H-A); use CDC pelo WAL ou guarda de versao no consumidor.")
        return "RabbitMQ quorum (K4)", notes

    return "RabbitMQ quorum (K4) ou NATS JetStream (K3)", notes


def recommend_flow(flow: FlowSpec) -> dict:
    eliminated = []
    warnings = []

    if not flow.allow_loss:
        eliminated.append(("dw-after / dw-before / cleanup=early", "Violam E1: perda (H-C, H-J) ou fantasma (H-D)."))

    can_pg_queue = True
    if flow.service == "S-log" or flow.fanout_groups > 1:
        can_pg_queue = False
        eliminated.append(("pg-queue", "Nao suporta S-log (fan-out nativo nem replay — G18)."))
    if flow.order == "O1":
        can_pg_queue = False
        eliminated.append(("pg-queue", "Entrega apenas O0 (6 violacoes @ 300 ev/s e 24 @ 600 ev/s em C2)."))
    if flow.rate_eps > 200 and flow.runtime == "distributed":
        can_pg_queue = False
        eliminated.append(("pg-queue", "Acopla consumo ao banco transacional em alta taxa."))

    can_notify = True
    if flow.runtime == "lambda":
        can_notify = False
        eliminated.append(("outbox-notify", "Lambda efemera nao mantem conexao persistente para LISTEN."))
    elif flow.writer_concurrency >= 16 or flow.rate_eps >= 250:
        can_notify = False
        eliminated.append(("outbox-notify", "Contencao no commit (M5/P3): queda de 40-49% (A-E2) e colapso em 300-600 ev/s (p99 570-6190 ms)."))

    if flow.durability in ("K3", "K4"):
        warnings.append(f"Cadeia {flow.durability} exige PostgreSQL com standby sincrono (PG2).")
    if flow.order == "O1":
        warnings.append("N21: O1 estrito com poison pills bloqueia a particao (p99 4364 ms vs 148 ms com DLQ); isole poison pills rapido.")
    if flow.runtime == "lambda":
        warnings.append("A-S2: use RDS Proxy / pooler (conexao efemera por tx aumentou latencia em 3-4,7x).")
    if flow.rate_eps >= 100:
        warnings.append("N09 (M6): use cleanup `partition-drop` (0 MB bloat, 7,7 ms) em vez de `mark_only` (51,9 MB, 152,9 ms).")
    if flow.payload_kb >= 16:
        warnings.append("N20: se trafego cross-AZ for caro, `json+zlib` reduz bytes em 63% por ~0,05 ms.")

    transport, t_notes = choose_transport(flow)
    warnings.extend(t_notes)

    # Strategy choice calibrated with lambda* ≈ 500 ev/s (W2-B / P1)
    if can_pg_queue and flow.runtime in ("ecs-single", "lambda") and flow.rate_eps <= 100 and flow.slo_e2e_p99_ms >= 25:
        strategy = "pg-queue"
        transport = "PostgreSQL (pg-queue)"
        rationale = "Taxa W0, S-fila e O0: pg-queue entrega ~13 ms p99 (C2) sem custo fixo de broker."
    elif flow.rate_eps >= 600:
        # Above lambda* (~500 ev/s in W2-B), batched polling beats single-threaded CDC!
        strategy = "outbox-poll (p=100ms, batch=200)"
        rationale = (
            "Acima do cruzamento lambda* (~500 ev/s em W2-B/P1), polling em lote supera CDC "
            "(em 1000 ev/s no Kafka: poll p=100ms 53,5 ms vs CDC 280,9 ms) e o sono p desaparece (P2)."
        )
    elif flow.slo_e2e_p99_ms < 50:
        if flow.writer_concurrency < 8 and can_notify and flow.runtime == "ecs-single":
            strategy = "outbox-notify (ou outbox-cdc)"
            rationale = "SLO < 50 ms com baixa concorrencia (c<8): NOTIFY evita operar CDC, mas CDC e mais estavel sob skew."
        else:
            strategy = "outbox-cdc"
            rationale = (
                "Abaixo de lambda* (~500 ev/s) com SLO < 50 ms: CDC via WAL logico entrega 13-25 ms p99 "
                "(vs ~110 ms no poll p=100ms), sem lock de NOTIFY e preservando ordem O1 pelo WAL."
            )
    else:
        poll_p = 500 if flow.slo_e2e_p99_ms >= 1000 else (100 if flow.slo_e2e_p99_ms >= 200 else 10)
        strategy = f"outbox-poll (p={poll_p}ms, batch=100)"
        rationale = f"SLO de {flow.slo_e2e_p99_ms:.0f} ms comporta polling p={poll_p} ms (mediana ~p/2, M1) com operacao minima."

    # Attach empirical snippet
    evidence = None
    if "Kafka" in transport and flow.rate_eps >= 500 and EMP["w2_high"]:
        st_k = "outbox-cdc" if "cdc" in strategy else "outbox-poll"
        p_k = None if st_k == "outbox-cdc" else (10 if "p=10ms" in strategy else 100)
        cands = [k for k in EMP["w2_high"] if k[0] == st_k and k[1] == p_k]
        if cands:
            best = min(cands, key=lambda k: abs(k[2] - flow.rate_eps))
            m = EMP["w2_high"][best]
            evidence = f"Onda 2B (`kafka`/`{st_k}` @ {best[2]} ev/s): L_e2e p50={m['l_e2e_p50_ms']}ms, p99={m['l_e2e_p99_ms']}ms, cens={m['censored']}"
    elif EMP["w1"]:
        tr_k = "pg-queue" if "PostgreSQL" in transport else ("kafka" if "Kafka" in transport else ("rabbitmq" if "RabbitMQ" in transport else "redis"))
        st_k = "pg-queue" if strategy == "pg-queue" else ("outbox-cdc" if "cdc" in strategy else "outbox-poll")
        p_k = None if st_k != "outbox-poll" else (10 if "p=10ms" in strategy else 100)
        cands = [k for k in EMP["w1"] if k[0] == tr_k and k[1] == st_k and k[2] == p_k]
        if cands:
            best = min(cands, key=lambda k: abs(k[3] - flow.rate_eps))
            m = EMP["w1"][best]
            evidence = f"Onda 1 (`{tr_k}`/`{st_k}` @ {best[3]} ev/s, R={m['blocks']}): L_e2e p50={m['e2e_p50_ms']}ms, p99={m['e2e_p99_ms']}ms, cens={m['censored']}, O1_viol={m['o1_viol']}"

    return {
        "flow": flow.name,
        "inputs": asdict(flow),
        "recommended_strategy": strategy,
        "recommended_transport": transport,
        "rationale": rationale,
        "empirical_evidence": evidence,
        "warnings": warnings,
        "eliminated": [{"option": k, "reason": v} for k, v in eliminated],
    }


DEMO_HYBRID_FLOWS = [
    FlowSpec("faturamento-contabil", 150, 40, 16, "K3", "O1", "S-log", "distributed", payload_kb=2, fanout_groups=2),
    FlowSpec("telemetria-pedidos", 1000, 100, 32, "K3", "O0", "S-log", "distributed", payload_kb=32, fanout_groups=4),
    FlowSpec("notificacao-email", 20, 2000, 4, "K2", "O0", "S-fila", "ecs-single", payload_kb=1, fanout_groups=1),
    FlowSpec("webhook-parceiros", 40, 500, 4, "K2", "O0", "S-fila", "lambda", payload_kb=4, fanout_groups=1),
]


def print_markdown(results: List[dict]):
    print("| Fluxo | Métricas (`λ`, SLO, `c`, K/O/S, runtime, payload, fan-out) | Estratégia recomendada | Transporte |")
    print("|---|---|---|---|")
    for r in results:
        i = r["inputs"]
        inp = f"{i['rate_eps']} ev/s · {i['slo_e2e_p99_ms']}ms · c={i['writer_concurrency']} · {i['durability']}/{i['order']}/{i['service']} · {i['runtime']} · {i['payload_kb']}KiB · {i['fanout_groups']}g"
        print(f"| `{r['flow']}` | {inp} | **{r['recommended_strategy']}** | {r['recommended_transport']} |")
    print()
    for r in results:
        print(f"### `{r['flow']}`")
        print(f"- **Por quê:** {r['rationale']}")
        if r.get("empirical_evidence"):
            print(f"- **Evidência empírica:** {r['empirical_evidence']}")
        for w in r["warnings"]:
            print(f"- **Atenção:** {w}")
        if r["eliminated"]:
            print("- **Eliminadas:** " + "; ".join(f"`{e['option']}` ({e['reason']})" for e in r["eliminated"]))
        print()


def main():
    ap = argparse.ArgumentParser(description="Seletor v2 de estrategia e transporte por metricas.")
    ap.add_argument("--demo-hybrid", action="store_true")
    ap.add_argument("--spec", type=str)
    ap.add_argument("--json", action="store_true")
    ap.add_argument("--name", default="fluxo-unico")
    ap.add_argument("--rate", type=float, default=50.0)
    ap.add_argument("--slo-ms", type=float, default=200.0)
    ap.add_argument("--concurrency", type=int, default=4)
    ap.add_argument("--durability", choices=["K2", "K3", "K4"], default="K2")
    ap.add_argument("--order", choices=["O0", "O1"], default="O0")
    ap.add_argument("--service", choices=["S-fila", "S-log"], default="S-fila")
    ap.add_argument("--runtime", choices=["lambda", "ecs-single", "distributed"], default="ecs-single")
    ap.add_argument("--payload-kb", type=int, default=1)
    ap.add_argument("--fanout", type=int, default=1)
    args = ap.parse_args()

    if args.demo_hybrid:
        flows = DEMO_HYBRID_FLOWS
    elif args.spec:
        raw = json.loads(Path(args.spec).read_text())
        flows = [FlowSpec(**item) for item in (raw if isinstance(raw, list) else [raw])]
    else:
        flows = [FlowSpec(args.name, args.rate, args.slo_ms, args.concurrency, args.durability, args.order, args.service, args.runtime, args.payload_kb, args.fanout)]

    results = [recommend_flow(f) for f in flows]
    if args.json:
        print(json.dumps(results, indent=2, ensure_ascii=False))
    else:
        print_markdown(results)


if __name__ == "__main__":
    main()
