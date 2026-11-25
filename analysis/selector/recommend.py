#!/usr/bin/env python3
"""Seletor de estrategia de publicacao e transporte guiado por metricas.

Permite avaliar tanto um fluxo isolado quanto uma aplicacao hibrida que combina
diferentes metodos/fluxos de eventos, respeitando os contratos K/E/O/S (doc 12),
os mecanismos M1-M12 (doc 13) e as evidencias coletadas em analysis/colab_slice/.

Uso:
  python3 analysis/selector/recommend.py --demo-hybrid
  python3 analysis/selector/recommend.py --spec meu_app.json
  python3 analysis/selector/recommend.py --rate 50 --slo-ms 500 --concurrency 4 \
      --durability K2 --order O0 --service S-fila --runtime ecs-single
"""

import argparse
import json
import statistics
import sys
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import List, Optional

CAMPAIGN_JSON = Path(__file__).resolve().parent.parent / "colab_campaign" / "results.json"


def load_empirical_lookup():
    """Loads median measured metrics from analysis/colab_campaign/results.json if present."""
    if not CAMPAIGN_JSON.exists():
        return {}
    d = json.loads(CAMPAIGN_JSON.read_text())
    by_key = {}
    buckets = {}
    for r in d.get("C2_multi_transport_frontier", []):
        k = (r["transport"], r["strategy"], r.get("poll_ms"), r["rate"])
        buckets.setdefault(k, []).append(r)
    for k, rows in buckets.items():
        by_key[k] = {
            "e2e_p50_ms": round(statistics.median(x["l_e2e_p50_ms"] for x in rows), 1),
            "e2e_p99_ms": round(statistics.median(x["l_e2e_p99_ms"] for x in rows), 1),
            "req_p99_ms": round(statistics.median(x["l_req_p99_ms"] for x in rows), 1),
            "censored_total": sum(x["censored"] for x in rows),
            "o1_violations": sum(x["order_violations_O1"] for x in rows),
            "blocks": len(rows),
        }
    return by_key


EMPIRICAL = load_empirical_lookup()


def lookup_evidence(strategy: str, transport: str, rate_eps: float) -> Optional[str]:
    if not EMPIRICAL:
        return None
    # Map recommendation strings to measured keys
    tr = "pg-queue" if "PostgreSQL" in transport else (
        "kafka" if "Kafka" in transport else (
            "rabbitmq" if "RabbitMQ" in transport else (
                "redis" if "Redis" in transport else None
            )
        )
    )
    if tr is None:
        return None
    if strategy.startswith("pg-queue"):
        st, p_ms = "pg-queue", None
    elif strategy.startswith("outbox-cdc"):
        st, p_ms = "outbox-cdc", None
    elif strategy.startswith("outbox-notify"):
        st, p_ms = "outbox-notify", None
    elif strategy.startswith("outbox-poll"):
        st = "outbox-poll"
        p_ms = 10 if "p=10ms" in strategy else 100
    else:
        return None
    # Find closest measured rate for (tr, st, p_ms)
    candidates = [k for k in EMPIRICAL if k[0] == tr and k[1] == st and k[2] == p_ms]
    if not candidates:
        return None
    best = min(candidates, key=lambda k: abs(k[3] - rate_eps))
    m = EMPIRICAL[best]
    p_lbl = f", p={p_ms}ms" if p_ms is not None else ""
    return (
        f"Medido no Colab (`{tr}`/`{st}`{p_lbl} @ {best[3]} ev/s, R={m['blocks']}): "
        f"L_e2e p50={m['e2e_p50_ms']}ms, p99={m['e2e_p99_ms']}ms, L_req p99={m['req_p99_ms']}ms, "
        f"censurados={m['censored_total']}, violacoes_O1={m['o1_violations']}"
    )


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
    allow_loss: bool = False   # False = exige E1 (at-least-once)


def choose_transport(flow: FlowSpec) -> tuple[str, List[str]]:
    notes = []
    if flow.service == "S-log":
        if flow.durability == "K4":
            return "Redpanda / Pulsar (K4) ou Kafka+fsync", notes
        if flow.order == "O1":
            notes.append("Kafka suporta cercamento transacional de produtor (fencing em 2 camadas para O1 sob F7).")
        return "Kafka (K3) ou NATS JetStream (K3)", notes

    # S-fila
    if flow.runtime in ("lambda", "ecs-single") and flow.order == "O0" and flow.rate_eps <= 100 and flow.durability == "K2":
        notes.append("Sem fan-out/replay e em taxa baixa (W0), pg-queue elimina o custo fixo de broker externo.")
        return "PostgreSQL (pg-queue) ou SQS/RabbitMQ", notes

    if flow.runtime == "lambda":
        notes.append("Em Lambda, filas gerenciadas (SQS/EventBridge) evitam custo fixo de cluster dedicado.")
        return "AWS SQS / SNS (gerenciado) ou RabbitMQ quorum", notes

    if flow.durability == "K4":
        if flow.order == "O1":
            notes.append("Atencao (H-A/P6): RabbitMQ nao tem cercamento de produtor obsoleto; use CDC pelo WAL ou guarda de versao no consumidor.")
        return "RabbitMQ quorum (K4)", notes

    if flow.order == "O1":
        notes.append("Para O1 estrito sob F7 com relay distribuido, Kafka (com transactional.id) protege melhor que S-fila sem fencing.")
    return "RabbitMQ quorum (K4) / SQS FIFO", notes


def recommend_flow(flow: FlowSpec) -> dict:
    eliminated = []
    warnings = []

    if not flow.allow_loss:
        eliminated.append(("dw-after / dw-before / cleanup=early", "Violam E1: perda confirmada (H-C, H-J) ou fantasma (H-D)."))

    # Eligibility checks
    can_pg_queue = True
    if flow.service == "S-log":
        can_pg_queue = False
        eliminated.append(("pg-queue", "Nao suporta S-log (fan-out nativo e replay)."))
    if flow.order == "O1":
        can_pg_queue = False
        eliminated.append(("pg-queue", "Entrega apenas O0 sob concorrencia (6 violacoes O1 em A-E1)."))
    if flow.rate_eps > 200 and flow.runtime == "distributed":
        can_pg_queue = False
        eliminated.append(("pg-queue", "Acopla escala de consumo ao banco transacional em alta taxa."))

    can_notify = True
    if flow.runtime == "lambda":
        can_notify = False
        eliminated.append(("outbox-notify", "Lambda efemera nao mantem conexao persistente para LISTEN."))
    elif flow.writer_concurrency >= 16:
        can_notify = False
        eliminated.append(("outbox-notify", f"Contencao severa no commit (M5/P3): queda de 40-49% em c={flow.writer_concurrency} (A-E2)."))
    elif flow.writer_concurrency >= 8:
        warnings.append("LISTEN/NOTIFY ja degrada throughput transacional em c>=8 (M5/P3).")

    # PG durability chain rule (doc 12 §8)
    if flow.durability in ("K3", "K4"):
        warnings.append(f"Contrato {flow.durability} na cadeia exige PostgreSQL com standby sincrono (PG2); PG1 sozinho limita a cadeia a K2.")

    transport, t_notes = choose_transport(flow)
    warnings.extend(t_notes)

    # Select primary strategy
    if can_pg_queue and flow.runtime in ("ecs-single", "lambda") and flow.rate_eps <= 100 and flow.slo_e2e_p99_ms >= 25:
        strategy = "pg-queue"
        transport = "PostgreSQL (sem broker externo)"
        rationale = (
            "Taxa baixa (W0), S-fila e O0: pg-queue entregou L_e2e p50 ~10 ms (p99 17-35 ms em A-S1) "
            "com menor custo fixo e menor complexidade operacional."
        )
    elif flow.slo_e2e_p99_ms < 50:
        if flow.writer_concurrency < 8 and can_notify and flow.runtime == "ecs-single":
            strategy = "outbox-notify (ou outbox-cdc)"
            rationale = (
                "SLO apertado (<50 ms) com baixa concorrencia (c<8): NOTIFY evita operar CDC, "
                "mas monitore p99 sob skew (A-E1 mostrou CDC mais estavel: 13-21 ms vs 22-115 ms)."
            )
        else:
            strategy = "outbox-cdc"
            rationale = (
                "SLO < 50 ms com concorrencia/escala: CDC via WAL logico evita o piso p/2 do polling (M1), "
                "evita o lock global do NOTIFY (M5) e preserva ordem O1 pelo WAL."
            )
    else:
        # SLO >= 50 ms -> outbox-poll is viable and operationally simplest
        if flow.slo_e2e_p99_ms >= 1000:
            poll_p = 500
        elif flow.slo_e2e_p99_ms >= 200:
            poll_p = 100
        else:
            poll_p = 10
        strategy = f"outbox-poll (p={poll_p}ms, batch)"
        if flow.rate_eps >= 300:
            rationale = (
                "Em taxa alta (regime saturado, P2), o relay nao dorme e o intervalo p deixa de dominar a latencia; "
                "polling em lote evita o custo de manter slot WAL/Debezium e o lock de NOTIFY."
            )
        else:
            rationale = (
                f"SLO de {flow.slo_e2e_p99_ms:.0f} ms comporta polling com p={poll_p} ms (espera mediana ~p/2 = {poll_p/2:.0f} ms, M1), "
                "com impacto minimo em L_req (<0,5 ms sobre tx-only em A-S1) e operacao simples."
            )

    # Fencing note when O1 + polling/notify
    if flow.order == "O1" and "outbox-poll" in strategy or (flow.order == "O1" and "outbox-notify" in strategy):
        warnings.append(
            "Para O1 com multiplos relays, use partition-lease + epoch no banco + fencing no broker (Kafka) "
            "ou guarda de aggregate_version no consumidor (H-A/P6)."
        )

    evidence = lookup_evidence(strategy, transport, flow.rate_eps)

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
    FlowSpec(
        name="faturamento-contabil",
        rate_eps=150,
        slo_e2e_p99_ms=40,
        writer_concurrency=16,
        durability="K3",
        order="O1",
        service="S-log",
        runtime="distributed",
    ),
    FlowSpec(
        name="notificacao-email",
        rate_eps=20,
        slo_e2e_p99_ms=2000,
        writer_concurrency=4,
        durability="K2",
        order="O0",
        service="S-fila",
        runtime="ecs-single",
    ),
    FlowSpec(
        name="webhook-parceiros",
        rate_eps=40,
        slo_e2e_p99_ms=500,
        writer_concurrency=4,
        durability="K2",
        order="O0",
        service="S-fila",
        runtime="lambda",
    ),
    FlowSpec(
        name="auditoria-replay",
        rate_eps=500,
        slo_e2e_p99_ms=1000,
        writer_concurrency=32,
        durability="K3",
        order="O0",
        service="S-log",
        runtime="distributed",
    ),
]


def print_markdown(results: List[dict]):
    print("| Fluxo | Entrada (`λ`, SLO, `c`, K/O/S, runtime) | Estratégia recomendada | Transporte |")
    print("|---|---|---|---|")
    for r in results:
        i = r["inputs"]
        inp = f"{i['rate_eps']} ev/s · {i['slo_e2e_p99_ms']}ms · c={i['writer_concurrency']} · {i['durability']}/{i['order']}/{i['service']} · {i['runtime']}"
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
            elims = "; ".join(f"`{e['option']}` ({e['reason']})" for e in r["eliminated"])
            print(f"- **Eliminadas:** {elims}")
        print()


def main():
    ap = argparse.ArgumentParser(description="Seletor de estrategia e transporte por metricas.")
    ap.add_argument("--demo-hybrid", action="store_true", help="Executa exemplo de aplicacao hibrida com 4 fluxos.")
    ap.add_argument("--spec", type=str, help="Arquivo JSON com lista de fluxos (FlowSpec).")
    ap.add_argument("--json", action="store_true", help="Saida em JSON.")
    ap.add_argument("--name", default="fluxo-unico")
    ap.add_argument("--rate", type=float, default=50.0)
    ap.add_argument("--slo-ms", type=float, default=200.0)
    ap.add_argument("--concurrency", type=int, default=4)
    ap.add_argument("--durability", choices=["K2", "K3", "K4"], default="K2")
    ap.add_argument("--order", choices=["O0", "O1"], default="O0")
    ap.add_argument("--service", choices=["S-fila", "S-log"], default="S-fila")
    ap.add_argument("--runtime", choices=["lambda", "ecs-single", "distributed"], default="ecs-single")
    args = ap.parse_args()

    if args.demo_hybrid:
        flows = DEMO_HYBRID_FLOWS
    elif args.spec:
        with open(args.spec) as f:
            raw = json.load(f)
        flows = [FlowSpec(**item) for item in (raw if isinstance(raw, list) else [raw])]
    else:
        flows = [
            FlowSpec(
                name=args.name,
                rate_eps=args.rate,
                slo_e2e_p99_ms=args.slo_ms,
                writer_concurrency=args.concurrency,
                durability=args.durability,
                order=args.order,
                service=args.service,
                runtime=args.runtime,
            )
        ]

    results = [recommend_flow(f) for f in flows]
    if args.json:
        print(json.dumps(results, indent=2, ensure_ascii=False))
    else:
        print_markdown(results)


if __name__ == "__main__":
    main()
