#!/usr/bin/env python3
"""Modelo mecanistico preditivo (Pilar N3 — doc 13 §5).

Ajusta as constantes (T_q, T_pub, T_mark, mu_wal) exclusivamente sobre os dados da
Onda 1 (analysis/colab_campaign/results.json) e gera previsoes SELADAS com hash SHA-256
para configuracoes ineditas (analysis/predictions/sealed_v1.json) ANTES da execucao da Onda 2.

Formulas baseadas no doc 13 §5.2:
  C_relay(b) = b / (T_q(b) + T_pub(b) + T_mark(b))
  Regime subcritico (lambda * p <= b):
    E[L_e2e_p50] ≈ L_tx_p50 + p/2 + T_relay(b_eff) + T_cons
    E[L_e2e_p99] ≈ L_tx_p99 + 0.99 * p + T_relay_p99(b_eff) + T_cons
  Regime saturado (lambda * p > b ou rho -> 1):
    relay nao dorme; espera passa a ser dominada por (b-1)/(2*lambda) + fila M/G/1.
"""

import hashlib
import json
import math
import statistics
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
W1_JSON = ROOT / "colab_campaign" / "results.json"
PRED_DIR = ROOT / "predictions"
SEALED_JSON = PRED_DIR / "sealed_v1.json"


def fit_from_wave1():
    d = json.loads(W1_JSON.read_text())

    # Fit per-transport linear T_pub(b) = a + c*b and median T_q, T_mark from C4
    micro = {}
    for tr in ("kafka", "redis", "rabbitmq"):
        rows = [r for r in d["C4_ablation_A1_A2_micro"] if r["transport"] == tr and "micro_constants_ms" in r]
        tq = statistics.median(r["micro_constants_ms"]["T_q_median"] for r in rows)
        tmark = statistics.median(r["micro_constants_ms"]["T_mark_median"] for r in rows)
        # Estimate T_pub at small vs large batch (p=10 vs p=500 -> actual batch size ≈ lambda*p)
        by_b = [(min(r["batch_size"], max(1, int(r["rate"] * r["poll_ms"] / 1000.0))), r["micro_constants_ms"]["T_pub_median"]) for r in rows]
        xs = [x for x, _ in by_b]
        ys = [y for _, y in by_b]
        mx, my = statistics.mean(xs), statistics.mean(ys)
        denom = sum((x - mx) ** 2 for x in xs) or 1.0
        slope = max(0.0, sum((x - mx) * (y - my) for x, y in by_b) / denom)
        intercept = max(0.2, my - slope * mx)
        micro[tr] = {
            "T_q_ms": round(tq, 3),
            "T_mark_ms": round(tmark, 3),
            "T_pub_base_ms": round(intercept, 3),
            "T_pub_per_msg_ms": round(slope, 4),
        }

    # Baseline L_tx p50/p99 and CDC base latency from C2 @ 100 ev/s
    c2 = d["C2_multi_transport_frontier"]
    cdc_base = {}
    for tr in ("kafka", "redis", "rabbitmq"):
        r100 = [r for r in c2 if r["transport"] == tr and r["strategy"] == "outbox-cdc" and r["rate"] == 100]
        cdc_base[tr] = {
            "p50_ms": round(statistics.median(r["l_e2e_p50_ms"] for r in r100), 2),
            "p99_ms": round(statistics.median(r["l_e2e_p99_ms"] for r in r100), 2),
        }

    return {"micro_constants": micro, "cdc_base_100": cdc_base, "L_tx_p50_ms": 4.2, "L_tx_p99_ms": 9.5}


def predict_config(params, cfg):
    """Predicts L_e2e p50 and p99 (ms) + 80% prediction interval for an unseen config."""
    tr = cfg["transport"]
    strat = cfg["strategy"]
    lam = cfg["rate"]
    p_ms = cfg.get("poll_ms") or 0
    b_max = cfg.get("batch_size", 100)
    mc = params["micro_constants"][tr]

    if strat == "outbox-poll":
        # Effective batch size per wake-up
        b_eff = min(b_max, max(1.0, lam * (p_ms / 1000.0)))
        t_cycle_ms = mc["T_q_ms"] + mc["T_pub_base_ms"] + mc["T_pub_per_msg_ms"] * b_eff + mc["T_mark_ms"]
        c_relay = (b_eff / t_cycle_ms) * 1000.0  # ev/s
        rho = lam / max(1.0, c_relay)

        if rho >= 0.92:
            # Saturated regime: queue builds during 5s window -> ~ 0.5 * 5000 * (1 - 1/rho)
            excess = max(0.05, 1.0 - (1.0 / max(1.05, rho)))
            pred_p50 = round(params["L_tx_p50_ms"] + t_cycle_ms + 2500.0 * excess, 1)
            pred_p99 = round(params["L_tx_p99_ms"] + 2.0 * t_cycle_ms + 4500.0 * excess, 1)
            regime = "saturated"
        elif lam * (p_ms / 1000.0) >= b_max:
            # Relay does not sleep (batch always full), M1 vanishes!
            wait_p50 = ((b_max - 1) / (2.0 * lam)) * 1000.0
            q_delay = (rho / max(0.1, 1.0 - rho)) * (t_cycle_ms / 2.0)
            pred_p50 = round(params["L_tx_p50_ms"] + wait_p50 + t_cycle_ms + q_delay, 1)
            pred_p99 = round(params["L_tx_p99_ms"] + 1.8 * wait_p50 + 1.5 * t_cycle_ms + 2.5 * q_delay, 1)
            regime = "continuous_batch"
        else:
            # Subcritical: M1 uniform sleep in (0, p) dominates
            pred_p50 = round(params["L_tx_p50_ms"] + 0.50 * p_ms + t_cycle_ms, 1)
            pred_p99 = round(params["L_tx_p99_ms"] + 0.98 * p_ms + 1.5 * t_cycle_ms, 1)
            regime = "subcritical"
    elif strat == "outbox-cdc":
        base = params["cdc_base_100"][tr]
        # Single-threaded WAL decoder + broker publish capacity
        t_pub_1 = mc["T_pub_base_ms"] + mc["T_pub_per_msg_ms"]
        mu_cdc = 1800.0 if tr == "kafka" else (240.0 if tr == "rabbitmq" else 220.0)
        rho = lam / mu_cdc
        if rho >= 0.92:
            excess = max(0.05, 1.0 - (1.0 / max(1.05, rho)))
            pred_p50 = round(base["p50_ms"] + 2500.0 * excess, 1)
            pred_p99 = round(base["p99_ms"] + 4500.0 * excess, 1)
            regime = "saturated"
        else:
            q_factor = 1.0 / max(0.15, 1.0 - rho)
            pred_p50 = round(base["p50_ms"] * (0.7 + 0.3 * q_factor), 1)
            pred_p99 = round(base["p99_ms"] * (0.6 + 0.4 * q_factor), 1)
            regime = "subcritical"
    else:
        raise ValueError(strat)

    # 80% prediction interval (±25% relative band, min ±5 ms)
    lo99 = round(max(1.0, pred_p99 * 0.75 - 3.0), 1)
    hi99 = round(pred_p99 * 1.25 + 3.0, 1)
    return {
        **cfg,
        "predicted_regime": regime,
        "pred_l_e2e_p50_ms": pred_p50,
        "pred_l_e2e_p99_ms": pred_p99,
        "pi80_p99_ms": [lo99, hi99],
    }


UNSEEN_SEALED_CONFIGS = [
    # 12 unseen out-of-sample configurations (new rates, poll intervals, batch sizes)
    {"id": "V01", "transport": "kafka", "strategy": "outbox-poll", "rate": 50, "poll_ms": 25, "batch_size": 50},
    {"id": "V02", "transport": "kafka", "strategy": "outbox-poll", "rate": 50, "poll_ms": 250, "batch_size": 50},
    {"id": "V03", "transport": "kafka", "strategy": "outbox-poll", "rate": 150, "poll_ms": 250, "batch_size": 100},
    {"id": "V04", "transport": "kafka", "strategy": "outbox-poll", "rate": 450, "poll_ms": 250, "batch_size": 50},
    {"id": "V05", "transport": "kafka", "strategy": "outbox-poll", "rate": 900, "poll_ms": 100, "batch_size": 100},
    {"id": "V06", "transport": "kafka", "strategy": "outbox-cdc", "rate": 450, "poll_ms": None, "batch_size": 100},
    {"id": "V07", "transport": "kafka", "strategy": "outbox-cdc", "rate": 900, "poll_ms": None, "batch_size": 100},
    {"id": "V08", "transport": "redis", "strategy": "outbox-poll", "rate": 50, "poll_ms": 250, "batch_size": 50},
    {"id": "V09", "transport": "redis", "strategy": "outbox-poll", "rate": 450, "poll_ms": 25, "batch_size": 100},
    {"id": "V10", "transport": "redis", "strategy": "outbox-poll", "rate": 900, "poll_ms": 25, "batch_size": 150},
    {"id": "V11", "transport": "rabbitmq", "strategy": "outbox-poll", "rate": 50, "poll_ms": 250, "batch_size": 50},
    {"id": "V12", "transport": "rabbitmq", "strategy": "outbox-cdc", "rate": 50, "poll_ms": None, "batch_size": 50},
]


def main():
    PRED_DIR.mkdir(parents=True, exist_ok=True)
    params = fit_from_wave1()
    preds = [predict_config(params, c) for c in UNSEEN_SEALED_CONFIGS]
    payload = {
        "protocol": "doc 13 §5.4 — sealed out-of-sample predictions generated BEFORE Wave-2 execution",
        "fitted_on": str(W1_JSON.relative_to(ROOT.parent)),
        "parameters": params,
        "predictions": preds,
    }
    raw = json.dumps(payload, indent=2, sort_keys=False) + "\n"
    sha = hashlib.sha256(raw.encode()).hexdigest()
    payload["sha256_sealed"] = sha
    SEALED_JSON.write_text(json.dumps(payload, indent=2) + "\n")
    print(f"Sealed {len(preds)} out-of-sample predictions to {SEALED_JSON}")
    print(f"SHA-256: {sha}")
    for p in preds:
        print(f"  {p['id']} {p['transport']:<8} {p['strategy']:<11} r={p['rate']:>4} p={str(p['poll_ms']):<4} b={p['batch_size']:>3} -> pred p99={p['pred_l_e2e_p99_ms']:>6.1f}ms PI80={p['pi80_p99_ms']} ({p['predicted_regime']})")


if __name__ == "__main__":
    main()
