"""Monte Carlo da base estatística do estudo (doc 05 §7, doc 03 §2).

Independe de broker, estratégia ou pergunta central: toda versão do estudo mede quantis de latência
por execução e compara execuções repetidas e pareadas por bloco.

  S1  precisão do quantil empírico dentro de uma execução (eventos por janela × cauda × autocorrelação)
  S2  cobertura e largura do IC entre repetições (t × bootstrap percentil, IC95 × IC90) e
      probabilidades da regra equivalente/diferente/inconclusivo
  S3  a regra adaptativa "3 iniciais → escalonar até R_max" infla o erro? (parada opcional)

Uso: python mc_stats.py [s1|s2|s3|all] [--quick] [--out DIR]
"""
import argparse
import itertools
import math
import os
import time
import zlib

import numpy as np
import torch
from scipy import stats

DEV = "cuda" if torch.cuda.is_available() else "cpu"
QS = (0.5, 0.99, 0.999, 0.9999)
CAP = 2**26  # elementos por lote na GPU


def seed(*key):
    return zlib.crc32(repr(key).encode())


def ar1(rows, n, rho, gen):
    """Processo AR(1) estacionário de variância 1, via scan de Hillis-Steele (log2 n passos)."""
    e = torch.randn(rows, n, device=DEV, generator=gen)
    if rho == 0:
        return e
    e[:, 1:] *= math.sqrt(1 - rho * rho)
    s, c = 1, rho
    while s < n and c > 1e-30:
        e[:, s:] = e[:, s:] + c * e[:, :-s].clone()
        s, c = 2 * s, c * c
    return e


def latencies(family, rows, n, sigma, rho, gen):
    x = ar1(rows, n, rho, gen)
    if family == "lognormal":
        return torch.exp(sigma * x)
    # "polling": espera uniforme em (0, p) + serviço lognormal; p = 1 e serviço com mediana 0,1·p
    u = torch.rand(rows, n, device=DEV, generator=gen)
    return u + 0.1 * torch.exp(sigma * x)


def true_quantiles(family, sigma):
    if family == "lognormal":
        return {q: math.exp(sigma * stats.norm.ppf(q)) for q in QS}
    gen = torch.Generator(device=DEV).manual_seed(12345)
    v = torch.cat([latencies(family, 1, 2**24, sigma, 0.0, gen).flatten() for _ in range(8)])
    v = v.sort().values
    return {q: v[math.ceil(q * v.numel()) - 1].item() for q in QS}


def s1(quick):
    ns = [10**3, 10**4, 10**5] if quick else [10**3, 10**4, 10**5, 10**6, 10**7]
    reps = 200 if quick else 2000
    rhos = [0.0, 0.99] if quick else [0.0, 0.9, 0.99, 0.999, 0.9999]
    grid = [("lognormal", s) for s in (0.5, 1.0, 1.5)] + [("polling", 0.5)]
    out = []
    for (family, sigma), rho, n in itertools.product(grid, rhos, ns):
        if family == "polling" and rho not in (0.0, 0.99):
            continue
        gen = torch.Generator(device=DEV).manual_seed(seed(family, sigma, rho, n))
        tq = true_quantiles(family, sigma)
        rows = max(1, min(reps, CAP // n))
        est = {q: [] for q in QS}
        done = 0
        while done < reps:
            r = min(rows, reps - done)
            v = latencies(family, r, n, sigma, rho, gen).sort(dim=1).values
            for q in QS:
                est[q].append(v[:, math.ceil(q * n) - 1].double().cpu())
            done += r
        for q in QS:
            rel = torch.cat(est[q]).numpy() / tq[q] - 1
            out.append(dict(family=family, sigma=sigma, rho=rho, n=n, q=q, reps=reps,
                            bias=rel.mean(), rrmse=math.sqrt((rel**2).mean()),
                            abs_p95=np.quantile(np.abs(rel), 0.95)))
        print(f"s1 {family} σ={sigma} ρ={rho} n={n:.0e} p99 rrmse={out[-3]['rrmse']:.4f}", flush=True)
    return out


def diffs(dist, m, n, cv, gen):
    """Diferença pareada de log(métrica) por bloco sob efeito nulo; cada braço tem desvio `cv`."""
    def eps():
        if dist == "normal":
            return torch.randn(m, n, device=DEV, generator=gen)
        if dist == "t3":
            z = torch.randn(m, n, device=DEV, generator=gen)
            chi = torch.randn(m, n, 3, device=DEV, generator=gen).pow(2).sum(-1)
            return z / torch.sqrt(chi / 3) / math.sqrt(3)
        # contaminada: 5% das execuções com desvio 5×, variância total normalizada
        z = torch.randn(m, n, device=DEV, generator=gen)
        out = torch.rand(m, n, device=DEV, generator=gen) < 0.05
        return torch.where(out, 5 * z, z) / math.sqrt(0.95 + 0.05 * 25)
    return cv * (eps() - eps())


def t_ci(d, level):
    n = d.shape[1]
    hw = stats.t.ppf(0.5 + level / 2, n - 1) * d.std(dim=1) / math.sqrt(n)
    mu = d.mean(dim=1)
    return mu - hw, mu + hw


def boot_ci(d, level, B, gen):
    m, n = d.shape
    lo, hi = [], []
    step = max(1, CAP // (B * n))
    for i in range(0, m, step):
        x = d[i:i + step]
        idx = torch.randint(0, n, (x.shape[0], B, n), device=DEV, generator=gen)
        bm = torch.gather(x.unsqueeze(1).expand(-1, B, -1), 2, idx).mean(-1).sort(dim=1).values
        a = (1 - level) / 2
        lo.append(bm[:, int(a * B)])
        hi.append(bm[:, min(B - 1, int((1 - a) * B))])
    return torch.cat(lo), torch.cat(hi)


def classify(lo, hi, delta):
    """Regra única do doc 05 §7.2, em escala de razão: margem [1−δ, 1+δ]."""
    a, b = math.log(1 - delta), math.log(1 + delta)
    eq = (lo >= a) & (hi <= b)
    diff = (hi < a) | (lo > b)
    return eq.float().mean().item(), diff.float().mean().item()


DISTS = ("normal", "t3", "contaminated")
CVS = (0.01, 0.02, 0.05, 0.10, 0.20)


def s2(quick):
    ns = [3, 5, 10] if quick else [3, 4, 5, 6, 8, 10, 12, 15, 20, 30]
    m, B = (2000, 500) if quick else (20000, 4000)
    out = []
    for dist, cv, n in itertools.product(DISTS, CVS, ns):
        gen = torch.Generator(device=DEV).manual_seed(seed(dist, cv, n))
        d = diffs(dist, m, n, cv, gen)
        for method, level in itertools.product(("t", "boot"), (0.95, 0.90)):
            lo, hi = t_ci(d, level) if method == "t" else boot_ci(d, level, B, gen)
            row = dict(dist=dist, cv=cv, n=n, method=method, level=level, m=m,
                       coverage=((lo <= 0) & (hi >= 0)).float().mean().item(),
                       rel_halfwidth_median=math.expm1(((hi - lo) / 2).median().item()))
            # CI é equivariante em locação: deslocar pelo efeito verdadeiro dá as probabilidades
            for delta in (0.05, 0.10):
                for k, eff in (("0", 1.0), ("half", 1 + delta / 2), ("at", 1 + delta), ("2x", 1 + 2 * delta)):
                    s = math.log(eff)
                    row[f"p_eq_d{delta}_{k}"], row[f"p_diff_d{delta}_{k}"] = classify(lo + s, hi + s, delta)
            out.append(row)
        print(f"s2 {dist} cv={cv} n={n} t95 cov={out[-4]['coverage']:.3f} boot95 cov={out[-2]['coverage']:.3f}",
              flush=True)
    return out


def s3(quick):
    """Procedimentos de repetição: fixo, dois estágios (doc 05 §7.4) e sequencial um a um."""
    m = 20000 if quick else 200000
    n0, rmax = 3, 15
    out = []
    for dist, cv, target in itertools.product(DISTS, CVS, (0.05, 0.10)):
        gen = torch.Generator(device=DEV).manual_seed(seed(dist, cv, target))
        full = diffs(dist, m, rmax, cv, gen)
        h = math.log1p(target)
        tcrit = torch.tensor([0] + [stats.t.ppf(0.975, k - 1) if k > 1 else 0 for k in range(1, rmax + 1)],
                             device=DEV, dtype=full.dtype)
        k = torch.arange(1, rmax + 1, device=DEV)
        cs, cs2 = full.cumsum(1), (full**2).cumsum(1)
        mu_k = cs / k
        sd_k = torch.sqrt(((cs2 - cs**2 / k) / (k - 1).clamp(min=1)).clamp(min=0))
        hw_k = tcrit[k] * sd_k / torch.sqrt(k.to(full.dtype))
        # dois estágios: n = ceil((t·s3/h)²) a partir das 3 iniciais, limitado a [3, R_max]
        need = torch.ceil((tcrit[n0] * sd_k[:, n0 - 1] / h) ** 2).clamp(n0, rmax).long()
        # sequencial: para na primeira k ≥ 3 com meia-largura ≤ alvo (ou em R_max)
        ok = (hw_k <= h) & (k >= n0)
        ok[:, -1] = True
        stop = ok.float().argmax(1) + 1
        for proc, nf in (("fixed15", torch.full((m,), rmax, device=DEV)), ("two_stage", need), ("sequential", stop)):
            i = (nf - 1).unsqueeze(1)
            mu, hw = mu_k.gather(1, i).squeeze(1), hw_k.gather(1, i).squeeze(1)
            row = dict(dist=dist, cv=cv, target=target, proc=proc, m=m,
                       coverage=(hw >= mu.abs()).float().mean().item(),
                       mean_n=nf.float().mean().item(),
                       p_target_met=(hw <= h).float().mean().item())
            for delta in (0.05, 0.10):
                for kname, eff in (("0", 1.0), ("at", 1 + delta)):
                    s = math.log(eff)
                    row[f"p_eq_d{delta}_{kname}"], row[f"p_diff_d{delta}_{kname}"] = classify(mu - hw + s, mu + hw + s,
                                                                                                delta)
            out.append(row)
        print(f"s3 {dist} cv={cv} target={target} cov: " +
              " ".join(f"{r['proc']}={r['coverage']:.3f}/n̄={r['mean_n']:.1f}" for r in out[-3:]), flush=True)
    return out


def stalled(rows, cycles, per_cycle, frac, gen):
    """Malha aberta com pausa periódica (checkpoint/vacuum/GC) de fração `frac` do ciclo (período 1).

    Chegadas em grade com fase aleatória; quem chega durante a pausa espera o fim dela, sem
    coordinated omission. Serviço base lognormal com mediana 0,001 ciclo.
    """
    n = int(cycles * per_cycle)
    t = torch.arange(n, device=DEV, dtype=torch.float64) / per_cycle
    t = t + torch.rand(rows, 1, device=DEV, generator=gen, dtype=torch.float64)
    wait = (frac - torch.frac(t)).clamp(min=0)
    base = 0.001 * torch.exp(0.5 * torch.randn(rows, n, device=DEV, generator=gen, dtype=torch.float64))
    return (wait + base).float()


def s4(quick):
    """Quantos ciclos de manutenção a janela de medição precisa cobrir (doc 03 §4.3)."""
    per_cycle = 10**3 if quick else 10**4
    reps = 200 if quick else 2000
    cycles = [0.5, 1, 3, 10] if quick else [0.5, 1, 1.5, 2, 2.5, 3, 3.5, 5, 5.5, 10, 10.5, 20, 50]
    out = []
    for frac in (0.001, 0.005, 0.01, 0.02, 0.05):
        gen = torch.Generator(device=DEV).manual_seed(seed("s4", frac))
        ref = stalled(4, 500 if not quick else 50, per_cycle, frac, gen).flatten().sort().values
        tq = {q: ref[math.ceil(q * ref.numel()) - 1].item() for q in QS}
        for k in cycles:
            n = int(k * per_cycle)
            rows = max(1, min(reps, CAP // (2 * n)))
            est = {q: [] for q in QS}
            done = 0
            while done < reps:
                r = min(rows, reps - done)
                v = stalled(r, k, per_cycle, frac, gen).sort(dim=1).values
                for q in QS:
                    est[q].append(v[:, math.ceil(q * n) - 1].double().cpu())
                done += r
            for q in QS:
                e = torch.cat(est[q]).numpy()
                rel = e / tq[q] - 1
                out.append(dict(stall_frac=frac, cycles=k, per_cycle=per_cycle, q=q, reps=reps,
                                true_q=tq[q], cv=e.std() / e.mean(), bias=rel.mean(),
                                rrmse=math.sqrt((rel**2).mean()), abs_p95=np.quantile(np.abs(rel), 0.95)))
            print(f"s4 frac={frac} cycles={k} p99 cv={out[-3]['cv']:.3f} p99.9 cv={out[-2]['cv']:.3f}", flush=True)
    return out


def demo():
    """Autoverificação: AR(1) estacionário e t-IC com cobertura nominal sob normalidade."""
    gen = torch.Generator(device=DEV).manual_seed(1)
    x = ar1(4, 2**16, 0.9, gen)
    assert abs(x.var().item() - 1) < 0.1, x.var()
    lag1 = (x[:, 1:] * x[:, :-1]).mean().item()
    assert abs(lag1 - 0.9) < 0.05, lag1
    d = diffs("normal", 40000, 5, 0.05, gen)
    lo, hi = t_ci(d, 0.95)
    cov = ((lo <= 0) & (hi >= 0)).float().mean().item()
    assert abs(cov - 0.95) < 0.01, cov
    print("demo ok", DEV)


if __name__ == "__main__":
    import pandas as pd
    ap = argparse.ArgumentParser()
    ap.add_argument("study", choices=["s1", "s2", "s3", "s4", "all", "demo"])
    ap.add_argument("--quick", action="store_true")
    ap.add_argument("--out", default="out")
    a = ap.parse_args()
    demo()
    if a.study != "demo":
        os.makedirs(a.out, exist_ok=True)
        for name in (["s1", "s2", "s3", "s4"] if a.study == "all" else [a.study]):
            t0 = time.time()
            pd.DataFrame(globals()[name](a.quick)).to_csv(f"{a.out}/{name}.csv", index=False)
            print(f"{name} done in {time.time() - t0:.0f}s", flush=True)
