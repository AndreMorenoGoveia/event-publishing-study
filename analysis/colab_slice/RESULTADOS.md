# Fatia Vertical Enxuta no Colab — Resultados Empíricos

- **Execução:** VM somente CPU do Google Colab (2 vCPUs, Ubuntu, PostgreSQL 16 `wal_level=logical` + RabbitMQ com fila durável e *publisher confirms*), via `colab run` com *teardown* automático.
- **Duração total:** 438,2 s (~7,3 min) · **Custo:** < 0,01 compute units (saldo mantido em 200,96 CU; `Active assignments: 0` verificado antes e depois).
- **Dados brutos:** [results.json](results.json) · **Script:** [run_study.py](../../experiments/colab_slice/run_study.py).

> **Escopo e honestidade metodológica (doc 07 §1 e doc 12 §10):** ambiente **single-host (T1/PG1)**.
> **Verificado aqui:** histórias determinísticas F6/F7 (H-A, H-B, H-C, H-D, H-J), regime do polling (M1/P2), contenção do `NOTIFY` (M5/P3), overhead de `L_req` (P7) e ordem O1 sob carga representativa com skew.
> **Não verificado aqui:** F2 real com perda de page cache, replicação multi-host T3/PG2 (K3/K4) e rede cross-AZ.

---

## 1. A-F1 — Histórias Mínimas Determinísticas (Correção antes de performance)

| História | Cenário injetado | Resultado observado | Previsão |
|---|---|---|---|
| **H-A (sem fencing)** | Relay R1 zumbi publica v1 após R2 publicar v1, v2 | Sequência `[1, 2, 1]` · 1 inversão · marcação no banco **não** bloqueada | Confirma **P6** |
| **H-A (fencing só no banco)** | Mesmo cenário no RabbitMQ (sem cercamento no broker) | `db_mark_blocked = true`, mas sequência ainda `[1, 2, 1]` (1 inversão) | Confirma **P6** (fencing no banco sozinho não basta) |
| **H-B (crash pós-ack)** | Falha entre ack do broker e `UPDATE published_at` | `lost = 0`, `duplicates = 1` | Conforme esperado (E1) |
| **H-C (`dw-after`)** | Crash entre `COMMIT` e `publish` | `lost_confirmed = 1` | Perda confirmada |
| **H-D (`dw-before`)** | `publish` ok + `ROLLBACK` no banco | `ghost_events = 1` | Fantasma confirmado |
| **H-J (`cleanup=early` × `after_ack`)** | Limpeza antes vs. depois do ack com falha do broker | `early`: `lost_confirmed = 1` · `after_ack`: `lost_confirmed = 0` | Confirma **P5** |

---

## 2. A-S1 — Perfil Empresa Pequena (`W0`: 10 e 50 ev/s, `tx-realistic`)

| Estratégia | λ (ev/s) | `L_req` p50 (ms) | `L_req` p99 (ms) | `L_e2e` p50 (ms) | `L_e2e` p99 (ms) | Entregues |
|---|---:|---:|---:|---:|---:|---:|
| `tx-only` (baseline) | 10 | 4,18 | 11,52 | — | — | 80/80 |
| `dw-after` | 10 | **8,55** | **18,94** | 8,69 | 19,31 | 80/80 |
| `pg-queue` (sem broker) | 10 | 4,15 | 27,56 | **10,10** | 34,59 | 80/80 |
| `outbox-poll (p=100ms)` | 10 | 4,52 | 11,32 | 61,56 | 113,65 | 80/80 |
| `outbox-poll (p=1000ms)` | 10 | 4,41 | 10,96 | 508,90 | 985,92 | 80/80 |
| `outbox-notify` | 10 | 4,54 | 11,54 | 9,81 | 26,84 | 80/80 |
| `outbox-cdc` (WAL lógico) | 10 | 4,63 | 14,21 | 9,47 | 21,16 | 80/80 |
| `tx-only` (baseline) | 50 | 3,75 | 6,52 | — | — | 400/400 |
| `dw-after` | 50 | **8,26** | **12,99** | 8,40 | 13,12 | 400/400 |
| `pg-queue` (sem broker) | 50 | 3,79 | 8,05 | **9,62** | **17,24** | 400/400 |
| `outbox-poll (p=100ms)` | 50 | 4,19 | 12,57 | 58,84 | 110,92 | 400/400 |
| `outbox-poll (p=1000ms)` | 50 | 4,12 | 7,53 | 510,03 | 1001,42 | 400/400 |
| `outbox-notify` | 50 | 4,12 | 11,12 | 9,38 | 25,46 | 400/400 |
| `outbox-cdc` (WAL lógico) | 50 | 4,21 | 17,24 | 8,96 | 49,54 | 400/400 |

**Leitura prática para Empresa Pequena (Lambda / ECS):**
- **P7 confirmado na mediana:** `outbox-*` acrescenta < 0,5 ms na request contra `tx-only`, enquanto `dw-after` **dobra** `L_req` (e ainda perde eventos em H-C).
- **`pg-queue`** entrega ~10 ms de `L_e2e` p50 sem broker externo — opção mais econômica para 1 ECS/Lambda em baixa taxa quando fan-out/replay não são obrigatórios.
- **`outbox-poll`** exibe exatamente o piso `p/2` na mediana (~59 ms para `p=100ms`, ~510 ms para `p=1000ms`) e é a opção mais simples quando há broker externo (ex.: SQS/RabbitMQ) sem manter conexão persistente de `LISTEN` ou slot de CDC.

---

## 3. A-E1 — Perfil Grande Empresa (Carga Representativa Zipf + payload variável, `c=4`)

| Estratégia | λ = 25 (`L_e2e` p99) | λ = 100 (`L_e2e` p99) | λ = 300 (`L_e2e` p99 / cens.) | λ = 600 (`L_e2e` p99 / cens.) | Violações O1 |
|---|---:|---:|---:|---:|---:|
| `outbox-poll (p=10ms)` | 24,38 ms | 38,10 ms | 6608,8 ms (163 cens.) | 11707,8 ms (2356 cens.) | 0 |
| `outbox-poll (p=100ms)` | 112,64 ms | 111,83 ms | 6733,8 ms (164 cens.) | 11778,7 ms (2437 cens.) | 0 |
| `outbox-notify` | 22,10 ms | **114,56 ms** | 6732,3 ms (161 cens.) | 13597,5 ms (1877 cens.) | 0 |
| `outbox-cdc` | **13,48 ms** | **21,03 ms** | 6877,7 ms (209 cens.) | 12046,2 ms (2490 cens.) | 0 |
| `pg-queue` | 16,24 ms | 17,94 ms | **34,57 ms** (0 cens.) | 1065,9 ms (0 cens.) | **6** (em 300 e 600) |

**Leitura prática para Grande Empresa:**
- **Regime subcrítico (25–100 ev/s):** `outbox-cdc` domina `L_e2e` p99 (13–21 ms) com zero violações O1. Já `outbox-notify` começa a degradar no p99 logo em 100 ev/s (114,6 ms).
- **Previsão P2 (M1) confirmada:** no regime subcrítico, mudar `p` de 10 ms para 100 ms adiciona ~80 ms ao p99; no regime saturado (300 e 600 ev/s, acima do teto do relay single-threaded com *publisher confirms* síncronos em 2 vCPUs), a diferença entre `p=10ms` e `p=100ms` desaparece (< 2%).
- **Ordem no `pg-queue`:** embora sustente mais throughput sem broker, apresentou **6 violações de O1** em 300 e 600 ev/s sob escritores concorrentes no mesmo agregado — confirmando que `pg-queue` entrega apenas **O0** (doc 02 §9).

---

## 4. A-E2 — Contenção do `LISTEN/NOTIFY` sob Concorrência (Ablação A7 / Previsão P3)

Throughput transacional máximo (`tx-realistic`, malha fechada de 5 s):

| Concorrência (`c`) | `outbox-poll` (tx/s) | `outbox-notify` (tx/s) | Queda com `NOTIFY` |
|---:|---:|---:|---:|
| 4 | 988,0 | 594,5 | **-39,8%** |
| 16 | 1024,1 | 609,5 | **-40,5%** |
| 32 | 992,3 | 507,3 | **-48,9%** |

**Conclusão:** confirma diretamente o mecanismo **M5** e a previsão **P3** — o lock global da fila de notificações no commit do PostgreSQL derruba o throughput transacional em **40% a 49%** conforme a concorrência cresce. Para grandes empresas com alta concorrência de escritores no banco, `LISTEN/NOTIFY` é um gargalo estrutural severo; `outbox-poll` com lote ou `outbox-cdc` evitam esse custo.
