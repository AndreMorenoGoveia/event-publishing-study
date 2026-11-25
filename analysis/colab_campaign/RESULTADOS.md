# Campanha Multi-Transporte no Colab — Resultados Empíricos

- **Ambiente:** VM Colab `--high-mem` (8 vCPUs, 51.0 GiB RAM, PostgreSQL 16 `wal_level=logical`, Apache Kafka 3.9.0 KRaft, RabbitMQ durável + confirms, Redis Streams AOF `always`).
- **Duração total:** 1109.5 s (~18.5 min) · **Gasto real:** `0,08` compute units (`200,96` → `200,88` CU; `Active assignments: 0`).
- **Dados brutos:** [results.json](results.json) · **Script:** [run_campaign.py](../../experiments/colab_campaign/run_campaign.py).

> **Escopo e honestidade metodológica (doc 07 §1 e doc 12 §10):** execução **single-host (T1/PG1)** com `R=3` blocos pareados e IC `t` pareado sobre `log(razão)` (conforme S2 do Monte Carlo). **Não verificado aqui:** F2 real com perda de page cache, replicação multi-host T3/PG2 (K3/K4) e tráfego cross-AZ.

## 1. C1 — Fencing em Duas Camadas: Kafka × RabbitMQ (Previsão P6)

| Broker | Mecanismo testado | Sequência entregue | Zumbi cercado pelo broker? | Inversões O1 |
|---|---|---|---:|---:|
| **Kafka 3.9** | Epoch no banco + `transactional.id` (`read_committed`) | `[1, 2]` | **True** | **0** |
| **RabbitMQ** | Epoch só no banco (`db_mark_blocked=True`) | `[1, 2, 1]` | **False** | **1** |

**Resultado (P6 confirmada):** no Kafka, o produtor zumbi recebeu erro de fencing no broker (`PRODUCER_FENCED` / `INVALID_PRODUCER_EPOCH`) e **zero** inversões chegaram ao consumidor `read_committed`. No RabbitMQ, mesmo com fencing de `epoch` no banco bloqueando o `UPDATE`, a mensagem do zumbi já enviada entrou na fila fora de ordem (`[1, 2, 1]`).

## 2. C2 — Fronteira Multi-Transporte (`R=3` blocos, carga representativa Zipf)

| λ (ev/s) | Transporte | Estratégia | `L_req` p99 (ms) | `L_e2e` p50 (ms) | `L_e2e` p99 (ms) | Censurados (3 blocos) | Violações O1 |
|---:|---|---|---:|---:|---:|---:|---:|
| 25 | `kafka` | `outbox-cdc` | 10.5 | 9.7 | 18.7 | 0 | 0 |
| 25 | `kafka` | `outbox-notify` | 8.8 | 10.3 | 14.8 | 0 | 0 |
| 25 | `kafka` | `outbox-poll (p=10ms)` | 9.7 | 14.9 | 51.2 | 0 | 0 |
| 25 | `kafka` | `outbox-poll (p=100ms)` | 10.3 | 56.7 | 108.2 | 0 | 0 |
| 25 | `pg-queue` | `pg-queue` | 5.9 | 9.2 | 13.4 | 0 | 0 |
| 25 | `rabbitmq` | `outbox-cdc` | 6.7 | 10.0 | 13.0 | 0 | 0 |
| 25 | `rabbitmq` | `outbox-poll (p=10ms)` | 5.8 | 13.5 | 21.2 | 0 | 0 |
| 25 | `rabbitmq` | `outbox-poll (p=100ms)` | 6.3 | 55.6 | 94.6 | 0 | 0 |
| 25 | `redis` | `outbox-cdc` | 6.5 | 9.3 | 11.9 | 0 | 0 |
| 25 | `redis` | `outbox-poll (p=10ms)` | 6.4 | 15.1 | 20.9 | 0 | 0 |
| 25 | `redis` | `outbox-poll (p=100ms)` | 7.4 | 58.4 | 112.6 | 0 | 0 |
| 100 | `kafka` | `outbox-cdc` | 7.4 | 8.7 | 13.2 | 0 | 0 |
| 100 | `kafka` | `outbox-notify` | 6.9 | 9.1 | 13.1 | 0 | 0 |
| 100 | `kafka` | `outbox-poll (p=10ms)` | 7.3 | 14.0 | 21.7 | 0 | 0 |
| 100 | `kafka` | `outbox-poll (p=100ms)` | 10.8 | 57.9 | 111.6 | 0 | 0 |
| 100 | `pg-queue` | `pg-queue` | 5.8 | 9.1 | 12.9 | 0 | 0 |
| 100 | `rabbitmq` | `outbox-cdc` | 5.7 | 8.6 | 13.0 | 0 | 0 |
| 100 | `rabbitmq` | `outbox-poll (p=10ms)` | 7.0 | 14.1 | 21.3 | 0 | 0 |
| 100 | `rabbitmq` | `outbox-poll (p=100ms)` | 6.7 | 54.1 | 110.3 | 0 | 0 |
| 100 | `redis` | `outbox-cdc` | 7.0 | 8.7 | 29.1 | 0 | 0 |
| 100 | `redis` | `outbox-poll (p=10ms)` | 7.1 | 24.9 | 35.3 | 0 | 0 |
| 100 | `redis` | `outbox-poll (p=100ms)` | 7.0 | 25.6 | 77.7 | 0 | 0 |
| 300 | `kafka` | `outbox-cdc` | 7.7 | 8.5 | 12.9 | 0 | 0 |
| 300 | `kafka` | `outbox-notify` | 563.6 | 234.0 | 569.9 | 0 | 0 |
| 300 | `kafka` | `outbox-poll (p=10ms)` | 8.6 | 11.5 | 18.6 | 0 | 0 |
| 300 | `kafka` | `outbox-poll (p=100ms)` | 8.9 | 11.8 | 66.3 | 0 | 0 |
| 300 | `pg-queue` | `pg-queue` | 6.7 | 8.7 | 13.2 | 0 | 6 |
| 300 | `rabbitmq` | `outbox-cdc` | 7.9 | 1718.0 | 2924.7 | 0 | 0 |
| 300 | `rabbitmq` | `outbox-poll (p=10ms)` | 7.5 | 1512.8 | 2683.7 | 0 | 0 |
| 300 | `rabbitmq` | `outbox-poll (p=100ms)` | 8.0 | 1592.4 | 2815.4 | 0 | 0 |
| 300 | `redis` | `outbox-cdc` | 6.6 | 3154.8 | 5470.8 | 370 | 0 |
| 300 | `redis` | `outbox-poll (p=10ms)` | 8.4 | 27.8 | 42.4 | 0 | 0 |
| 300 | `redis` | `outbox-poll (p=100ms)` | 8.2 | 27.9 | 84.8 | 0 | 0 |
| 600 | `kafka` | `outbox-cdc` | 13.0 | 9.3 | 17.7 | 0 | 0 |
| 600 | `kafka` | `outbox-notify` | 6183.4 | 2918.7 | 6189.6 | 0 | 0 |
| 600 | `kafka` | `outbox-poll (p=10ms)` | 39.1 | 12.4 | 47.3 | 0 | 0 |
| 600 | `kafka` | `outbox-poll (p=100ms)` | 13.3 | 12.5 | 27.6 | 0 | 0 |
| 600 | `pg-queue` | `pg-queue` | 8.4 | 8.5 | 13.9 | 0 | 24 |
| 600 | `rabbitmq` | `outbox-cdc` | 21.5 | 3836.5 | 6719.4 | 2949 | 0 |
| 600 | `rabbitmq` | `outbox-poll (p=10ms)` | 15.8 | 3821.9 | 6876.3 | 2972 | 0 |
| 600 | `redis` | `outbox-cdc` | 16.2 | 3457.6 | 4717.2 | 0 | 0 |
| 600 | `redis` | `outbox-poll (p=10ms)` | 14.3 | 31.5 | 46.1 | 0 | 0 |
| 600 | `redis` | `outbox-poll (p=100ms)` | 17.2 | 33.3 | 91.5 | 0 | 0 |

### Comparações pareadas por bloco (IC `t` 95% sobre `log(razão)`, margem δ = 10%)

| Comparação (`L_e2e` p99) | Taxa | Razão geom. | IC 95% | Classe estatística |
|---|---:|---:|---|---|
| Kafka CDC vs Kafka Poll(100ms) | 25 ev/s | 0.27× | [0.02, 3.675] | `inconclusivo` |
| Kafka CDC vs Kafka Poll(100ms) | 100 ev/s | 0.098× | [0.049, 0.195] | `diferente` |
| Kafka CDC vs Kafka Poll(10ms) | 100 ev/s | 0.688× | [0.369, 1.284] | `inconclusivo` |
| Kafka CDC vs Kafka Poll(10ms) | 300 ev/s | 0.29× | [0.007, 12.821] | `inconclusivo` |
| Kafka CDC vs Kafka NOTIFY | 100 ev/s | 0.717× | [0.18, 2.861] | `inconclusivo` |
| Kafka CDC vs Redis CDC | 100 ev/s | 0.518× | [0.218, 1.23] | `inconclusivo` |
| Kafka CDC vs RabbitMQ CDC | 100 ev/s | 1.222× | [0.549, 2.719] | `inconclusivo` |
| Redis Poll(10ms) vs Kafka Poll(10ms) | 300 ev/s | 0.97× | [0.021, 44.407] | `inconclusivo` |
| pg-queue vs Kafka CDC | 100 ev/s | 0.863× | [0.436, 1.71] | `inconclusivo` |

## 3. C3 — Ablação A5: Contenção por Agregado (Previsão P8, Kafka, `c=8`, 200 ev/s)

| Agregados ativos | Estratégia | `L_req` p99 (ms) | `L_e2e` p99 (ms) | Censurados |
|---:|---|---:|---:|---:|
| 200 | `outbox-poll (p=10ms)` | 6.1 | 2806.9 | 0 |
| 200 | `outbox-poll (p=100ms)` | 5.3 | 2806.6 | 0 |
| 200 | `outbox-cdc` | 4.8 | 2801.9 | 0 |
| 1 | `outbox-poll (p=10ms)` | 6.5 | 2807.0 | 0 |
| 1 | `outbox-poll (p=100ms)` | 5.5 | 2806.7 | 0 |
| 1 | `outbox-cdc` | 5.9 | 2808.2 | 0 |

## 4. C4 — Ablação A1/A2 e Microconstantes do Modelo Mecanístico (`λ=100 ev/s`)

| Transporte | Lote (`b`) | Poll (`p`, ms) | `L_e2e` p99 (ms) | `T_q` (ms) | `T_pub` (ms) | `T_mark` (ms) |
|---|---:|---:|---:|---:|---:|---:|
| `kafka` | 1 | 10 | 2810.9 | 0.465 | 0.535 | 2.201 |
| `kafka` | 1 | 100 | 2812.1 | 0.342 | 0.442 | 2.037 |
| `kafka` | 50 | 10 | 2811.0 | 0.481 | 0.557 | 2.226 |
| `kafka` | 50 | 100 | 2821.3 | 0.708 | 1.491 | 2.433 |
| `kafka` | 50 | 500 | 2814.1 | 0.917 | 1.53 | 2.579 |
| `kafka` | 200 | 10 | 2812.4 | 0.532 | 0.608 | 2.383 |
| `kafka` | 200 | 100 | 2821.2 | 0.749 | 1.519 | 2.458 |
| `kafka` | 200 | 500 | 2810.2 | 1.176 | 2.933 | 2.838 |
| `redis` | 1 | 10 | 367.1 | 0.477 | 6.443 | 3.34 |
| `redis` | 1 | 100 | 475.4 | 0.473 | 6.481 | 3.331 |
| `redis` | 50 | 10 | 34.5 | 0.48 | 6.371 | 3.417 |
| `redis` | 50 | 100 | 107.3 | 0.479 | 6.842 | 3.32 |
| `redis` | 50 | 500 | 484.6 | 0.502 | 6.902 | 3.435 |
| `redis` | 200 | 10 | 33.3 | 0.496 | 6.572 | 3.298 |
| `redis` | 200 | 100 | 110.4 | 0.49 | 6.513 | 3.236 |
| `redis` | 200 | 500 | 510.5 | 0.478 | 6.504 | 3.335 |
| `rabbitmq` | 1 | 10 | 21.1 | 0.441 | 4.414 | 2.251 |
| `rabbitmq` | 1 | 100 | 108.4 | 0.399 | 4.294 | 2.177 |
| `rabbitmq` | 50 | 10 | 21.5 | 0.486 | 4.554 | 2.331 |
| `rabbitmq` | 50 | 100 | 109.2 | 0.481 | 8.06 | 2.31 |
| `rabbitmq` | 50 | 500 | 506.8 | 0.525 | 17.116 | 2.352 |
| `rabbitmq` | 200 | 10 | 20.6 | 0.489 | 4.716 | 2.424 |
| `rabbitmq` | 200 | 100 | 109.5 | 0.519 | 8.398 | 2.44 |
| `rabbitmq` | 200 | 500 | 504.9 | 0.623 | 20.234 | 2.549 |

## 5. C5 — Aplicação Híbrida Real (4 fluxos simultâneos no mesmo PostgreSQL)

| Fluxo simultâneo | Taxa alvo | Entregues | `L_req` p99 (ms) | `L_e2e` p50 (ms) | `L_e2e` p99 (ms) |
|---|---:|---:|---:|---:|---:|
| `billing_cdc_kafka` | 100 ev/s | 1000/1000 | 7.08 | 11.87 | 2662.3 |
| `audit_poll_kafka` | 250 ev/s | 2500/2500 | 13.93 | 60.18 | 2652.12 |
| `webhooks_poll_rmq` | 40 ev/s | 400/400 | 7.75 | 57.64 | 108.14 |
| `emails_pg_queue` | 30 ev/s | 300/300 | 8.06 | 13.01 | 21.2 |

