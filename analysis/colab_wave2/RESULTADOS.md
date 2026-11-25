# Campanha Onda 2 — Modelo Preditivo Selado, Kafka 3000 ev/s, NATS JetStream, `fsync` (P4) e `R=10` Blocos

- **Ambiente:** VM Colab `--high-mem` (8 vCPUs, 51.0 GiB RAM, PG 16, Kafka 3.9 KRaft, NATS JetStream fast+fsync, RabbitMQ, Redis AOF `always`).
- **Duração total:** 2621.2 s (~43.7 min).
- **Previsões seladas (SHA-256):** `ee427a429d6db02851ef4511979fbfe74e3bbada265c0b572f61ee85f214f566` ([sealed_v1.json](../predictions/sealed_v1.json)).

## 1. W2-A — Validação Fora da Amostra do Modelo Mecanístico (Pilar N3 / N17)

- **APE mediano:** **34.2%** · **MAPE médio:** **40.5%** (meta doc 13 §5.3: ≤ 25%) · **Cobertura PI80:** **50.0%** (meta: ≥ 70%).

| ID | Configuração inédita | Regime previsto | Previsto p99 (ms) | Observado p99 (ms) | Erro (%) | Dentro PI80? |
|---|---|---|---:|---:|---:|---:|
| `V01` | `kafka/outbox-poll r=50 p=25 b=50` | subcritical | 39.7 | 296.19 | 86.6% | Não |
| `V02` | `kafka/outbox-poll r=50 p=250 b=50` | subcritical | 260.7 | 262.96 | 0.9% | Sim |
| `V03` | `kafka/outbox-poll r=150 p=250 b=100` | subcritical | 261.9 | 294.54 | 11.1% | Sim |
| `V04` | `kafka/outbox-poll r=450 p=250 b=50` | continuous_batch | 115.9 | 85.74 | 35.2% | Sim |
| `V05` | `kafka/outbox-poll r=900 p=100 b=100` | subcritical | 117.5 | 336.22 | 65.1% | Não |
| `V06` | `kafka/outbox-cdc r=450 p=None b=100` | subcritical | 14.9 | 30.01 | 50.3% | Não |
| `V07` | `kafka/outbox-cdc r=900 p=None b=100` | subcritical | 18.4 | 533.16 | 96.5% | Não |
| `V08` | `redis/outbox-poll r=50 p=250 b=50` | subcritical | 270.1 | 267.19 | 1.1% | Sim |
| `V09` | `redis/outbox-poll r=450 p=25 b=100` | subcritical | 49.6 | 74.17 | 33.1% | Não |
| `V10` | `redis/outbox-poll r=900 p=25 b=150` | subcritical | 49.6 | 1891.91 | 97.4% | Não |
| `V11` | `rabbitmq/outbox-poll r=50 p=250 b=50` | subcritical | 270.9 | 259.88 | 4.2% | Sim |
| `V12` | `rabbitmq/outbox-cdc r=50 p=None b=50` | subcritical | 14.3 | 15.07 | 5.1% | Sim |

## 2. W2-B — Fronteira do Kafka em Alta Taxa (`500 → 3000 ev/s`, Cruzamento `λ*` / P1)

| λ (ev/s) | Estratégia | `L_e2e` p50 (ms) | `L_e2e` p99 (ms) | `L_req` p99 (ms) | Censurados |
|---:|---|---:|---:|---:|---:|
| 500 | `outbox-poll (p=10ms)` | 12.11 | 21.82 | None | 0 |
| 500 | `outbox-poll (p=100ms)` | 12.44 | 23.66 | None | 0 |
| 500 | `outbox-cdc` | 8.93 | 25.27 | None | 0 |
| 1000 | `outbox-poll (p=10ms)` | 15.32 | 107.48 | None | 0 |
| 1000 | `outbox-poll (p=100ms)` | 16.67 | 53.46 | None | 0 |
| 1000 | `outbox-cdc` | 171.09 | 280.93 | None | 0 |
| 1500 | `outbox-poll (p=10ms)` | 1298.6 | 2344.39 | None | 0 |
| 1500 | `outbox-poll (p=100ms)` | 1291.77 | 2337.7 | None | 0 |
| 1500 | `outbox-cdc` | 1351.1 | 2433.37 | None | 0 |
| 2200 | `outbox-poll (p=10ms)` | 2663.71 | 4883.18 | None | 0 |
| 2200 | `outbox-poll (p=100ms)` | 2713.12 | 5385.75 | None | 0 |
| 2200 | `outbox-cdc` | 3008.07 | 5631.01 | None | 0 |
| 3000 | `outbox-poll (p=10ms)` | 4594.19 | 8697.26 | None | 0 |
| 3000 | `outbox-poll (p=100ms)` | 4678.01 | 8541.93 | None | 0 |
| 3000 | `outbox-cdc` | 4880.71 | 8955.51 | None | 0 |

## 3. W2-C — Ablação A3: Page Cache × `fsync` no Mesmo Sistema (Kafka & NATS JetStream — M8/P4)

| λ (ev/s) | Transporte | `L_e2e` p50 (ms) | `L_e2e` p99 (ms) | Censurados |
|---:|---|---:|---:|---:|
| 100 | `kafka` | 14.24 | 21.47 | 0 |
| 100 | `kafka-fsync` | 27.95 | 67.37 | 0 |
| 100 | `nats` | 12.53 | 19.71 | 0 |
| 100 | `nats-fsync` | 22.07 | 36.56 | 0 |
| 500 | `kafka` | 12.51 | 23.89 | 0 |
| 500 | `kafka-fsync` | 157.25 | 299.79 | 0 |
| 500 | `nats` | 11.32 | 21.51 | 0 |
| 500 | `nats-fsync` | 3559.5 | 5525.16 | 553 |
| 1000 | `kafka` | 221.6 | 502.94 | 0 |
| 1000 | `kafka-fsync` | 1305.03 | 2099.25 | 0 |
| 1000 | `nats` | 594.92 | 1182.37 | 0 |
| 1000 | `nats-fsync` | 4925.24 | 8227.79 | 2334 |

## 4. W2-D — Alta Precisão Estatística (`R = 10` blocos pareados, `t = 2,262`, δ = 10%)

| Comparação (`L_e2e` p99) | Taxa | Mediana A (ms) | Mediana B (ms) | Razão geom. | IC 95% (`R=10`) | Classe |
|---|---:|---:|---:|---:|---|---|
| Kafka CDC vs Kafka Poll(100ms) | 100 ev/s | 1984.3 | 1985.5 | 1.016× | [0.684, 1.509] | `inconclusivo` |
| Kafka CDC vs Kafka Poll(10ms) | 100 ev/s | 1984.3 | 1984.9 | 1.015× | [0.739, 1.395] | `inconclusivo` |
| Kafka Poll(10ms) vs Redis Poll(10ms) | 100 ev/s | 1984.9 | 63.7 | 25.374× | [19.428, 33.138] | `diferente` |
| Kafka CDC vs Kafka Poll(100ms) | 300 ev/s | 1985.1 | 1986.1 | 0.929× | [0.701, 1.232] | `inconclusivo` |
| Kafka CDC vs Kafka Poll(10ms) | 300 ev/s | 1985.1 | 1988.9 | 0.931× | [0.794, 1.092] | `inconclusivo` |
| Kafka Poll(10ms) vs Redis Poll(10ms) | 300 ev/s | 1988.9 | 83.7 | 18.16× | [13.481, 24.461] | `diferente` |
| pg-queue vs Kafka CDC | 300 ev/s | 25.6 | 1985.1 | 0.018× | [0.016, 0.02] | `diferente` |

## 5. W2-E — Cenários de Produção (G04 Payload, G08 Fan-out, G18 Replay)

### G04 — Tamanho de Payload (`1 → 64 KiB` @ 100 ev/s, `L_e2e` p99 em ms)
| Payload | Kafka | NATS JetStream | Redis Streams | RabbitMQ |
|---:|---:|---:|---:|---:|
| 1 KiB | 21.22 | 18.97 | 43.33 | 28.04 |
| 8 KiB | 22.1 | 19.59 | 49.09 | 28.52 |
| 32 KiB | 22.24 | 20.8 | 62.94 | 44.0 |
| 64 KiB | 24.83 | 23.75 | 129.36 | 84.78 |

### G08 — Fan-out (`1, 2, 4` consumer groups independentes @ 300 ev/s, `L_e2e` p99 em ms)
| Grupos | Kafka | NATS JetStream |
|---:|---:|---:|
| 1 | 17.99 | 18.86 |
| 2 | 19.73 | 20.56 |
| 4 | 99.2 | 36.29 |

### G18 — Replay de 5.000 eventos retidos (requisito funcional S-log)
| Transporte | Suporta Replay? | Eventos | Tempo (s) | Taxa de Replay (ev/s) |
|---|---|---:|---:|---:|
| **Kafka** | Sim (S-log) | 5000 | 0.076 | **65964.3** |
| **NATS JetStream** | Sim (S-log) | 5000 | 0.178 | **28031.8** |
| **Redis Streams** | Sim (limitado por RAM) | 5000 | 0.179 | **27966.3** |
| **RabbitMQ / `pg-queue`** | **Não** (S-fila) | — | — | — |

## 6. Figuras geradas

- `F18_kafka_frontier_lambda_star.png`
- `F05_ablation_fsync_same_system.png`
- `F06_payload_size.png`
- `FN3_sealed_model_validation.png`
