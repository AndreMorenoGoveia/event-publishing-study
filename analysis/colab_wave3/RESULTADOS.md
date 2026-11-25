# Campanha Onda 3 (Paralela em 4 VMs) — Lacunas Enterprise, Manutenção PG, Resiliência e Fatorial N12

Consolidado das 4 execuções paralelas isoladas (`3A`, `3B`, `3C`, `3D`) no Google Colab com *teardown* automático.

---

## 1. Onda 3A — Lacunas Enterprise ([w3a.json](w3a.json))

### N20 — Overhead de TLS e Serialização (NATS JetStream, 1.000 eventos)
| TLS | Formato | Bytes/evento | `pub` p50 (ms) | `pub` p99 (ms) | Throughput (ev/s) |
|---|---|---:|---:|---:|---:|
| Não | `binary` | 918 | 0,407 | 0,894 | 2252,3 |
| Não | `json` | 918 | 0,332 | 0,759 | 2686,8 |
| Não | `json_zlib` | **342 (-63%)** | 0,403 | 0,812 | 2267,6 |
| **Sim** | `binary` | 918 | 0,379 | 0,801 | 2350,9 |
| **Sim** | `json` | 918 | 0,406 | 0,837 | 2208,3 |
| **Sim** | `json_zlib` | **342 (-63%)** | 0,427 | 0,912 | 2142,9 |

### N21 — *Poison Pill* / DLQ e *Head-of-Line Blocking* sob O1 (2% *poison pills*, 3×40 ms retry)
| Padrão | Eventos saudáveis p50 (ms) | Eventos saudáveis p99 (ms) | DLQ |
|---|---:|---:|---:|
| **Kafka O1 estrito (bloqueia partição)** | **3911,2** | **4364,2** | 6 |
| **RabbitMQ DLQ paralelo (O0)** | **92,7** | **147,9** | 6 |

> **Impacto prático:** exigir O1 sem necessidade penaliza em **~30×** os eventos saudáveis quando há *poison pills*.

### N19 — Multi-Tenant / *Noisy Neighbor* (1, 4, 8 serviços no mesmo PostgreSQL)
| Serviços concorrentes | Serviço alvo `tx` p50 (ms) | Serviço alvo `tx` p99 (ms) | Cluster total (tx/s) |
|---:|---:|---:|---:|
| 1 | 3,67 | 4,69 | 98,5 |
| 4 | 4,09 | 6,79 | 799,2 |
| 8 | 4,06 | 6,36 | 1761,2 |

---

## 2. Onda 3B — Manutenção PG (N09 / M6) e Empresa Pequena (A-S2) ([w3b.json](w3b.json))

### N09 — Estratégias de limpeza da outbox (30.000 linhas em 6 lotes de 5.000)
| Modo de limpeza | `autovacuum` | Tamanho final (MB) | *Dead tuples* | Limpeza 5k p99 (ms) |
|---|---|---:|---:|---:|
| `mark_only` | ON | **51,89** | 5.000 | **152,88** |
| `delete_after_ack` | OFF | 34,84 | **25.000** | 11,94 |
| `delete_after_ack` | ON | 23,68 | 15.000 | 12,35 |
| **`partition_drop`** | ON | **0,00** | **0** | **7,73** |

### A-S2 — Empresa Pequena: Lambda sem pool × ECS pool persistente (`W0`)
| Modo | 5 ev/s (p50 / p99 ms) | 25 ev/s (p50 / p99 ms) | 50 ev/s (p50 / p99 ms) |
|---|---|---|---|
| **Lambda conexão efêmera** | 13,05 / 17,39 | 13,27 / 19,14 | **19,16 / 29,15** |
| **ECS / RDS Proxy (pool)** | **4,63** / 37,49 | **4,04 / 6,34** | **4,09 / 7,59** |

---

## 3. Onda 3C — Rajada (G11), Backlog (G12), Rede (G15) e Deduplicação (G19) ([w3c.json](w3c.json))

### G11 — Rajada 5× (`100 → 500 → 100 ev/s`, `L_e2e` p99 em ms)
| Transporte | Pré-rajada | Durante rajada (500 ev/s) | Pós-rajada |
|---|---:|---:|---:|
| **NATS JetStream** | 3,59 | **2,53** | **3,44** |
| **Kafka 4.4** | 1179,33 *(warmup)* | **10,11** | **9,13** |
| **Redis Streams** | 206,67 | 62,86 | 208,19 |
| **RabbitMQ** | 13,01 | **7874,09** | **7943,67** *(não recuperou)* |

### G12 — Drenagem de Backlog (5.000 eventos acumulados)
| Transporte | Tempo (s) | Taxa de drenagem (ev/s) |
|---|---:|---:|
| **Kafka 4.4** | **0,085** | **59.129,6** |
| **NATS JetStream** | 0,178 | 28.136,6 |
| **Redis Streams** | 0,218 | 22.953,2 |
| **RabbitMQ** | 2,426 | 2.060,8 |

### G15 — Amplificação de Latência de Rede (`+0 / +10 / +30 ms` RTT, p50 em ms)
| RTT injetado | Redis 1× | Redis batch-20 | RabbitMQ confirm |
|---:|---:|---:|---:|
| +0 ms | 6,56 | 48,07 | 7,22 |
| +10 ms | 16,93 (+1,0×) | 58,02 (+0,05×/ev) | 17,50 (+1,0×) |
| +30 ms | 37,44 (+1,0×) | 78,93 (+0,05×/ev) | 37,86 (+1,0×) |

### G19 — Custo de Deduplicação no Consumidor (2.000 eventos, 10% duplicatas)
| Estratégia | Throughput (ev/s) | p99 (ms) | Durável? |
|---|---:|---:|---|
| `dedup_memory` | 11.918.951 | < 0,01 | Não |
| **`dedup_postgres` (`ON CONFLICT`)** | **356,9** | **4,57** | **Sim** |
| `dedup_redis` (`SET NX`, AOF `always`) | 193,8 | 9,60 | Sim |

---

## 4. Onda 3D — Fatorial Fracionado N12 `2^(4-1)` ([w3d.json](w3d.json))
- **Achado de correção confirmado:** com `k=2` relays usando `SKIP LOCKED` (`600 ev/s, b=200`), ocorreu **1 violação de O1** (`O1=1`), confirmando doc 02 §6.1.
- **Anomalia registrada (doc 08 F9):** no Kafka 4.4.0 recém-lançado, tópicos recém-criados exibiram piso de ~925 ms no p99, mascarando os efeitos principais de `poll`/`batch` no fatorial (razões ~0,97–1,00).
