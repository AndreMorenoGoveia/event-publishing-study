# 16 — Seletor de estratégia e transporte por métricas (inclusive aplicações híbridas)

Este documento traduz os contratos (doc 12), os mecanismos (doc 13) e os dados coletados na fatia
vertical ([RESULTADOS.md](../analysis/colab_slice/RESULTADOS.md)) em uma **regra prática guiada por
7 métricas**. A mesma regra pode ser aplicada **por fluxo de evento**, permitindo que uma única
aplicação combine métodos diferentes conforme a criticidade e o perfil de cada fluxo.

## 1. As 7 métricas mínimas de entrada (por fluxo)

| # | Métrica | Valores / faixa | O que decide |
|---|---|---|---|
| 1 | **Taxa (`λ`, ev/s)** | `< 50` · `50–200` · `> 200` | Regime subcrítico × saturado (M1/P2), viabilidade de `pg-queue` e custo ocioso |
| 2 | **SLO de `L_e2e` (p99)** | `< 50 ms` · `50–500 ms` · `> 500 ms` | Elimina `outbox-poll` com `p` grande quando o SLO é apertado |
| 3 | **Concorrência no banco (`c`)** | `< 8` · `8–16` · `> 16` | Bloqueia `outbox-notify` sob alta concorrência (M5/P3: queda de 40–49% em A-E2) |
| 4 | **Durabilidade (`K`)** | `K2` (banco único) · `K3` (réplica) · `K4` (quórum + `fsync`) | Exige standby síncrono no PG para K3/K4 e elimina brokers que não atingem o contrato |
| 5 | **Ordem (`O`)** | `O0` (sem ordem) · `O1` (por agregado) | Elimina `pg-queue` (6 violações O1 em A-E1); sob F7 exige fencing no broker (Kafka) ou guarda de versão no consumidor |
| 6 | **Serviço (`S`)** | `S-fila` (1 consumidor, remove) · `S-log` (fan-out + replay) | Separa filas (RabbitMQ/SQS/`pg-queue`) de logs (Kafka/Redpanda/Pulsar) |
| 7 | **Runtime da aplicação** | `lambda` · `ecs-single` · `distributed` | Em `lambda`, bloqueia `LISTEN/NOTIFY` persistente e penaliza infraestrutura dedicada de alto custo fixo |

## 2. Regra rápida de decisão (filtros eliminatórios → escolha)

1. **Filtro de contrato (obrigatório — doc 12):**
   - Precisa de **E1 (não perder evento)** → elimina `dw-after`, `dw-before` e `cleanup=early` (H-C, H-D, H-J).
   - Precisa de **S-log (replay / múltiplos consumer groups)** → elimina `pg-queue` e RabbitMQ classic/quorum; escolhe broker S-log (**Kafka** K3, **Redpanda/Pulsar** K4, **NATS JetStream** K3/K4).
   - Precisa de **O1 estrito sob troca de lease (F7)** → elimina `pg-queue` (O0) e exige **fencing no broker** (Kafka `transactional.id`) **ou** CDC ordenado pelo WAL (**`outbox-cdc`**) **ou** verificação de `aggregate_version` no consumidor (H-A/P6).
   - **Runtime = `lambda`** → elimina `outbox-notify` (sem conexão persistente); prefere `outbox-poll` agendado ou fila gerenciada (SQS/EventBridge).
   - **Concorrência `c ≥ 16`** → elimina `outbox-notify` (A-E2 mostrou queda de -40,5% a -48,9% no throughput do PG).

2. **Escolha da estratégia de publicação (entre as elegíveis):**
   - **`S-fila` + `O0` + `λ ≤ 100 ev/s` + time pequeno (`ecs-single`/`lambda`)** → **`pg-queue`** (zero broker extra, ~10–18 ms `L_e2e` em A-S1).
   - **SLO `L_e2e` p99 ≥ 150 ms** (ou `λ` alto onde o relay não dorme) → **`outbox-poll`** (`p` ajustado ao SLO; custo operacional mínimo, sem slot WAL nem lock de `NOTIFY`).
   - **SLO `L_e2e` p99 < 50 ms + `c < 8` + `runtime ≠ lambda`** → **`outbox-notify`** (se quiser evitar CDC) ou **`outbox-cdc`** (mais estável no p99 sob skew, 13–21 ms em A-E1).
   - **SLO `L_e2e` p99 < 50 ms + (`c ≥ 8` ou múltiplos serviços/alta taxa)** → **`outbox-cdc`** (sem contenção de `NOTIFY`, preserva ordem do WAL).

## 3. Exemplo de aplicação híbrida (vários métodos na mesma app)

Uma mesma aplicação de e-commerce pode declarar cada fluxo com suas métricas e usar o seletor
[recommend.py](../analysis/selector/recommend.py):

| Fluxo na mesma aplicação | Métricas (`λ`, SLO, `c`, `K`, `O`, `S`, runtime) | Estratégia + Transporte recomendados |
|---|---|---|
| `faturamento-contabil` | 150 ev/s, SLO 50 ms, `c=16`, K3, **O1**, **S-log**, `distributed` | **`outbox-cdc` → Kafka (K3)** (ordem pelo WAL, replay, sem contenção de `NOTIFY`) |
| `notificacao-email` | 20 ev/s, SLO 2000 ms, `c=4`, K2, O0, **S-fila**, `ecs-single` | **`pg-queue`** (ou **`outbox-poll p=1000ms` → SQS/RabbitMQ** se já houver broker) |
| `webhook-parceiros` | 40 ev/s, SLO 500 ms, `c=4`, K2, O0, **S-fila**, `lambda` | **`outbox-poll (agendado)` → SQS / RabbitMQ** (compatível com Lambda efêmera) |
| `auditoria-replay` | 500 ev/s, SLO 1000 ms, `c=32`, K3, O0, **S-log**, `distributed` | **`outbox-poll (p=100ms, batch)` → Kafka (K3)** (no saturado, `p` não importa e evita slot CDC extra) |

## 4. Uso via CLI

```bash
# Exemplo pronto com aplicação híbrida de 4 fluxos
python3 analysis/selector/recommend.py --demo-hybrid

# Avaliar um fluxo específico
python3 analysis/selector/recommend.py \
  --rate 150 --slo-ms 50 --concurrency 16 \
  --durability K3 --order O1 --service S-log --runtime distributed
```
