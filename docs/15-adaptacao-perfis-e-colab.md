# 15 — Adaptação por perfil de empresa e protocolo enxuto no Colab

Este documento registra a avaliação de validade do estudo para dois perfis extremos de adoção —
**grande empresa com múltiplos serviços distribuídos** e **empresa pequena com uma Lambda ou um
ECS isolado** — e define as adaptações de escopo, cenários e execução econômica via **Colab CLI**.

## 1. Avaliação de validade por perfil

### 1.1 Grande empresa (vários serviços distribuídos por eventos)
**Validade do desenho atual:** **alta** no núcleo técnico (contratos K/E/O/S, cadeia banco→broker,
fencing em duas camadas, carga representativa com skew e contenção por agregado, manutenção do
PostgreSQL).

**Lacunas cobertas por esta adaptação:**
1. **Multi-serviço / *noisy neighbor*:** contenção compartilhada de WAL/slots lógicos e de
   `LISTEN/NOTIFY` quando vários escritores/serviços concorrem no mesmo banco ou cluster.
2. **Segurança e serialização:** impacto de TLS/mTLS e validação de schema (Avro/Protobuf) em CPU,
   latência e bytes na rede (antes fora do núcleo).
3. **Consumidor com falhas parciais (DLQ / *poison pill*):** sob ordem estrita O1, uma mensagem
   com erro bloqueia a partição (*head-of-line blocking*), diferenciando S-log de S-fila na prática.
4. **Topologia gerenciada multi-AZ:** custo de tráfego cross-AZ e limites de IOPS de rede em
   RDS/Aurora + MSK/Confluent/Event Hubs.

### 1.2 Empresa pequena (1 Lambda ou 1 ECS publicando eventos sozinho)
**Validade do desenho atual:** **baixa** sem adaptação — o núcleo assume cluster T3 + standby
síncrono (7 hosts, ~337 h de máquina) e piso de custo em `W1 = 500 ev/s`, excluindo o que empresas
pequenas realmente usam.

**Lacunas cobertas por esta adaptação:**
1. **Faixa de carga realista (`W0`):** `1 a 50 ev/s` (com picos ocasionais), onde **custo fixo
   ocioso** e simplicidade operacional dominam sobre MST.
2. **Execução efêmera (Lambda / ECS único):**
   - **Lambda:** não mantém conexão persistente para `LISTEN/NOTIFY` nem relay contínuo em memória;
     o padrão prático é poller agendado (EventBridge Scheduler) ou publicação direta em fila
     gerenciada (SQS/SNS/EventBridge) com reconciliação.
   - **ECS (1–2 tasks):** relay *in-process* (`outbox-poll` ou `pg-queue`) evita o custo fixo de
     manter Kafka Connect/Debezium Server e clusters T3 dedicados.
3. **`pg-queue` promovido a baseline primário em `W0`:** para quem já tem PostgreSQL e baixa taxa,
   eliminar o broker reduz drasticamente custo e superfície de falha.

## 2. Cenários adaptados (extensão do doc 04)

| ID | Perfil | O que mede |
|---|---|---|
| **A-S1** | Empresa pequena (`W0`: 5–100 ev/s) | `tx-only`, `dw-after`, `pg-queue`, `outbox-poll` (`p=100ms, 1000ms`), `outbox-notify`, `outbox-cdc` — latência `L_req`, `L_e2e`, custo fixo mensal estimado |
| **A-E1** | Grande empresa (200–2000 ev/s + skew) | Cruzamento `λ*` entre polling, `NOTIFY` e CDC sob carga com Zipf + múltiplos escritores por agregado (`c ∈ {1, 4, 8, 16}`) |
| **A-E2** | Grande empresa (concorrência `NOTIFY`) | Ablação A7 / previsão P3: impacto do `NOTIFY` no throughput transacional conforme `c` cresce |
| **A-F1** | Ambos (correção determinística) | Histórias mínimas H-A (fencing de lease), H-B (duplicata pós-ack), H-C (`dw-after` perda), H-D (`dw-before` fantasma), H-J (`cleanup=early` perda) |
| **A-E3** *(futuro multi-host)* | Grande empresa | Multi-tenant (N serviços/slots WAL), TLS+Schema e *head-of-line blocking* com DLQ sob O1 |

## 3. Protocolo de execução econômica via Colab CLI

Para coletar resultados empíricos reais da **fatia vertical enxuta** sem desperdiçar unidades de
computação:

1. **Sempre usar VM somente CPU** (omitir `--gpu` e `--tpu` no `colab run`): custo mínimo
   (~0,07 CU/h vs. GPUs caras), suficiente para PostgreSQL + RabbitMQ + harness de carga.
2. **Usar `colab run` (sem `--keep`):** provisiona uma VM limpa, executa o experimento e **libera
   automaticamente a VM** ao término — inclusive em caso de erro.
3. **Auditoria obrigatória antes e depois:** executar `colab sessions` e `colab usage` antes e
   depois de cada rodada, garantindo `Active assignments: 0` e `Usage rate: 0.00/hr`.
4. **Limitações declaradas (honestidade metodológica — doc 07 §1 e doc 12 §10):**
   - A execução em VM única do Colab testa **ambiente single-host** (PostgreSQL + RabbitMQ +
     harness na mesma VM, T1/PG1).
   - **Verificado nesta fatia:** fronteiras relativas em single-host (`outbox-poll` × `outbox-notify`
     × `outbox-cdc` via replicação lógica nativa do PG × `pg-queue` × `dw-*` × `tx-only`),
     degradação de `NOTIFY` sob concorrência (P3), efeito de regime do polling (P2), overhead de
     `L_req` (P7) e histórias determinísticas de correção (H-A, H-B, H-C, H-D, H-J).
   - **Não verificado nesta fatia (permanece marcado como tal):** F2 com perda real de page cache,
     replicação multi-host T3/PG2 (K3/K4), rede cross-AZ e ciclos longos de 4 h (N16).
