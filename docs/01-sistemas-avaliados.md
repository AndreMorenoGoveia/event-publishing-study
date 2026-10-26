# 01 — Sistemas avaliados

## 1. Trilhas

| Trilha | Sistemas | Papel |
|---|---|---|
| **Núcleo (P0)** | PostgreSQL + **Kafka** + **RabbitMQ quorum queues** | Responder à pergunta central (doc 00 §1) |
| **Guia (P1)** | Redis Streams, NATS JetStream, Redpanda, RabbitMQ classic e Streams, `pg-queue` | Caracterização ampla, sem alegação de novidade |
| **Guia (P2)** | Pulsar, Valkey, ActiveMQ Artemis, Kafka share groups, NATS core | Extensões, só depois do núcleo e do guia P1 |

### 1.1 Por que estes dois brokers no núcleo

A escolha não é por popularidade, e sim pelos **eixos que a pergunta central exige contrastar**:

| Eixo | Kafka | RabbitMQ quorum |
|---|---|---|
| Semântica do ack (doc 12 §3) | K3: réplicas no ISR, **sem `fsync`** | K4: maioria Raft **com `fsync`** |
| Classe de serviço (doc 12 §6) | S-log (retenção e replay) | S-fila (consumo remove) |
| Custo dominante | Page cache e replicação | WAL e `fsync` |
| Cercamento de produtor obsoleto (doc 12 §12) | Sim (identidade transacional) | Não |

Essa combinação permite testar as previsões P4, P6 e P9 sem trocar de linguagem, de cliente ou de
padrão de acesso ao mesmo tempo. Redpanda entra no guia como **mesma API, outra implementação** —
com a ressalva do doc 13 §1: isso não isola "efeito de implementação".

## 2. Catálogo

> **Versões:** fixadas por digest em `infra/versions.env` na F0. As linhas abaixo são indicativas.

| ID | Sistema | Trilha | Contrato máximo (doc 12 §9) | Classe de serviço | Papel no estudo |
|---|---|---|---|---|---|
| `postgres` | PostgreSQL 17/18 | Núcleo | K2 (nó único) · K3/K4 com standby síncrono | — | Origem durável dos eventos |
| `kafka` | Apache Kafka 4.x (KRaft) | Núcleo | K3 (K4 com flush por mensagem) | S-log | Broker de destino A |
| `rabbitmq-quorum` | RabbitMQ 4.x quorum queues | Núcleo | K4 | S-fila | Broker de destino B |
| `noop`, `inmemory`, `chaos` | Adapters de calibração e teste | Núcleo | — | — | Piso do harness, teto do gerador, teste do verificador |
| `redis-streams` | Redis 8.x | Guia P1 | **K2** (K3/K4 não atingíveis) | S-log com retenção limitada por memória | Caracterização; **fora** dos grupos K3/K4 |
| `nats-jetstream` | NATS 2.x JetStream | Guia P1 | K3 (K4 com sync sempre) | S-log/S-fila conforme política | Caracterização |
| `redpanda` | Redpanda 25.x | Guia P1 | K4 | S-log | Mesma API do Kafka, outra implementação |
| `rabbitmq-classic` | RabbitMQ classic queues | Guia P1 | K2 | S-fila | Sem replicação na 4.x |
| `rabbitmq-stream` | RabbitMQ Streams | Guia P1 | K3 | S-log | Log dentro do RabbitMQ |
| `pg-queue` | PostgreSQL como fila | Guia P1 | = contrato do banco | S-fila | Baseline sem broker |
| `pulsar` | Apache Pulsar 4.x | Guia P2 | K4 | S-log | Armazenamento desacoplado |
| `valkey-streams`, `artemis`, `kafka-share`, `nats-core` | — | Guia P2 | — | — | Extensões |

## 3. Resumo de arquitetura

| Sistema | Armazenamento | Paralelismo | Consumo | Particularidade que afeta a medição |
|---|---|---|---|---|
| Kafka | Log segmentado, page cache, replicação por ISR | Partição | Pull, consumer group, offset | Sem `fsync` antes do ack; `linger.ms`/`batch.size` dominam o trade-off; forte dependência de page cache (M10) |
| RabbitMQ quorum | Log Raft (WAL + segmentos) | Fila | Push com prefetch | Confirm após maioria com `fsync`; memória cresce com não confirmados; flow control por crédito |
| PostgreSQL | Heap + WAL | Linha/lote | `SKIP LOCKED`, `DELETE ... RETURNING` | Bloat, autovacuum, checkpoint (M6); `NOTIFY` disputa lock no commit (M5); decodificação lógica é tarefa única (M7) |
| Redis Streams | Memória + AOF | Chave de stream | `XREADGROUP` + PEL | Replicação assíncrona sem consenso; limitado por RAM |
| NATS JetStream | Arquivos por stream, Raft | Stream/subject | Pull consumer | `fsync` periódico por padrão |
| Redpanda | Log por partição, Raft, thread-per-core | Partição | Igual Kafka | `fsync` antes do ack; reserva agressiva de CPU/memória |
| RabbitMQ Streams | Log replicado | Stream/super stream | Pull com crédito | Protocolo binário próprio |
| Pulsar | Broker + BookKeeper + metadata store | Partição | Subscriptions | Mais componentes no custo total |

## 4. Contratos

O mapeamento configuração → contrato, a semântica de cada ack e as obrigações de verificação estão
em [12-contratos-e-garantias.md](12-contratos-e-garantias.md) §9–§10. Regras que valem aqui:

- Um sistema **não aparece** em um grupo de comparação cujo contrato ele não alcança.
- O contrato efetivo é **asserido lendo o sistema em execução** (doc 08, F5); divergência reprova a execução.
- Redis Streams: máximo **K2**. Não participa de nenhuma comparação K3/K4.
- RabbitMQ classic: máximo K2 (sem replicação na 4.x).

## 5. Topologias

| ID | Descrição | Contratos alcançáveis |
|---|---|---|
| **T1** | Broker em nó único | K1, K2 |
| **T3** | Broker em 3 nós, mesmos limites por nó | K3, K4 |
| **PG1** | PostgreSQL em nó único | K2 |
| **PG2** | PostgreSQL + standby síncrono | K3 (K4 conforme `synchronous_commit`) |

### 5.1 Cadeias do núcleo

O contrato da cadeia é o mínimo dos trechos (doc 12 §8), então a cadeia é declarada como par:

| Cadeia | Banco | Broker | Uso |
|---|---|---|---|
| **CH-A** | PG1 (K2) | T3 (K3) | Configuração mais comum na prática; mostra o elo fraco no banco |
| **CH-B** | PG2 (K3) | T3 (K3) | Cadeia K3 coerente — base das comparações principais |
| **CH-C** | PG2 (K3) | T3 (K4) | Custo de subir o contrato do broker (previsão P4) |

Notas de topologia: Kafka em T3 usa modo combinado (broker+controller) para manter três nós com os
mesmos recursos de todos os sistemas — registrado como ameaça à validade (doc 05 §8). Redis em T3
(primário + réplicas + Sentinels) existe apenas para o cenário de failover do guia, nunca para
comparação K3.

## 6. Mapeamento idiomático

| Conceito | Kafka / Redpanda | RabbitMQ (filas) | RabbitMQ Streams | Redis Streams | NATS JetStream | Pulsar | `pg-queue` |
|---|---|---|---|---|---|---|---|
| Destino | tópico | exchange → fila | stream / super stream | chave de stream | stream (subjects) | tópico | tabela |
| Partição por agregado | partições + hash | consistent-hash exchange → N filas | super stream | N chaves (hash no cliente) | subject `events.<n>` | tópico particionado | `partition_key` |
| Grupo de consumo | consumer group | consumidores na fila | consumidores com offset | `XREADGROUP` | durable pull consumer | subscription `Shared`/`Key_Shared` | N workers com `SKIP LOCKED` |
| Ack de consumo | commit de offset | `basic.ack` | store offset | `XACK` | `ack` | `acknowledge` | `DELETE` na tx |
| Fan-out | N consumer groups | fanout → N filas | N consumidores | N consumer groups | N consumers | N subscriptions | **não nativo** |
| Replay (S-log) | por offset/tempo | **não** | por offset/tempo | por ID, limitado por memória | conforme política | por posição | **não** |

## 7. Clientes Java

| Sistema | Biblioteca | Observação |
|---|---|---|
| Kafka, Redpanda | `org.apache.kafka:kafka-clients` | Mesmo cliente e versão; o **relay** usa identidade transacional por partição para cercamento (doc 12 §12) |
| RabbitMQ (filas) | `com.rabbitmq:amqp-client` 5.x | Confirms assíncronos; um canal por thread; **sem** cercamento disponível |
| RabbitMQ Streams | `com.rabbitmq:stream-client` | |
| Redis | `io.lettuce:lettuce-core` | Pipelining controlado pelo adapter |
| NATS | `io.nats:jnats` | `publishAsync` |
| Pulsar | `org.apache.pulsar:pulsar-client` | `sendAsync` |
| PostgreSQL | `org.postgresql:postgresql` + HikariCP | `reWriteBatchedInserts=true` nos lotes |

Variações de cliente (AMQP 1.0, Jedis, Spring) são P2 e medidas como **overhead de camada**.

## 8. Considerados e não priorizados

| Sistema | Motivo |
|---|---|
| Serviços gerenciados (SQS/SNS, Pub/Sub, Service Bus, Event Hubs) | Não reprodutíveis; emuladores não representam desempenho. Só no modelo de custo |
| RocketMQ | Menor adoção no ecossistema alvo; candidato a P2 |
| ZeroMQ, Aeron, Chronicle | Categoria diferente (sem broker / ultra-baixa latência) |
| `LISTEN/NOTIFY` como pub/sub | Não durável; usado apenas como gatilho do relay |
| Transações Kafka como padrão de publicação | Fora do escopo; o mecanismo é usado só para **cercamento** do relay |
