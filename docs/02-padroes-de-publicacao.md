# 02 — Estratégias de publicação

## 1. O problema

Um serviço muda estado no banco e precisa publicar um evento correspondente. Banco e broker não
compartilham transação. Cada estratégia escolhe onde aceitar o risco — e a **fronteira de sucesso**
de cada uma está definida no doc 12 §1.

## 2. Catálogo

| ID | Estratégia | Trilha | Contratos alcançáveis (doc 12) |
|---|---|---|---|
| `outbox-poll` | Outbox + relay por polling | **Núcleo** | K = min(banco, broker) · E1 · O1 (com fencing) |
| `outbox-notify` | Outbox + relay acordado por `LISTEN/NOTIFY` | **Núcleo** | idem |
| `outbox-cdc` | Outbox + CDC (Debezium) | **Núcleo** | idem, O1 pela ordem do WAL |
| `tx-only` | Transação sem evento | **Núcleo** (controle) | — |
| `dw-after` | Commit e depois publica | **Núcleo** (controle negativo) | Perde eventos por construção |
| `dw-before` | Publica e depois commit | **Núcleo** (controle negativo) | Gera fantasmas por construção |
| `pg-queue` | Banco como fila | Guia P1 | K do banco · E1 · O0 com N workers |
| `direct` | Publicação sem banco | Guia P1 | K do broker · E1 |

### 2.1 Combinações do núcleo

| Estratégia | Kafka | RabbitMQ quorum |
|---|---|---|
| `outbox-poll` | ✔ | ✔ |
| `outbox-notify` | ✔ | ✔ |
| `outbox-cdc` (Kafka Connect) | ✔ | — |
| `outbox-cdc` (Debezium Server) | ✔ | ✔ |
| `dw-after`, `dw-before` | ✔ | ✔ |

O `2×2` de runtime × destino do CDC (`{Connect, Server}` × `{Kafka, RabbitMQ}`) é **P0**
(ablação A4, doc 13 §3): sem ele não se separa "o runtime é lento" de "o destino é lento".

## 3. Modelo de domínio

Duas variantes de transação: `tx-simple` (um `INSERT`) e `tx-realistic` (lock do agregado +
`UPDATE` + `INSERT` + outbox, com dois índices secundários). A `tx-realistic` é a padrão, porque é
ela que expõe contenção (M4) e serializa a versão do agregado.

```sql
CREATE TABLE customers (               -- agregado
  id            UUID PRIMARY KEY,
  balance_cents BIGINT NOT NULL,
  version       BIGINT NOT NULL DEFAULT 0,   -- ordem canônica (doc 12 §5.1)
  updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE orders (
  id                UUID PRIMARY KEY,
  customer_id       UUID NOT NULL REFERENCES customers(id),
  aggregate_version BIGINT NOT NULL,          -- = customers.version após o incremento
  status            TEXT NOT NULL,
  amount_cents      BIGINT NOT NULL,
  data              BYTEA,
  -- metadados do verificador (fonte da verdade do conjunto esperado)
  event_id          UUID   NOT NULL,
  producer_id       INT    NOT NULL,
  producer_seq      BIGINT NOT NULL,
  created_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX orders_customer_idx ON orders (customer_id);

CREATE TABLE outbox (
  id                BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  event_id          UUID        NOT NULL,
  aggregate_id      UUID        NOT NULL,
  aggregate_version BIGINT      NOT NULL,     -- ordem verificada é por (aggregate_id, version)
  event_type        TEXT        NOT NULL,
  partition_key     INT         NOT NULL,     -- hash(aggregate_id) % N
  payload           BYTEA       NOT NULL,
  created_at        TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  published_at      TIMESTAMPTZ
);
CREATE INDEX outbox_pending_idx ON outbox (id) WHERE published_at IS NULL;
CREATE INDEX outbox_part_pending_idx ON outbox (partition_key, id) WHERE published_at IS NULL;

CREATE TABLE outbox_relay_lease (
  partition_key INT PRIMARY KEY,
  owner         TEXT        NOT NULL,
  epoch         BIGINT      NOT NULL,         -- monotônico; base do fencing (doc 12 §12)
  expires_at    TIMESTAMPTZ NOT NULL
);
```

### 3.1 Transação de negócio (`tx-realistic`)

```sql
BEGIN;
  SELECT balance_cents, version FROM customers WHERE id = :agg FOR UPDATE;   -- serializa o agregado
  UPDATE customers SET balance_cents = :novo, version = version + 1 WHERE id = :agg;
  INSERT INTO orders (..., aggregate_version) VALUES (..., :version + 1);
  INSERT INTO outbox (..., aggregate_version) VALUES (..., :version + 1);
COMMIT;
```

Consequências: escritores concorrentes do mesmo agregado **são permitidos** (a concorrência `c` é
fator do estudo, ablação A5) e, sob o lock, ordem de versão = ordem de commit para aquele agregado.
O gerador **não** precisa mais restringir uma chave a um único produtor.

### 3.2 Estratégias de limpeza

| Valor | Mecânica | Contrato |
|---|---|---|
| `mark` | `UPDATE published_at` + `DELETE` em lote periódico | Válido: só marca após ack no contrato alvo |
| `delete` | `DELETE` após o ack | Válido |
| `partition-drop` | Tabela particionada por tempo + `DROP PARTITION` | Válido |
| `insert-delete` | (CDC) `INSERT` + `DELETE` na mesma transação | Válido: o CDC lê o `INSERT` do WAL |
| **`early`** | Marca/remove **antes** do ack | **Controle negativo** (doc 12 §8, história H-J): rebaixa a cadeia e produz perda sob F2 |

## 4. Publicação direta (`direct`, guia)

```
future = publisher.publish(evento)      # assíncrono; completa no ack do contrato
```
Parâmetros: batching do cliente, conexões, compressão (desligada; payload incompressível).

## 5. Dual-write (controles negativos)

| Estratégia | Sequência | Falha esperada |
|---|---|---|
| `dw-after` | `COMMIT` → `publish().await()` | Perda em F1/F6 entre commit e ack (história H-C) |
| `dw-before` | `publish().await()` → `COMMIT` | Fantasma quando o commit falha (história H-D) |

Retentativas limitadas e em memória, sem buffer em disco: representam a implementação ingênua.

## 6. Outbox com polling

### 6.1 Algoritmo A — lote com `SKIP LOCKED` (`relay.mode=skip-locked`)

```
loop:
  BEGIN
  rows := SELECT id, event_id, aggregate_id, aggregate_version, payload
          FROM outbox WHERE published_at IS NULL
          ORDER BY id LIMIT :b FOR UPDATE SKIP LOCKED
  if rows vazio: COMMIT; espera(p) ou NOTIFY; continue
  aguarda todas as publicações do lote (ack no contrato alvo)
  UPDATE outbox SET published_at = clock_timestamp() WHERE id = ANY(:ids)
  COMMIT
  se o lote veio cheio, repete sem dormir        # origem do regime "saturado" (M1)
```

Propriedades: com `k > 1` relays, **O1 não se sustenta** — e isso é medido, não assumido.
`ORDER BY id` é heurística de leitura, não define a ordem canônica (doc 12 §5.1).

### 6.2 Algoritmo B — partições com lease e fencing (`relay.mode=partition-lease`)

1. Aquisição/renovação de lease por partição, com `epoch` monotônico; renovação com folga
   (`agora + margem < expires_at`) antes de cada lote.
2. Processamento **sequencial** dentro da partição.
3. **Marcação condicional ao epoch** (doc 12 §12): o dono obsoleto não consegue marcar.
4. **Cercamento no broker quando existir**: no Kafka, produtor com identidade transacional
   `relay-<partition>`; ao assumir, o novo dono invalida a sessão do antigo. No RabbitMQ não há
   equivalente → sob F7, O1 **não é garantido**, e a história H-A mede frequência e magnitude das
   inversões.

### 6.3 Parâmetros

| Parâmetro | Níveis |
|---|---|
| `p` (intervalo) | 10 ms, 100 ms, 1 s |
| `b` (lote) | 1, 100, 1000 |
| `k` (relays) | 1, 2, 4 |
| `relay.mode` | `skip-locked`, `partition-lease` |
| partições (lease) | 16, 64 |
| `cleanup` | `mark`, `delete`, `partition-drop`, `early` (controle) |
| publicação | `sequential`, `async-batch` |

## 7. Outbox com `LISTEN/NOTIFY`

- `NOTIFY` emitido na transação (entregue no commit); relay com conexão dedicada em `LISTEN`,
  drenando até esvaziar, com polling de segurança (1 s) porque notificações se perdem em reconexão.
- **Custo previsto (P3):** transações que notificam disputam um lock global da fila de notificações
  no commit → queda do throughput da operação, crescente com a concorrência (M5). Variantes
  medidas: `notify-per-tx` e `notify-via-statement-trigger`.

## 8. Outbox com CDC

| Variante | Componente | Observações |
|---|---|---|
| `outbox-cdc-connect` | Debezium em Kafka Connect | `wal_level=logical`, publicação só da tabela `outbox`, SMT de roteamento; sem snapshot de dados |
| `outbox-cdc-server` | Debezium Server | Mesma origem, sink para Kafka ou RabbitMQ; offsets em arquivo |

Riscos medidos: WAL retido quando o conector para (F7/S-N14); duplicatas após restart,
proporcionais ao intervalo de flush de offsets; teto de capacidade da decodificação lógica (M7),
que é tarefa única.

## 9. Banco como fila (`pg-queue`, guia)

Publicação = `INSERT` na mesma transação; consumo com `DELETE ... RETURNING` + `SKIP LOCKED` na
transação do consumidor. Sem fan-out nativo; O0 com N workers.

## 10. Consumidor

- Contrato padrão **C-proc** (confirma depois de processar). **C-auto** entra como controle
  negativo no cenário de falha do consumidor (doc 12 §7).
- O consumidor de benchmark **não deduplica**: ele conta multiplicidade por grupo. O custo de
  deduplicar é medido à parte (guia), com `dedup-memory`, `dedup-pg` e `dedup-redis`.

## 11. Alegações e testes que as verificam

Nenhuma linha desta tabela é premissa: cada uma é uma alegação com teste associado.

| Estratégia | Alegação | Verificada por |
|---|---|---|
| `outbox-poll` (k=1 ou lease) | Sem perda; duplicatas possíveis; O1 | H-B, H-A, N14 |
| `outbox-poll` (`skip-locked`, k>1) | Sem perda; **sem O1** | H-A, N14 |
| `outbox-notify` | Igual ao polling, com custo no commit | H-B, P3 |
| `outbox-cdc` | Sem perda; duplicatas após restart; O1 pela ordem do WAL | H-B, N14 |
| `dw-after` | **Perde** sob F1/F6 | H-C |
| `dw-before` | Gera **fantasma** | H-D |
| `cleanup=early` | **Perde** sob F2 | H-J |
| `pg-queue` | Sem perda; sem duplicata (consumo transacional); O0 | N14 |
| Qualquer | Contrato não testado = **"não verificado"** | doc 12 §10 |
