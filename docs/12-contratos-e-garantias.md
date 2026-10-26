# 12 — Contratos de garantia

> Paridade de garantias **não se declara com uma nota de rodapé**: especifica-se, mapeia-se
> configuração a contrato e verifica-se com falhas. Este documento substitui os antigos "perfis de
> durabilidade D0–D3".

## 1. Fronteira de sucesso

Antes de falar em garantia é preciso dizer **onde a operação é considerada bem-sucedida**:

| Padrão | Fronteira de sucesso | Consequência |
|---|---|---|
| `direct` | Ack do broker ao produtor | A partir daí, o broker é o único responsável pelo evento |
| `outbox-*`, `pg-queue` | `COMMIT` da transação de negócio | O banco é o responsável **enquanto a linha existir**; depois, o broker (§8) |
| `dw-after` | `COMMIT` (o evento ainda não existe em lugar nenhum) | Janela de perda por construção |
| `dw-before` | Ack do broker antes do `COMMIT` | Janela de evento fantasma por construção |

Todo resultado de latência, perda e duplicação é sempre relativo à fronteira declarada.

## 2. Modelo de falhas

| ID | Classe de falha | O que se perde | Como é injetada |
|---|---|---|---|
| **F1** | Crash de processo | Estado em memória do processo; **page cache do host permanece** | `docker kill -s KILL` |
| **F2** | Queda de nó (energia/reset) | Memória volátil **e page cache**; sobrevive apenas o que foi `fsync`ado | Reset de VM (API da nuvem/hipervisor) ou `echo b > /proc/sysrq-trigger` na VM; em container, `dm-log-writes`/`dm-flakey` para descartar escritas não sincronizadas |
| **F3** | Perda permanente de um nó | Nó e seu disco | Remoção do nó sem retorno |
| **F4** | Perda de zona | Vários nós correlacionados | Parada simultânea de um subconjunto |
| **F5** | Partição de rede | Nenhum dado; nó vivo e isolado | `iptables` no sidecar de rede |
| **F6** | Falha da aplicação produtora em janela crítica | Depende do ponto exato | **Fault points** determinísticos (doc 06 §9.1) |
| **F7** | Falha do relay/conector, incluindo **zumbi** (lease expirada, dono antigo retoma) | Depende do ponto | Fault points + `pause`/`unpause` prolongado |
| **F8** | Falha do consumidor | Progresso não confirmado; estado local | `kill` do consumidor; restart |
| **F9** | Falha do **observador** (verificador) | Registro da observação | `kill` do consumidor antes do checkpoint |

F2 e F9 são as duas classes que o plano anterior não cobria — e são exatamente as que separam
"sobreviver a crash de processo" de "sobreviver a queda de energia", e "perda" de "não observado".

## 3. Contratos de durabilidade (K)

> "Um evento aceito na fronteira de sucesso sobrevive à classe de falha X."

| Contrato | Sobrevive a | Exige |
|---|---|---|
| **K0** | Nada | — |
| **K1** | F1 (crash de processo) em um nó | Escrita entregue ao SO (page cache) |
| **K2** | F2 (queda de nó) em **um** nó | `fsync` local antes do sucesso |
| **K3** | F3 (perda de um nó) | Cópia em ≥ 2 nós antes do sucesso, com consenso para eleger o sobrevivente |
| **K4** | F3 **e** F2 correlacionada (queda de energia de vários nós / zona) | `fsync` em maioria antes do sucesso |

**Regras de comparação**
1. Comparações de latência/throughput são feitas **dentro do mesmo contrato**.
2. Um sistema que não alcança um contrato **não aparece naquele grupo** — nem com asterisco.
3. Comparações entre contratos diferentes são permitidas, mas são reportadas como
   *"o que custa mudar de contrato"*, nunca como "quem é mais rápido".

## 4. Contratos de entrega (E)

| Contrato | Definição |
|---|---|
| **E0** | Melhor esforço; nenhuma obrigação |
| **E1** | *At-least-once por grupo assinante*: todo evento aceito é entregue ≥ 1 vez a **cada** grupo registrado antes da aceitação |
| **E2** | *Efetivamente uma vez* por grupo (fora do escopo; anotado onde o sistema alega) |

Multiplicidade é sempre **por grupo**: dois grupos recebendo uma cópia cada é fan-out correto;
duas cópias no mesmo grupo é duplicação (doc 06 §8).

## 5. Contratos de ordem (O)

### 5.1 Qual ordem

Quatro ordens candidatas — intenção, versão do agregado, commit e publicação — **não são
equivalentes**. O estudo adota como ordem canônica a **versão do agregado**:

- A transação de negócio toma o lock da linha do agregado (`SELECT ... FOR UPDATE` em `customers`)
  e atribui `aggregate_version = versão_anterior + 1`.
- Sob o lock, ordem de versão **é** ordem de commit para o mesmo agregado; escritores concorrentes
  do mesmo agregado são serializados pelo banco (e a concorrência vira um fator do estudo, não uma
  restrição do gerador).
- Entre agregados diferentes **não há ordem definida** — e isso é declarado, não medido.
- `ORDER BY id` na outbox **não** define a ordem canônica: ele é uma heurística de leitura, e a
  ordem verificada é sempre por `(aggregate_id, aggregate_version)`.

### 5.2 Contratos

| Contrato | Definição |
|---|---|
| **O0** | Sem garantia de ordem |
| **O1** | Ordem por agregado, por grupo: para cada agregado e cada grupo, a sequência das **primeiras entregas** tem `aggregate_version` não decrescente |
| **O2** | Ordem total por partição/fila |

Reentregas (duplicatas) não violam O1 por definição, mas são contadas à parte
(`order_violations_redelivery`), e uma inversão cuja diferença de instantes de entrega seja menor
que a banda de erro de relógio é classificada como `indeterminada` (doc 06 §8).

## 6. Contratos de serviço (S)

Fila e log **não entregam o mesmo serviço**; comparar sem dizer isso é comparar produtos diferentes.

| Contrato | Definição | Exemplos |
|---|---|---|
| **S-fila** | Consumo remove a mensagem; sem releitura após confirmação | RabbitMQ (classic/quorum), `pg-queue`, Redis Streams com trim agressivo |
| **S-log** | Retenção independente do consumo; replay por offset/tempo; novos grupos leem o histórico | Kafka, Redpanda, RabbitMQ Streams, Pulsar, NATS JetStream (conforme política), Redis Streams com retenção |

Regras: comparações **dentro** da mesma classe são diretas; entre classes exigem declaração
explícita da diferença funcional na tabela e na figura. O guia trata replay como **requisito**
(capítulo C6/C11), com teste próprio: tempo e custo para um novo grupo reprocessar N eventos.

## 7. Contratos do consumidor

| Contrato | Definição | Verificação |
|---|---|---|
| **C-proc** | Confirmação só depois de processar (o que garante E1 na ponta) | S-F8: kill do consumidor entre processar e confirmar → duplicata esperada, perda proibida |
| **C-auto** | Confirmação automática/antecipada | S-F8 deve mostrar perda **no consumo** — é o controle negativo |

O consumidor de benchmark usa `C-proc`; `C-auto` entra como controle no cenário de falha do
consumidor.

## 8. Contrato de cadeia e a janela de dependência

Nos padrões com banco, a garantia **muda de responsável ao longo do tempo**:

```
COMMIT ───────────────► linha na outbox existe ───────────────► linha removida/marcada
        durabilidade = contrato K do PostgreSQL        durabilidade = contrato K do broker
```

Consequências, que viram regra do estudo:

1. **A limpeza da outbox só pode ocorrer após o ack do broker no contrato alvo.** Limpar antes
   rebaixa a cadeia ao contrato do broker mais fraco no intervalo — e isso é um erro de
   implementação comum, medido aqui como variante (`cleanup=early`, controle negativo).
2. O contrato da cadeia é o **mínimo** entre o contrato do banco e o do broker, avaliado em cada
   trecho. Anunciar K4 no broker com `synchronous_commit=off` no banco não produz cadeia K4.
3. Por isso o **standby síncrono do PostgreSQL entra no núcleo P0** sempre que o contrato alvo da
   cadeia for K3/K4 — e não em P2, como no plano anterior.

## 9. Mapeamento configuração → contrato

O contrato é **asserido em tempo de execução** lendo a configuração efetiva do sistema (doc 08,
F5); divergência reprova a execução.

| Sistema / configuração | Contrato máximo | Observação |
|---|---|---|
| Kafka `acks=all`, RF3, `min.insync.replicas=2` | **K3** | Sem `fsync` antes do ack: não é K4 |
| Kafka idem + `flush.messages=1` | K4 | Configuração não recomendada; entra como medida do custo de K4 |
| Redpanda `acks=all`, RF3 (sem write caching) | **K4** | `fsync` antes do ack |
| RabbitMQ quorum, 3 réplicas, confirms | **K4** | Confirmar na versão fixada |
| RabbitMQ classic durável, 1 nó, confirms | K2 | Não participa de grupos K3/K4 |
| RabbitMQ Streams, 3 réplicas | K3 | Sem `fsync` explícito |
| NATS JetStream R3 (file) | K3 | K4 com `sync_interval: always` |
| Pulsar E3/W3/A2, journal com fsync | **K4** | |
| Redis Streams, AOF `always`, 1 nó | **K2** | **K3/K4 não atingíveis**: replicação assíncrona sem consenso; failover pode perder escrita confirmada. Redis **não entra** em grupos K3/K4 |
| PostgreSQL `synchronous_commit=on` | **K2** | |
| PostgreSQL + standby síncrono (`on`/`remote_apply`) | **K3/K4** | Necessário para cadeia K3+ |

## 10. Obrigações de verificação

Um contrato só pode ser **afirmado** no guia se as falhas correspondentes tiverem sido injetadas.

| Contrato | Falhas obrigatórias | Se não testado |
|---|---|---|
| K1 | F1 | "não verificado" |
| K2 | F1, **F2** | "não verificado" |
| K3 | F1, F3, F5 | "não verificado" |
| K4 | F1, **F2 correlacionada**, F3, F5 | "não verificado" |
| E1 | F1, F6, F7, F8 (+F9 para separar perda de não observado) | "não verificado" |
| O1 | F7 com **lease expirada e dono antigo retomando** | "não verificado" |

**Linguagem permitida nas conclusões:** "nenhuma perda observada sob F1, F2 e F6, em N execuções,
com a cobertura da tabela X". É proibido escrever "não perde eventos" ou "perda impossível".

## 11. Histórias mínimas (catálogo de violações)

Cada história é um teste determinístico (fault points + Testcontainers) com classificação esperada.
Elas rodam na CI, **não** dependem da campanha, e são pré-requisito para qualquer alegação de contrato.

| ID | História | Mecanismo exercitado | Resultado esperado |
|---|---|---|---|
| H-A | Relay R1 pausa, lease expira, R2 assume e publica, R1 retoma e publica registros antigos | Fencing de lease | **Sem fencing:** violação de O1 detectada. **Com fencing:** publicação do zumbi rejeitada ou marcada (§12) |
| H-B | Crash entre ack do broker e marcação/remoção da linha | Janela de duplicação do outbox | Duplicata por grupo; zero perda |
| H-C | Crash entre `COMMIT` e publicação (`dw-after`) | Dual-write | Perda confirmada |
| H-D | Publicação bem-sucedida e `ROLLBACK` (`dw-before`) | Dual-write | Evento fantasma |
| H-E | Ack sem `fsync` seguido de F2 | K1 × K2 | Perda em K1; zero perda em K2 |
| H-F | Failover de primário Redis com escrita confirmada | Ausência de consenso | Perda possível → Redis fora de K3 |
| H-G | Consumidor confirma antes de processar e morre | C-auto × C-proc | Perda no consumo em `C-auto` |
| H-H | Rebalance de grupo durante entrega | Reentrega | Duplicata por grupo; O1 preservado |
| H-I | Consumidor morre antes do checkpoint do verificador | F9 | Eventos classificados como **indeterminados**, nunca como perdidos |
| H-J | Broker indisponível e limpeza antecipada da outbox (`cleanup=early`) | Janela de dependência (§8) | Perda confirmada — controle negativo |

## 12. Fencing de lease (resolução de H-A)

Proteção em **duas camadas**, porque nenhuma isolada resolve:

1. **No banco (sempre disponível):** a lease carrega um `epoch` monotônico por partição. A
   marcação/remoção das linhas publicadas é condicional:
   ```sql
   UPDATE outbox SET published_at = clock_timestamp()
   WHERE id = ANY(:ids)
     AND EXISTS (SELECT 1 FROM outbox_relay_lease
                 WHERE partition_key = :p AND owner = :me AND epoch = :epoch
                   AND expires_at > clock_timestamp());
   ```
   Se o zumbi perdeu a lease, ele **não marca** — mas isso não desfaz o que ele já publicou.
2. **No broker (quando existe):** produtor com identidade transacional por partição
   (`transactional.id = relay-<partition>` no Kafka) faz o broker **cercar** o produtor obsoleto:
   ao assumir, o novo dono invalida a sessão do antigo. Onde esse mecanismo **não existe**
   (RabbitMQ, Redis, NATS), **O1 não é garantido sob F7** — e essa é uma conclusão do estudo, não
   uma limitação escondida: a história H-A mede a frequência e a magnitude das inversões.

Além disso, o relay só pode publicar enquanto `agora + margem < expires_at` (renovação com folga),
o que **reduz** a janela sem eliminá-la — a diferença entre reduzir e eliminar é o que H-A expõe.

## 13. Modelo formal de apoio

Modelo pequeno em TLA+ (ou Alloy), com escopo limitado e propósito declarado:

| Escopo | Propriedades verificadas |
|---|---|
| Outbox + lease com/sem fencing + broker abstrato | (a) marcação ⇒ publicado ≥ 1 vez; (b) com fencing e lock por agregado, O1 se mantém; (c) **sem** fencing existe contraexemplo (o modelo o gera, e ele vira a história H-A) |
| Composição banco + broker com limpeza | O contrato da cadeia é o mínimo dos trechos (§8), e `cleanup=early` produz contraexemplo (H-J) |
| Classificação do verificador | Entregue/perdido/indeterminado é **completa e disjunta**; nenhuma execução sem prova de quiescência pode classificar "perdido" |

O modelo **não prova o sistema real**. Ele gera histórias mínimas e impede que o desenho afirme
propriedades que a composição não sustenta.
