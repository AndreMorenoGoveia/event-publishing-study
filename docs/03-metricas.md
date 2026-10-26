# 03 — Métricas

Toda métrica tem definição, fonte, unidade e regra de agregação. Nenhum número entra no artigo ou
no guia sem estar definido aqui.

## 1. Instantes medidos

| Símbolo | Quem registra | Definição |
|---|---|---|
| `t_int` | gerador | Instante **planejado** de início da operação |
| `t_send` | produtor | Chamada de publicação (ou início da transação) |
| `t_tx_begin`, `t_tx_commit` | produtor | Início e retorno do `COMMIT` |
| `t_ack` | produtor | Ack do broker no contrato alvo (`direct`, `dw-*`) |
| `t_relay_read` | relay | Linha lida da outbox (amostrado) |
| `t_relay_send` | relay | Publicação enviada (amostrado) |
| `t_relay_ack` | relay | Ack recebido pelo relay (amostrado) |
| `t_recv` | consumidor | Primeira entrega ao handler |
| `t_cack` | consumidor | Confirmação de consumo concluída |

**Relógios.** Mesmo processo: `System.nanoTime()`. Entre processos: relógio de parede em µs
transportado no envelope. Multi-host: `chrony`, com offset medido antes e depois e registrado; a
**banda de erro** resultante é usada na verificação de ordem (§8) e invalida `L_e2e` se exceder 5%
do p50 reportado.

## 2. Latência: população, censura e decomposição

### 2.1 População

A amostra de uma execução é **o conjunto de eventos cujo `t_int` cai na janela de medição** — não
os que terminaram dentro dela. Consequências obrigatórias:

- O registro continua durante a drenagem; cada evento termina em um destes estados:

  | Estado | Significado |
  |---|---|
  | `concluído` | Tem `t_ack` (produtor) e/ou `t_recv` (ponta a ponta) |
  | `falhou` | Erro definitivo na publicação/operação; **não** entra na latência, entra no `error_rate` |
  | `censurado` | Ainda pendente quando a drenagem terminou; o valor é conhecido apenas como "> limite" |

- Os histogramas são selecionados pela **fase do `t_int`** do evento, não pela fase em que ele
  concluiu. Sem isso, os eventos mais lentos desaparecem da janela — exatamente a cauda de interesse.

### 2.2 Regra de identificabilidade

Com fração `c` de observações censuradas (censura à direita, todas no mesmo limite), os quantis
até `1 − c` são identificáveis. Acima disso, o quantil é reportado como **limite inferior**
(`> x`), nunca como valor. Comparações estatísticas só usam quantis identificáveis nos dois lados.

### 2.3 Métricas

| Métrica | Fórmula | Uso |
|---|---|---|
| `L_pub` | `t_ack − t_int` | Principal do produtor no `direct` |
| `L_svc` | `t_ack − t_send` | Diagnóstico; nunca comparação sob carga |
| `L_e2e` | `t_recv − t_int` | Principal ponta a ponta |
| `L_tx` | `t_tx_commit − t_tx_begin` | Transação de negócio |
| `L_req` | `L_pub` no `direct`; fim da operação − `t_int` nos padrões com banco | **Latência da request** (capítulo C2 do guia) |

### 2.4 Decomposição do outbox (grafo, não soma linear)

A entrega ao consumidor e a confirmação ao relay são **ramos concorrentes**. Subtrair um do outro
produz valores negativos e gráficos errados. A decomposição correta:

```
t_int → t_tx_begin → t_tx_commit → t_relay_read → t_relay_send ─┬─► t_recv        (ramo de entrega)
                                                                └─► t_relay_ack   (ramo de confirmação)
```

| Estágio | Fórmula | Sempre ≥ 0 |
|---|---|---|
| `espera_no_gerador` | `t_tx_begin − t_int` | sim |
| `transação` | `t_tx_commit − t_tx_begin` | sim |
| `pickup` | `t_relay_read − t_tx_commit` | sim |
| `relay` | `t_relay_send − t_relay_read` | sim |
| `entrega` | `t_recv − t_relay_send` | sim |
| `confirmação_ao_relay` (ramo paralelo) | `t_relay_ack − t_relay_send` | sim, **fora** da soma |

`L_e2e` = soma dos cinco estágios sequenciais. O efeito de esperar acks de um lote aparece no
`pickup` do lote seguinte, que é onde ele de fato ocorre.

### 2.5 Registro

HdrHistogram (1 µs a 1 h, 3 dígitos), histogramas por intervalo de 1 s em `.hlog`, com tag
`<métrica>:<fase>`. Decomposição por amostragem determinística de 1% (hash do `eventId`), unida
offline. Percentis reportados: p50, p90, p99, p99.9, p99.99 e max, sujeitos a `N ≥ 100/(1−q)` **e**
à regra de censura (§2.2).

## 3. Chegada e *coordinated omission*

- Malha aberta: `t_int(i) = t0 + i/λ` (`constant`) ou intervalos exponenciais (`poisson`), ou o
  processo da carga representativa (doc 04). Latência sempre medida a partir de `t_int`.
- **Schedule lag** = `t_send − t_int`. Backpressure do sistema é latência legítima; falta de CPU no
  gerador invalida a execução (doc 05 §5).
- Malha fechada existe só para estimar capacidade bruta (`X_sat`), nunca para latência.

## 4. Throughput e sustentabilidade

| Métrica | Definição |
|---|---|
| `λ` | Taxa ofertada |
| `ack_rate` | Acks positivos por segundo |
| `delivery_rate` | Eventos **únicos** entregues por segundo, por grupo |
| `error_rate` | Falhas definitivas por segundo |
| `X_sat` | `ack_rate` em regime estacionário, malha fechada |
| **`W(t)`** | **Trabalho em sistema** = `|{e : t_int(e) ≤ t}| − |{e entregue a todos os grupos até t}|` |
| `lag_native` | Lag reportado pelo próprio sistema (offset lag, profundidade de fila, `XPENDING`, pendentes na outbox, WAL retido) |

`W(t)` é a correção ao lag antigo: ele inclui o que ainda está **retido no gerador**, o que está
em voo sem confirmação, o backlog do broker e o pendente no consumidor. Nenhuma fila da cadeia
fica de fora.

### 4.1 MST — throughput máximo sustentável

`λ` é sustentável quando, na janela de medição:

1. `ack_rate ≥ 0,99 λ` e `delivery_rate ≥ 0,99 λ` em **todos** os grupos;
2. p99 de `L_e2e` ≤ SLO (10 ms, 100 ms e 1 s, reportados separadamente);
3. **`W(t)` estável**: inclinação da regressão ≤ 0,5% de `λ` por segundo e `W` final ≤ `0,5 s · λ`;
4. `error_rate = 0` e nenhum evento censurado além de `0,1%`;
5. **gerador comprovadamente fora do gargalo** (§4.2);
6. duração ≥ **três ciclos de manutenção** do mecanismo mais lento envolvido (§4.3).

### 4.2 Prova de que o cliente não é o gargalo

Três evidências, todas obrigatórias no resultado de MST:

| Evidência | Critério |
|---|---|
| Escalonamento do gerador | Dobrar os recursos do gerador (processos/hosts) muda o MST em < 5% |
| Sensibilidade à janela de voo | MST com `max_in_flight` em 0,5× e 2× varia < 5%; caso contrário o MST é reportado como **dependente da janela**, com a curva |
| Saúde do gerador | CPU < 50% e schedule lag atribuível ao gerador (calibração `noop`) < 1 ms no p99 |

Um `noop` rápido **não** é prova suficiente: é apenas uma das três.

### 4.3 Ciclos de manutenção

| Mecanismo | Ciclo típico a cobrir |
|---|---|
| PostgreSQL: checkpoint, autovacuum da outbox | `checkpoint_timeout` e execuções de autovacuum |
| Kafka: roll de segmento, verificação de retenção | Intervalo de verificação de retenção |
| RabbitMQ quorum: WAL, snapshots do Raft | Ciclo de snapshot |
| JetStream: `sync_interval` | Intervalo de sincronização |
| CDC: flush de offsets, rotação de slot | Intervalo de flush |

Execuções de confirmação de MST duram **≥ 3 ciclos** (na prática 15–30 min). MST obtido em
execuções curtas é rotulado **provisório** e não entra em conclusão.

### 4.4 Aceitação entre repetições

O MST confirmado é a maior taxa que passa em **todas** as repetições. Se uma repetição falha, a
taxa é reduzida e a repetição que falhou é **analisada e reportada** (qual critério falhou, e por
quê). Não existe regra de "4 de 5".

## 5. Recursos

Amostragem de 1 s (cAdvisor → Prometheus) **e** leitura direta de cgroup v2 no início/fim da janela.

| Métrica | Fonte | Observação |
|---|---|---|
| CPU | `cpu.stat usage_usec` | Núcleos, média e p95 |
| Throttling | `cpu.stat nr_throttled` | > 1% marca `cpu-bound` |
| Memória anônima | `memory.stat anon` | |
| Page cache | `memory.stat file` | Reportado à parte; **e** working set (§7.2) |
| Disco | `io.stat` | MiB/s e IOPS |
| Rede | por interface | Separa `client-net` de `cluster-net` |
| JVM | JFR | GC, alocação |
| PostgreSQL | `pg_stat_*`, WAL, slots, locks | Inclui WAL retido e dead tuples |
| `fsync` | eBPF/`strace` | Só em execução de diagnóstico |

## 6. Eficiência

Calculadas em uma taxa de referência comum e no MST de cada opção.

| Métrica | Fórmula |
|---|---|
| CPU do SUT por milhão de eventos | Σ CPU-s dos componentes do SUT ÷ (entregues ÷ 10⁶) |
| CPU do cliente por milhão | Produtor + consumidor (+ relay) |
| Bytes de rede por evento | Σ tx cliente→broker ÷ eventos |
| Amplificação de replicação | Σ bytes `cluster-net` ÷ Σ bytes recebidos |
| Amplificação de escrita | Σ `wbytes` ÷ (eventos × envelope) |
| Bytes armazenados por evento | Disco ao final ÷ eventos retidos |
| Custo do banco por operação | CPU do PostgreSQL e WAL por operação, contra `tx-only` |

### 6.1 Métricas de rede (capítulo C5 do guia)

| Métrica | Definição |
|---|---|
| Bytes cliente→broker e broker→consumidor por evento | Por interface, ÷ eventos |
| Overhead de protocolo | Bytes por evento − tamanho do envelope |
| Amplificação de replicação | Ver acima |
| Amplificação de latência | Δ p99 observado ÷ latência injetada (cenário de rede) |
| Sensibilidade a perda | MST com 0,1% e 1% de perda ÷ MST sem perda |
| Custo de tráfego entre zonas | Bytes por milhão de eventos × preço/GB |

### 6.2 Métricas de disco (capítulo C6)

Escrita por mil ev/s (IOPS e MiB/s) · amplificação de escrita · bytes retidos por evento ·
**dependência de `fsync`** (Δ MST e Δ p99 entre K3 e K4 no mesmo sistema) · custo da limpeza
(retenção, compactação, autovacuum).

### 6.3 Composição do SUT

| Estratégia | Componentes somados |
|---|---|
| `direct` | Broker + metadata store |
| `dw-*` | Broker + PostgreSQL (+ standby) |
| `outbox-poll`, `outbox-notify` | Broker + PostgreSQL (+ standby) + relays |
| `outbox-cdc` | Broker + PostgreSQL (+ standby) + Connect/Debezium Server |
| `pg-queue` | PostgreSQL |

Produtor e consumidor de benchmark são sempre reportados à parte.

## 7. Modelo de custo

### 7.1 Cargas de referência

W1: 500 ev/s · W2: 10 mil ev/s · W3: 100 mil ev/s — payload da carga representativa, retenção
1/3/7 dias, contrato declarado (não "D2" genérico).

### 7.2 Memória: anônima **não** basta

Sistemas dependentes de page cache (Kafka) precisam de memória além do heap. O modelo usa
`memória_necessária = anon + working_set`, onde o **working set é medido**, não estimado: a
ablação A8 (doc 13 §3) reduz a memória do container até o desempenho cair além da margem de
equivalência; o menor valor que ainda sustenta o MST é o working set daquela carga e retenção.

### 7.3 Capacidade: linearidade é hipótese, não premissa

```
clusters = ceil( λ_alvo / (MST_medido × headroom) )
```
só é aplicável se a extrapolação for **testada**. Por isso:

- O MST é medido em **duas escalas** (ex.: 3 nós e 6 nós, ou dois tamanhos de nó). Se o ganho medido
  divergir da proporção em mais de 20%, o custo é reportado **apenas para as escalas medidas**,
  sem extrapolar.
- Distribuição de carga (partições por nó, chaves enviesadas) entra como fator; hot partition
  limita o cluster antes do agregado (testado no cenário de skew).
- Retenção e gargalos compartilhados (disco, rede entre zonas) entram na conta, e não só vCPU.

### 7.4 Sensibilidade que importa

A análise de sensibilidade cobre exatamente as premissas capazes de **inverter a recomendação**:
headroom (0,5–0,8), preço de instância (±20%), working set (±50%), retenção (×0,5 e ×2),
proporção de tráfego entre zonas e escala (com e sem extrapolação). Cada inversão observada é
reportada como condição, não como nota de rodapé.

### 7.5 Gerenciados

Tabela de preços aplicada às mesmas cargas, com data e fonte; sem comparação de desempenho.

## 8. Verificação (integridade)

O verificador trabalha com **conjuntos e multiplicidades por grupo**, não com contadores locais.

| Conceito | Definição |
|---|---|
| **Conjunto esperado** | Eventos cuja operação teve sucesso na fronteira declarada (doc 12 §1). Nos padrões com banco, é uma **consulta ao banco**; no `direct`, o log durável de acks do produtor |
| **Multiplicidade por grupo** | `m(e, G)` = número de entregas de `e` no grupo `G`, consolidado **entre as instâncias** de `G` |
| `entregue` | `m(e, G) ≥ 1` em todos os grupos |
| `duplicado` | `m(e, G) ≥ 2` em algum grupo (fan-out entre grupos **não** é duplicação) |
| `perdido_confirmado` | Esperado, `m(e, G) = 0`, **com prova de quiescência** (§8.1) |
| **`indeterminado`** | Esperado, não entregue, **sem** prova de quiescência, ou com estado do observador perdido (F9) |
| `fantasma` | Entregue e ausente do conjunto esperado |
| `sem_confirmação` | Operação falhou e o evento não foi entregue (esperado; reportado à parte) |

### 8.1 Prova de quiescência

Só é possível dizer "perdido" quando, após os produtores pararem, **todas** as condições valem:

1. `lag_native = 0` em todos os grupos e filas/partições;
2. pendentes na outbox = 0 e nenhum WAL retido por slot ativo;
3. nenhuma retentativa em voo nos relays/conectores;
4. nenhum progresso de entrega por `T_q` (padrão 60 s) **depois** de 1–3.

Sem isso, o restante é `indeterminado`. Uma execução com `indeterminado > 0` **não** sustenta
alegação de perda nem de ausência de perda: ela reporta a incerteza.

### 8.2 Estruturas e durabilidade do observador

- Por instância de consumidor e por `producerId`: bitmaps `visto` e `visto≥2` + mapa de estouro
  (contagens ≥ 3) → multiplicidade **exata** ao consolidar por grupo.
- Estado do observador com checkpoint + log append-only com `fsync` periódico; o que estiver além
  do último `fsync` quando o consumidor morre vira `indeterminado` (história H-I), nunca perda.

### 8.3 Ordem

Verificação **por grupo** (não por instância): para cada `(grupo, agregado)`, ordenar as
**primeiras entregas** por `t_recv` e exigir `aggregate_version` não decrescente.

- Inversões em eventos marcados como reentrega → `order_violations_redelivery`.
- Inversões cuja diferença de `t_recv` seja menor que a banda de erro de relógio → `ordem_indeterminada`.
- Violações entre instâncias e após rebalance **são detectadas** — é justamente o que o mecanismo
  de particionamento/lease deve evitar.

## 9. Resiliência

| Métrica | Definição |
|---|---|
| Janela de indisponibilidade | Do primeiro erro após `t_fault` até o primeiro segundo com `ack_rate ≥ 0,95 λ` |
| Erros e latência sob falha | Total e p99/max em `[t_fault, t_fault + 60 s]` |
| Tempo de recuperação | Até `W(t)` voltar a ±10% do nível pré-falha |
| Re-sincronização | Até réplicas voltarem a sincronizar |
| Acúmulo no banco | Pico de pendentes na outbox e de WAL retido |
| Integridade | Categorias do §8, incluindo `indeterminado` |
| **Cobertura** | Quais classes de falha (F1–F9) foram injetadas, quantas vezes e em quais janelas |

Toda afirmação de integridade vem acompanhada da cobertura. "Nenhuma perda observada sob F1, F2 e
F6, em N execuções" é permitido; "não perde eventos" não é.

## 10. Complexidade operacional

Rubrica qualitativa de 1 a 5 com evidência textual, **nunca** somada às métricas numéricas:
setup mínimo · parâmetros para alcançar o contrato alvo · observabilidade nativa · operação de
cluster · maturidade do cliente Java · backpressure (explícito, bloqueante ou silencioso) ·
ecossistema · licença. Proxies objetivos ficam em `docs/diario-de-implementacao.md`.
