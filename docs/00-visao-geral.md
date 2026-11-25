# 00 — Visão geral do estudo

## 1. Contribuição pretendida

O estudo investiga as **fronteiras de decisão entre estratégias de publicação consistente de
eventos** a partir de um banco de dados relacional — polling, polling com `LISTEN/NOTIFY` e CDC —
com **contratos de garantia explícitos** e **explicações verificáveis** para essas fronteiras.

> **Pergunta central.** Sob quais condições polling, polling+NOTIFY e CDC satisfazem
> simultaneamente requisitos de durabilidade, ordem e latência, e **quais mecanismos determinam a
> mudança da estratégia mais eficiente**?

Três pilares sustentam a contribuição, e nenhum deles é "medir mais coisas":

| Pilar | Documento |
|---|---|
| **N1 — Fronteiras de decisão** mapeadas e explicadas | [13-explicacao-e-validacao.md](13-explicacao-e-validacao.md) |
| **N2 — Verificação executável dos contratos** da cadeia banco→relay→broker→consumidor, sob falhas determinísticas | [12-contratos-e-garantias.md](12-contratos-e-garantias.md) |
| **N3 — Modelo mecanístico preditivo**, validado fora da amostra | [13-explicacao-e-validacao.md](13-explicacao-e-validacao.md) §5 |

**A novidade não está demonstrada até a revisão de literatura ser concluída**
([11-trabalhos-relacionados.md](11-trabalhos-relacionados.md)). Essa revisão é pré-requisito de
qualquer linha de código do harness, e pode mudar a pergunta central — o que seria um resultado
legítimo, registrado no próprio documento.

## 2. Duas trilhas, com estatutos diferentes

| Trilha | O que é | Estatuto |
|---|---|---|
| **Núcleo científico** (P0) | Pergunta central, contratos, ablações, modelo e validação. PostgreSQL + **dois brokers de arquiteturas distintas** (Kafka: log replicado com ack sem `fsync`; RabbitMQ quorum: fila replicada por Raft com `fsync`) + **três estratégias de relay** + controles `tx-only`, `dw-after`, `dw-before`, `pg-queue` | Alegação de contribuição original |
| **Guia operacional** (P1/P2) | Caracterização ampla dos demais sistemas (Redis Streams, NATS JetStream, Redpanda, Pulsar, RabbitMQ classic/Streams), organizada por problema para quem precisa escolher | **Sem** alegação de novidade; declarado como caracterização sob método documentado ([09-analise-e-guia.md](09-analise-e-guia.md)) |

A redução de escopo é deliberada: representatividade, correção e controles causais passam a ser
**requisitos obrigatórios**, e catálogo grande deixa de ser o objetivo.

## 3. Perguntas de pesquisa

### Núcleo

| ID | Pergunta |
|---|---|
| QP1 | Quais **contratos** (durabilidade K, entrega E, ordem O, serviço S) cada estratégia efetivamente atinge, e a que custo de latência, throughput e recursos? |
| QP2 | **Onde ficam as fronteiras** entre polling, polling+NOTIFY e CDC no espaço (taxa, intervalo de polling, lote, relays, concorrência, contrato)? |
| QP3 | **Quais mecanismos** determinam a mudança de fronteira (espera de polling, capacidade do relay, contenção, `fsync`, decodificação de WAL, manutenção do banco)? |
| QP4 | O modelo mecanístico **prevê** as fronteiras em configurações seladas, e com que erro? |
| QP5 | As fronteiras **se transferem** para outro armazenamento, outro ambiente e carga representativa? |
| QP6 | Quais **violações de contrato** aparecem sob falhas determinísticas, e com qual cobertura de falhas? |

### Guia (caracterização, sem alegação de novidade)

| ID | Pergunta |
|---|---|
| QG1 | Throughput sustentável, latência e custo computacional de cada sistema, por contrato |
| QG2 | Comportamento sob rajada, backlog, falha de nó e degradação de rede |
| QG3 | Custo financeiro estimado para cargas de referência |
| QG4 | Complexidade operacional relativa |

## 4. Previsões (hipóteses especificadas)

Cada previsão declara **população, condições, mecanismo, tamanho de efeito mínimo e critério de
refutação**. Previsões sem esses cinco itens não entram no protocolo.

| ID | Previsão | População e condições | Mecanismo | Efeito mínimo | Refutada se |
|---|---|---|---|---|---|
| **P1** | Existe taxa de cruzamento `λ*` abaixo da qual o CDC tem p99 de `L_e2e` menor que o polling com `p = 100 ms`, e acima da qual a relação se inverte | Cadeia K3, carga de controle, `b = 100`, `k = 1`, `c = 1`, destino Kafka | M1 (espera `p/2`) domina em λ baixo; M7 (decodificação de tarefa única) domina em λ alto | `λ*` dentro da faixa medida, com IC que não contém os extremos | Não há cruzamento na faixa, ou o modelo erra `λ*` em > 20% |
| **P2** | O efeito de `p` sobre o p99 depende do regime: ≈ `p/2` no subcrítico e < 10% de `p` no saturado (interação `p × λ`) | Polling, K3, `b = 100` | M1 desaparece quando o relay não dorme | Interação com IC fora de zero | Efeito de `p` indistinguível entre regimes |
| **P3** | O custo do `NOTIFY` sobre o throughput da operação cresce com a concorrência de escritores | `tx-realistic`, `c ∈ {8, 32, 64}` conexões | M5 (contenção no commit) | ≥ 10% de queda em 64, monotônica | Queda < 10% ou não monotônica |
| **P4** | Passar de K3 para K4 **no mesmo sistema** custa mais em disco de rede que em NVMe (interação contrato × mídia) | Kafka e JetStream, K3 × K4, duas mídias | M8 (`fsync` por commit) | Queda de MST ≥ 30% em disco de rede e interação com IC fora de zero | Queda < 10% ou ausência de interação |
| **P5** | Limpeza antecipada da outbox (`cleanup=early`) produz perda confirmada sob F2 correlacionada; limpeza após o ack no contrato alvo produz zero perda nas mesmas execuções | Outbox + Kafka K3, história H-J | Janela de dependência (doc 12 §8) | Perda > 0 × perda = 0 | Perda = 0 em ambos com a cobertura declarada |
| **P6** | Sob troca de dono de lease (F7), o relay **sem fencing** viola O1; com fencing no banco **e** cercamento no broker, não viola; com fencing só no banco, ainda viola | Histórias H-A em Kafka (com cercamento) e RabbitMQ (sem) | M12 | ≥ 1 violação por execução em ≥ 80% das execuções sem fencing | Zero violações sem fencing (com a cobertura declarada) |
| **P7** | O p99 de `L_req` com outbox (`p = 10 ms`) fica ≤ 1,2× o de `tx-only`, e o `dw-after` fica acima do outbox | `tx-realistic`, 30% do MST do banco, K2 no banco | Outbox acrescenta um `INSERT` local; dual-write acrescenta publicação síncrona | Razão ≤ 1,2 e ordenação outbox < dual-write | Razão > 1,2 ou dual-write ≤ outbox |
| **P8** | Aumentar escritores por agregado de 1 para 8 reduz o MST da operação em ≥ 30% **sem alterar a ordenação** das estratégias de relay | `tx-realistic`, K3 | M4 (serialização por lock de agregado) | Queda ≥ 30%; ordenação preservada | Ordenação muda — a fronteira depende de contenção |
| **P9** | Limitar a memória do broker a 25% do working set reduz mais o MST do Kafka que o do RabbitMQ quorum | K3, carga de controle | M10 (dependência de page cache) | Diferença de queda ≥ 15 pontos percentuais | Quedas equivalentes |

Previsões do guia (caracterização) ficam em [09-analise-e-guia.md](09-analise-e-guia.md) e **não**
entram na correção de multiplicidade do núcleo.

## 5. Escopo

### Dentro
- Publicação consistente a partir do PostgreSQL: polling, polling+NOTIFY, CDC, com controles.
- Contratos K/E/O/S especificados, mapeados a configurações e **verificados sob falhas**.
- Cadeia completa: banco (inclusive **standby síncrono**, quando o contrato alvo exigir), relay,
  broker, consumidor, observador.
- Ablações, fatoriais, modelo preditivo e validação de transferência.
- Guia operacional com os demais sistemas, em subconjunto de cenários.

### Fora (explicitamente)
- Lógica de negócio no consumidor; o consumidor registra e confirma.
- Replicação entre regiões.
- Benchmark de serviços gerenciados (só modelo de custo).
- *Exactly-once* ponta a ponta (anotado onde o sistema alega, não medido como contrato).
- Schema registry e formatos de serialização.
- Segurança (TLS/SASL) — apenas overhead, no guia.

## 6. Princípios

1. **Contribuição antes de instrumento.** Sem lacuna demonstrada e sem decisão sobre reúso do
   OpenMessaging Benchmark, o harness não começa (doc 11).
2. **Contratos, não "paridade".** Comparação só dentro do mesmo contrato; quem não alcança o
   contrato não aparece no grupo — nem com asterisco (doc 12).
3. **Correção antes de performance.** As histórias mínimas (doc 12 §11) rodam na CI e são
   pré-requisito de qualquer alegação de contrato.
4. **Ausência de observação não é perda.** O verificador tem a categoria **indeterminado**, e
   "perdido" exige prova de quiescência (doc 06 §8).
5. **População definida pela intenção.** A amostra de latência é o conjunto de eventos com
   instante de intenção na janela; pendentes ao fim viram observações **censuradas**, declaradas
   (doc 03 §2).
6. **Sustentabilidade demonstrada.** Trabalho em sistema estável, gerador comprovadamente fora do
   gargalo e duração que cobre os ciclos de manutenção (doc 03 §4).
7. **Regras estatísticas unificadas e precisão-alvo.** Margens declaradas, referência
   pré-especificada, e inconclusivo permanece inconclusivo — veredito editorial não resolve
   incerteza (doc 05 §7).
8. **Explicação por ablação.** A linguagem da conclusão é limitada pelo desenho que a sustenta
   (doc 13 §1).
9. **Validação fora da amostra e de transferência** (doc 13 §5–§6).
10. **Reprodutibilidade total**, dados brutos preservados, resultados negativos publicados.
11. **Guia é caracterização declarada**, separado do núcleo científico.

## 7. Entregáveis

| Entregável | Descrição |
|---|---|
| Matriz de trabalhos relacionados e declaração de lacuna | `docs/relacionados/` (doc 11) |
| Especificação de contratos + modelo formal pequeno + histórias mínimas | doc 12, `spec/` |
| Harness (ou extensão do OMB) e orquestrador | Java 21 + Python |
| Dataset bruto com manifestos | Arquivo com checksum |
| Modelo mecanístico + previsões seladas + erro de validação | `analysis/` |
| **Artigo** do núcleo científico | `docs/artigo/` |
| **Guia operacional** por problema | `docs/guia/` (doc 09) |

## 8. Mapa dos documentos

Ordem de leitura recomendada: 00 → 11 → 12 → 13 → 03 → 05 → 06 → 04 → 07 → 08 → 09 → 10.

| Documento | Conteúdo |
|---|---|
| [00-visao-geral.md](00-visao-geral.md) | Este documento: contribuição, perguntas, previsões, escopo |
| [11-trabalhos-relacionados.md](11-trabalhos-relacionados.md) | Protocolo de revisão, matriz, lacuna, decisão sobre o instrumento |
| [12-contratos-e-garantias.md](12-contratos-e-garantias.md) | Modelo de falhas, contratos K/E/O/S, cadeia, fencing, histórias mínimas |
| [13-explicacao-e-validacao.md](13-explicacao-e-validacao.md) | Mecanismos, ablações, fatoriais, modelo preditivo, transferência |
| [01-sistemas-avaliados.md](01-sistemas-avaliados.md) | Sistemas do núcleo e do guia; mapeamento a contratos; topologias |
| [02-padroes-de-publicacao.md](02-padroes-de-publicacao.md) | Estratégias, DDL, algoritmos de relay, fencing, ordem canônica |
| [03-metricas.md](03-metricas.md) | Latência com censura, MST, rede, disco, custo, verificação |
| [04-cenarios.md](04-cenarios.md) | Cenários do núcleo (N) e do guia (G), orçamento |
| [05-metodologia.md](05-metodologia.md) | Execução, justiça, validação, estatística, ameaças |
| [06-arquitetura-do-harness.md](06-arquitetura-do-harness.md) | Módulos, envelope, SPI, fault points, verificador |
| [07-infraestrutura.md](07-infraestrutura.md) | Ambientes, multi-host, observabilidade, injeção de falhas |
| [08-roadmap.md](08-roadmap.md) | Fases, critérios de aceite, ordem de trabalho |
| [09-analise-e-guia.md](09-analise-e-guia.md) | Guia por problema, classes, vereditos, pipeline, figuras |
| [10-decisoes-e-riscos.md](10-decisoes-e-riscos.md) | ADRs, questões em aberto, riscos |
| [14-resposta-as-criticas.md](14-resposta-as-criticas.md) | Cada crítica recebida e onde foi resolvida |
| [15-adaptacao-perfis-e-colab.md](15-adaptacao-perfis-e-colab.md) | Avaliação por perfil (grande × pequena empresa), cenários adaptados e protocolo econômico no Colab |
| [16-seletor-por-metricas.md](16-seletor-por-metricas.md) | Seletor prático por 7 métricas (inclusive aplicações híbridas com múltiplos métodos) |
