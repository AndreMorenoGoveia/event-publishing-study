# 11 — Trabalhos relacionados e declaração de lacuna

> **Este documento é pré-requisito de qualquer linha de código do harness.** Enquanto a lacuna não
> estiver demonstrada, o estudo não tem contribuição definida — apenas mais medições sobre um
> terreno já medido. Mais brokers, mais métricas e versões mais novas **não** constituem novidade.

## 1. O que precisa ser demonstrado

| Afirmação | Como se sustenta |
|---|---|
| Existe uma pergunta relevante **sem resposta** na literatura e nos artefatos de engenharia | Matriz de trabalhos (§4–§5) mostrando, para cada antecedente, o que ele responde e o que fica fora |
| A pergunta **pode ser respondida** com o desenho proposto | Contratos verificáveis (doc 12), desenho causal e modelo preditivo (doc 13) |
| O instrumento novo **é necessário** | Avaliação explícita de reúso/extensão do OpenMessaging Benchmark e afins (§6) |

Saída obrigatória desta fase: `docs/relacionados/matriz.csv`, o texto "Declaração de lacuna"
(§5.3) e a ADR do instrumento (§6.3). Sem esses três, a implementação não começa (doc 08, F0).

## 2. Protocolo da revisão

Revisão **sistemática leve**, registrada antes de ser executada.

| Item | Definição |
|---|---|
| Fontes | ACM DL, IEEE Xplore, Scopus, DBLP, arXiv, Google Scholar (para snowballing), além de artefatos de engenharia: repositórios de benchmark, relatórios de fornecedores e análises de segurança de dados (tipo Jepsen) |
| Consultas (exemplos) | `("message broker" OR "message queue" OR "event streaming") AND (benchmark OR "performance evaluation")`; `("transactional outbox" OR "dual write" OR "change data capture") AND (latency OR throughput OR consistency)`; `("exactly-once" OR "at-least-once") AND (verification OR "fault injection") AND (broker OR queue)`; `("polling" OR "LISTEN/NOTIFY" OR "logical replication") AND outbox` |
| Janela | Publicações dos últimos 12 anos + clássicos citados por elas (sem limite para teoria de filas e trabalhos fundacionais) |
| Inclusão | Trabalho que compara ≥ 2 mecanismos de publicação/entrega de eventos **ou** que caracteriza garantias (durabilidade, ordem, entrega) sob falhas **ou** que modela capacidade/latência desses mecanismos |
| Exclusão | Sem método reprodutível descrito; material promocional (mantido em lista separada, marcado como tal); domínio incompatível (ex.: filas intra-processo sem rede) |
| Snowballing | Uma rodada para trás (referências) e uma para a frente (citações) a partir dos incluídos |
| Encerramento | Saturação: uma rodada inteira sem novo trabalho que altere a matriz |

### 2.1 Campos extraídos de cada trabalho

`pergunta` · `sistemas/mecanismos` · `garantias declaradas` · `garantias verificadas (como?)` ·
`modelo de falhas` · `workload (chegada, payload, chaves, concorrência)` · `malha aberta?` ·
`tratamento de coordinated omission` · `instrumento e disponibilidade` · `estatística (repetições, incerteza)` ·
`reprodutibilidade (artefatos, versões)` · `conclusões` · `limitações declaradas` ·
`o que fica sem resposta`.

## 3. Como a matriz vira decisão

1. Cada célula "o que fica sem resposta" alimenta uma lista de candidatas a lacuna.
2. Uma candidata só vira contribuição se: (a) nenhum trabalho a responde; (b) o desenho deste
   estudo a responde; (c) a resposta muda alguma decisão prática ou alguma explicação.
3. Se a revisão mostrar que a pergunta central já está respondida, **o estudo muda de pergunta** —
   e o documento registra isso. Este é um resultado legítimo da fase.

## 4. Sementes conhecidas (a verificar e extrair na revisão)

Lista inicial, **não** uma revisão. Conteúdo, escopo e limitações de cada item serão extraídos na
revisão; nada aqui é citado como evidência antes disso.

| Semente | Tipo | Por que entra | Status |
|---|---|---|---|
| OpenMessaging Benchmark (OMB) | Artefato de engenharia | Instrumento mais próximo do harness planejado; base da avaliação de reúso (§6) | A avaliar |
| *Benchmarking Message Queues* (Telecom/MDPI, 2023) | Artigo | Comparação entre tecnologias para orientar escolha — antecedente direto do "guia" | A extrair |
| Dobbelaere & Esmaili, *Kafka versus RabbitMQ* (DEBS 2017) | Artigo | Comparação qualitativa e quantitativa entre duas arquiteturas distintas | A extrair |
| Relatórios de análise de segurança de dados sob falhas (tipo Jepsen) para Kafka, RabbitMQ, Redis, NATS | Artefato | Referência de como **verificar** garantias sob falhas, não só medir desempenho | A extrair |
| Laigner et al., *Data management in microservices* (VLDB 2021) e afins | Artigo | Estado da prática de consistência entre banco e eventos; contexto do outbox | A extrair |
| Documentação do padrão Transactional Outbox (microservices.io) e do Debezium | Prática | Define o padrão e sua janela de duplicação; delimita o que já é sabido | A extrair |
| Kleppmann, *How to do distributed locking* | Prática/teoria | Fencing e proprietários obsoletos de lease (doc 12 §11, história H-A) | A extrair |
| Tene, *coordinated omission* | Prática/método | Base do método de medição de latência | A extrair |
| Kounev et al., *Systems Benchmarking* | Livro/método | Rigor de benchmarking: carga, métricas, validade | A extrair |
| Teoria de filas (M/G/1, tempo de espera com lote e com férias do servidor) | Teoria | Base do modelo preditivo do relay (doc 13) | A extrair |
| Benchmarks de fornecedores (Confluent, Redpanda e afins) | Material promocional | Entram em lista separada, só como fonte de configuração e de alegações a testar | A extrair |

## 5. Candidatas a contribuição

### 5.1 Candidatas

| ID | Candidata | O que a falsearia |
|---|---|---|
| N1 | **Fronteiras de decisão** entre polling, polling+NOTIFY e CDC: sob quais condições cada uma satisfaz simultaneamente durabilidade, ordem e latência, e quais mecanismos determinam a troca | Existir trabalho que já mapeie essas fronteiras com garantias explícitas e mecanismos identificados |
| N2 | **Verificação executável de contratos** de durabilidade/ordem/entrega da cadeia banco→relay→broker→consumidor, com falhas determinísticas nas janelas críticas (incluindo lease obsoleta e perda de page cache) | Existir trabalho que verifique essas propriedades na cadeia completa, e não só no broker isolado |
| N3 | **Modelo mecanístico preditivo** das fronteiras (parametrizado por taxa, capacidade do relay, espera de polling, lote, contenção e manutenção do banco), validado **fora da amostra** | Existir modelo equivalente já validado, ou o modelo falhar na validação — caso em que o resultado é a refutação, reportada como tal |

**A pergunta central do estudo passa a ser N1** (ver doc 00 §2), com N2 como pré-requisito
metodológico e N3 como o que transforma caracterização em explicação.

### 5.2 O que **não** é apresentado como contribuição

- A comparação ampla de brokers (Redis, NATS, Redpanda, Pulsar, RabbitMQ streams, pg-queue).
  Ela continua no projeto como **guia operacional** (doc 09), declarada explicitamente como
  *caracterização sob um método documentado*, sem alegação de novidade.
- Atualização de versões, número de métricas ou de cenários.

### 5.3 Declaração de lacuna (a escrever ao fim da revisão)

Texto curto, no artigo e neste documento, no formato:

> Os trabalhos X, Y e Z comparam A e B sob as condições C, verificando as garantias por meio de D.
> Nenhum deles responde E, porque F. Este estudo responde E ao (i) especificar contratos G,
> (ii) verificá-los sob as falhas H, e (iii) explicar as fronteiras por meio do modelo I,
> validado fora da amostra com erro J.

## 6. Decisão sobre o instrumento

### 6.1 Critérios de avaliação do OMB (e de qualquer alternativa)

| Critério | Peso na decisão |
|---|---|
| Carga em malha aberta com latência medida a partir do instante de intenção | Bloqueante |
| Tratamento explícito de eventos pendentes/censurados ao fim da janela | Bloqueante |
| Verificação de perda, duplicação **por grupo** e ordem por agregado | Bloqueante |
| Suporte a padrões com banco (outbox, CDC, dual-write) | Bloqueante para o núcleo |
| Injeção determinística de falhas em janelas críticas | Bloqueante para N2 |
| Contratos configuráveis e asserção de configuração efetiva | Alto |
| Extensibilidade (SPI de adapters), licença, manutenção ativa | Alto |
| Coleta de recursos por componente e por interface de rede | Médio |

### 6.2 Regra de decisão

- Se o OMB atende os bloqueantes ou pode atendê-los com **extensões localizadas**: estende-se o
  OMB; o estudo contribui com as extensões (submetidas ao projeto upstream quando possível).
- Caso contrário: instrumento próprio, com a ADR registrando **quais** bloqueantes faltavam.

### 6.3 Como a decisão é tomada

*Spike* de tempo limitado (3 dias) na F0: executar o OMB em um cenário simples, ler o gerador de
carga e o caminho de métricas, preencher a tabela §6.1 com evidência (arquivo e linha), e
registrar a ADR. O resultado do spike é um artefato do estudo, não uma conversa.

## 7. Produtos desta fase

| Produto | Onde |
|---|---|
| Matriz de trabalhos preenchida | `docs/relacionados/matriz.csv` |
| Fichas dos trabalhos incluídos | `docs/relacionados/fichas/<id>.md` |
| Declaração de lacuna | §5.3 e artigo |
| ADR do instrumento | `docs/adr/016-instrumento.md` |
| Lista de alegações de fornecedores a testar | `docs/relacionados/alegacoes.md` |
