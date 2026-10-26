# 09 — Análise e guia de escolha

> O guia é a trilha de **caracterização** do projeto (doc 00 §2). Ele usa o mesmo método —
> contratos, população de latência, verificação com categorias — mas **não** carrega alegação de
> novidade nem entra na correção de multiplicidade do núcleo.

## 1. Formato

Guia de consulta organizado **por problema**: o leitor chega com uma restrição ("minha rede é o
gargalo", "a request ficou lenta", "não posso perder evento") e vai direto ao capítulo.

| Princípio | Consequência |
|---|---|
| Entrada por problema | O índice (§2) é a porta de entrada |
| Capítulos autocontidos | Cada um repete condições de medição, contrato e classe de serviço |
| Dados ≠ opinião | Frases com números são geradas pelo pipeline; veredito fica em bloco próprio |
| Incerteza explícita | Casos próximos e inconclusivos são sinalizados, com o IC ao lado |
| Cobertura declarada | Afirmação de integridade sempre acompanhada das falhas testadas |

## 2. Índice por problema

| Restrição/problema | Capítulo | Cenários |
|---|---|---|
| Volume alto | **C1 — Throughput** | G02, G03, G05, G10 |
| Request lenta ao publicar | **C2 — Latência da request** | N13, G03 |
| Consumidor recebe com atraso | **C3 — Latência ponta a ponta** | N04, G03, G06 |
| CPU, memória, tamanho de cluster | **C4 — Custo computacional** | G01, G03, G22 |
| Rede limitada, tráfego entre zonas, perda/latência | **C5 — Rede** | G03, G08, G15 |
| IOPS, retenção, `fsync` | **C6 — Disco e armazenamento** | N10, G10, G18 |
| Não posso perder evento | **C7 — Consistência e entrega** | N14, G16, G17 |
| Sobreviver a falha de nó | **C8 — Resiliência** | G14, G15, G17 |
| Picos e consumidores lentos | **C9 — Picos e backlog** | G11, G12 |
| Ordem por agregado | **C10 — Ordem** | N14, G07, G20 |
| Muitos assinantes/destinos, replay | **C11 — Fan-out, destinos e replay** | G08, G09, G18 |
| Payloads grandes | **C12 — Tamanho de payload** | G04 |
| Time pequeno | **C13 — Operação** | G01, G21, diário |
| Orçamento de nuvem | **C14 — Custo financeiro** | Modelo (doc 03 §7) |

### 2.1 Perfis de uso
Microsserviço com PostgreSQL emitindo eventos → C7 → C2 → C4 → C8 ·
Telemetria de alto volume → C1 → C4 → C6 → C12 → C14 ·
Notificações quase em tempo real → C3 → C9 → C8 ·
"Já tenho Redis/PostgreSQL" → C13 → C7 → C9 ·
Multi-zona com tráfego caro → C5 → C8 → C14 ·
Muitos consumidores → C11 → C10 → C9.

### 2.2 Índice reverso
Ficha por opção com a **classe** obtida em cada capítulo e links. Sem nota global, sem ranking.

## 3. Regras que o guia herda

| Tema | Fonte única |
|---|---|
| Margens de equivalência e classificação (`equivalente`/`diferente`/`inconclusivo`) | **doc 05 §7.2** |
| Referência pré-especificada de cada comparação | **doc 05 §7.3** |
| Repetições por precisão-alvo | doc 05 §7.4 |
| Quantis identificáveis sob censura | doc 03 §2.2 |
| Contratos e classes de serviço | doc 12 |
| Linguagem permitida por tipo de evidência | doc 13 §1 |

O guia **não redefine** nenhuma dessas regras. Toda tabela de capítulo mostra contrato, classe de
serviço, carga e a classe estatística de cada opção.

## 4. Modelo de capítulo

Blocos entre marcadores são regenerados pelo pipeline; texto do autor fora deles é preservado.

```markdown
## C5 — Rede
**Pergunta:** quantos bytes cada opção coloca na rede e quanto sofre com rede ruim?
**Leia se:** banda limitada, tráfego entre zonas caro, rede com latência/perda.
**Pule se:** tudo na mesma zona com rede folgada.

<!-- dados:condicoes C5 -->
Contrato K3 · S-log e S-fila declarados por linha · carga representativa · campanha ...
<!-- dados:fim -->

<!-- dados:tabela C5 -->
| Opção | Contrato | Classe | Bytes/evento | Replicação (×) | Δ p99 com +10 ms | Δ MST com 1% perda | Classe estatística |
<!-- dados:fim -->

### Como ler   (autor)
### Casos próximos
<!-- dados:proximos C5 -->
- kafka × redpanda em bytes/evento: razão 1,03 (IC 0,98–1,08) → inconclusivo, requer veredito
<!-- dados:fim -->

> **Veredito do autor** — ...
> *Base:* classificação `inconclusivo` (IC 0,98–1,08). *Critérios:* ...

### Ressalvas   (contrato, cobertura de falhas, limitações)
### Ver também
```

## 5. Vereditos

| Regra | Detalhe |
|---|---|
| Quando é obrigatório | Quando duas ou mais opções ficam `equivalente` ou `inconclusivo` na métrica primária do capítulo |
| Quando é proibido | Quando a classificação é `diferente` — aí quem fala é o dado; e **nunca** para transformar `inconclusivo` em afirmação de superioridade |
| O que precisa conter | A classificação estatística e o IC que a originou, e os critérios extra-estatísticos usados (operação, ecossistema, custo) |
| Onde fica | Sempre no bloco `> **Veredito do autor**`, fora dos blocos gerados |
| Lint | `studyctl guide --strict` falha com pendências ou com veredito sobre caso `diferente` |

Veredito editorial **não** resolve incerteza estatística (doc 05 §7.4): ele escolhe entre opções
que os dados não distinguem, e diz isso em voz alta.

## 6. Pipeline

```
runs/* ─► ingest ─► validate ─► aggregate ─► compare ─► classify ─┬─► figures ─► guide
                                     │                            └─► article (núcleo)
                              mst ─► cost ─► model
```

| Etapa | Saída |
|---|---|
| `ingest` | `runs.parquet`, percentis, histogramas fundidos, categorias de integridade, cobertura |
| `validate` | Inválidas excluídas; anomalias e `indeterminado` listados |
| `aggregate` | Mediana e dispersão por configuração, com blocos |
| `compare` | IC pareado por bloco (doc 05 §7.1) |
| `classify` | Classes com margens do doc 05 §7.2 e referência pré-especificada |
| `mst`, `cost`, `model` | MST com evidências de não-gargalo; custo com sensibilidade; modelo e erro |
| `figures`, `guide`, `article` | Figuras com CSV, capítulos, artigo |

Comandos: `studyctl analyze`, `studyctl guide [--strict]`, `studyctl article`.

## 7. Figuras

| ID | Figura |
|---|---|
| F01 | Latência × throughput por opção, com região insustentável e pontos censurados marcados |
| F02 | Distribuição de percentis HDR, truncada no limite de identificabilidade |
| F03 | MST por SLO, com barras de IC e marcação de `inconclusivo` |
| F04 | CPU do SUT por milhão de eventos × λ |
| F05 | Custo de contrato (K3 → K4) por sistema e mídia |
| F06 | MST em ev/s e MiB/s × tamanho de payload |
| F07 | Linha do tempo de falha com marcadores de fault point e cobertura |
| F08 | Backlog: memória/disco × acumulado; drenagem |
| F09 | Decomposição do outbox em estágios **sequenciais** (ramo de confirmação à parte) |
| F10 | `L_req` e MST da operação por estratégia, contra `tx-only` |
| F11 | Soak: p99 por janela com regressão |
| F12 | Custo mensal com faixa de sensibilidade e marcação de extrapolação não validada |
| F13 | Mapa de integridade (entregue/duplicado/perdido/indeterminado/fantasma) × falha, com cobertura |
| F14 | Pareto p99 × CPU por evento |
| F15 | Sensibilidade à rede (Δ p99 e Δ MST × latência e perda) |
| F16 | Bytes de rede por evento decompostos |
| F17 | Escrita em disco por mil ev/s e amplificação |
| F18 | **Fronteiras**: regiões de melhor estratégia no plano (λ, p), com `λ*` medido e previsto |

Regras: toda figura declara contrato, classe de serviço, carga, cobertura quando aplicável,
versões e campanha; cor fixa por opção; CSV ao lado; `inconclusivo` nunca é desenhado como diferença.

## 8. Estrutura do guia

1. Como usar · 2. Glossário (percentis, MST, contratos K/E/O/S, classes estatísticas) ·
3. Opções em uma página (com contrato máximo e classe de serviço) · 4. Capítulos C1–C14 ·
5. Fichas por opção · 6. Apêndices: método, calibração, cobertura de falhas, limitações,
anomalias, preços datados, como reproduzir.

## 9. Publicação

`docs/guia/` (Markdown + figuras) e HTML; resumos versionados; brutos arquivados com checksum;
tag `guide-v1`. O artigo do núcleo é publicado em `docs/artigo/` com tag `article-v1`.
