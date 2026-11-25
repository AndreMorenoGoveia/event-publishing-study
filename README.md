# Event Publishing Study

Estudo sobre **publicação consistente de eventos** a partir de um banco relacional, com duas
trilhas de estatutos diferentes:

- **Núcleo científico** — as *fronteiras de decisão* entre **polling**, **polling + `LISTEN/NOTIFY`**
  e **CDC**: sob quais condições cada estratégia satisfaz durabilidade, ordem e latência ao mesmo
  tempo, e **quais mecanismos** determinam a troca. Contratos de garantia explícitos, verificados
  sob falhas determinísticas, e um modelo mecanístico validado **fora da amostra**.
- **Guia operacional** — caracterização ampla (Redis Streams, NATS JetStream, Redpanda, Pulsar,
  RabbitMQ classic/Streams, PostgreSQL como fila) organizada **por problema**, para quem precisa
  escolher. Declarada como caracterização, **sem** alegação de novidade.

> **Status:** planejamento. A contribuição depende da revisão de literatura da fase F0, que é
> bloqueante e pode redirecionar a pergunta central. Ver [roadmap](docs/08-roadmap.md).

## Pergunta central

> Sob quais condições polling, polling+NOTIFY e CDC satisfazem simultaneamente requisitos de
> durabilidade, ordem e latência, e quais mecanismos determinam a mudança da estratégia mais
> eficiente?

## Como o estudo se protege de conclusões erradas

| Risco | Proteção |
|---|---|
| Comparar garantias diferentes | Contratos K/E/O/S, asserção da configuração efetiva, quem não alcança não entra no grupo |
| Chamar de perda o que não foi observado | Categoria `indeterminado` + prova de quiescência |
| Duplicação invisível | Multiplicidade **por grupo**, não por instância |
| Ordem não definida | Ordem canônica = versão do agregado sob lock; fencing em duas camadas |
| Falha que não testa `fsync` | F2 com perda real de page cache (reset de VM / `dm-log-writes`) |
| Janela crítica nunca atingida | Fault points determinísticos + relatório de cobertura |
| Cauda truncada | População por instante de intenção + censura explícita |
| MST limitado pelo cliente | `W(t)` + três evidências de não-gargalo + ciclos de manutenção |
| Estatística frouxa | Margem única, referência pré-especificada, precisão-alvo, `inconclusivo` publicado |
| Descrição virando causa | Ablações, fatoriais e linguagem limitada pelo desenho |

## Guia por problema

Volume alto → C1 · request lenta → C2 · atraso no consumo → C3 · CPU/memória → C4 · rede → C5 ·
disco e retenção → C6 · não perder evento → C7 · falha de nó → C8 · picos e backlog → C9 ·
ordem → C10 · fan-out e replay → C11 · payload grande → C12 · operar pouco → C13 · custo → C14.
Detalhes em [docs/09-analise-e-guia.md](docs/09-analise-e-guia.md).

## Documentação

Ordem de leitura: 00 → 11 → 12 → 13 → 03 → 05 → 06 → 04 → 07 → 08 → 09 → 10.

| # | Documento | Conteúdo |
|---|---|---|
| 00 | [Visão geral](docs/00-visao-geral.md) | Contribuição, perguntas, previsões P1–P9, escopo, princípios |
| 11 | [Trabalhos relacionados](docs/11-trabalhos-relacionados.md) | Protocolo de revisão, matriz, lacuna, decisão do instrumento |
| 12 | [Contratos de garantia](docs/12-contratos-e-garantias.md) | Falhas F1–F9, contratos K/E/O/S, cadeia, fencing, histórias mínimas |
| 13 | [Explicação e validação](docs/13-explicacao-e-validacao.md) | Mecanismos, ablações, fatoriais, modelo preditivo, transferência |
| 01 | [Sistemas](docs/01-sistemas-avaliados.md) | Núcleo e guia, contratos por sistema, topologias, cadeias |
| 02 | [Estratégias](docs/02-padroes-de-publicacao.md) | DDL, ordem canônica, relays com fencing, CDC, controles |
| 03 | [Métricas](docs/03-metricas.md) | Latência com censura, `W(t)`, MST, rede, disco, custo, verificação |
| 04 | [Cenários](docs/04-cenarios.md) | N01–N18 (núcleo), G01–G22 (guia), cargas, orçamento |
| 05 | [Metodologia](docs/05-metodologia.md) | Execução, justiça, validação, estatística única, ameaças |
| 06 | [Harness](docs/06-arquitetura-do-harness.md) | Envelope, SPI, gerador, verificador, fault points, formatos |
| 07 | [Infraestrutura](docs/07-infraestrutura.md) | Multi-host, F2 real, mídia de disco, working set, observabilidade |
| 08 | [Roadmap](docs/08-roadmap.md) | F0–F12 com critérios de aceite |
| 09 | [Análise e guia](docs/09-analise-e-guia.md) | Guia por problema, classes, vereditos, pipeline, figuras |
| 10 | [Decisões e riscos](docs/10-decisoes-e-riscos.md) | ADRs, questões abertas, riscos, congelamento |
| 14 | [Resposta às críticas](docs/14-resposta-as-criticas.md) | O que mudou e onde |
| 15 | [Adaptação por perfil e Colab](docs/15-adaptacao-perfis-e-colab.md) | Validade para grande × pequena empresa, cenários A-S1/A-E1/A-E2/A-F1 e protocolo econômico no Colab |

## Stack

Java 21 (harness) · Python + uv (orquestração, análise, modelo) · TLA+/Alloy (`spec/`) ·
Docker Compose por host com contextos remotos · Prometheus + cAdvisor + Grafana ·
PostgreSQL (+ standby síncrono) + Debezium.
