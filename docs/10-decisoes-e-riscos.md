# 10 — Decisões e riscos

## 1. ADRs

| ADR | Decisão | Alternativas | Motivo |
|---|---|---|---|
| 001 | Harness em Java 21 | Go, Rust, multi-linguagem | Clientes oficiais maduros; uma linguagem para todos |
| 002 | Clientes puros, sem framework no caminho quente | Spring | Isolar broker de framework |
| 003 | Orquestração e análise em Python | Java, shell | Ecossistema de dados |
| 004 | Docker Compose por host + contextos remotos | Swarm/Kubernetes | Evitar overlay no caminho medido (doc 07 §3.2) |
| 005 | Envelope binário fixo no corpo | JSON, headers, Protobuf | Custo de serialização idêntico |
| 006 | Malha aberta; população por `t_int`; censura explícita | Malha fechada; janela por conclusão | Não truncar a cauda nem esconder pendentes |
| **007** | **Contratos K/E/O/S** no lugar de perfis de durabilidade | "Perfis D0–D3" com nota de rodapé | Paridade se especifica e se verifica, não se declara (doc 12) |
| **008** | **Ordem canônica = versão do agregado sob lock** | Ordem de commit, de `id`, de publicação | Torna a propriedade definível e verificável (doc 12 §5.1) |
| **009** | **Fencing em duas camadas** (epoch no banco + cercamento no broker) | Só lease com expiração | Lease sozinha não impede dono obsoleto (doc 12 §12) |
| **010** | Verificador com **multiplicidade por grupo** e categoria **`indeterminado`** | Contadores locais; perda por contagem | Evita duplicata invisível e perda falsa (doc 06 §8) |
| **011** | **Fault points determinísticos** nas janelas críticas | Crashes aleatórios | Sorteio pode nunca atingir a janela (doc 04 §3.2) |
| **012** | **F2 com perda real de page cache** (reset de VM / `dm-log-writes`) | `docker kill` | Matar container não testa `fsync` (doc 07 §4) |
| **013** | **Escopo reduzido**: PostgreSQL + 2 brokers + 3 estratégias no núcleo | Catálogo amplo como núcleo | Representatividade, correção e causalidade viram obrigatórias (doc 00 §2) |
| **014** | **Ablações e fatoriais** no lugar de um fator por vez | OFAT | Os fatores interagem; OFAT não estima interação (doc 13 §4) |
| **015** | **Modelo preditivo com configurações seladas** | Só caracterização | Transforma descrição em explicação testável (doc 13 §5) |
| **016** | **Carga representativa é o padrão**; a uniforme é controle | Uniforme como padrão | Recomendação precisa valer fora do laboratório (doc 04 §1) |
| **017** | **Uma única regra estatística** (margem, referência pré-especificada, precisão-alvo) | Regras por documento | Elimina a inconsistência entre docs 05 e 09 |
| **018** | **Custo só extrapola se a linearidade for testada**; memória = anônima + working set medido | Extrapolação linear; só memória anônima | Premissas capazes de inverter a recomendação (doc 03 §7) |
| **019** | Guia é **caracterização declarada**, separado do núcleo | Guia como contribuição | Novidade exige lacuna demonstrada (doc 11) |
| **020** | **Lacuna e decisão de instrumento antes do harness** | Construir e depois justificar | Evita dataset caro que não responde à pergunta |
| **021** | Vereditos do autor só sobre `equivalente`/`inconclusivo`, com IC ao lado | Veredito livre; ranking ponderado | Opinião não substitui incerteza (doc 09 §5) |
| **022** | Dados brutos fora do git, com checksum | Git LFS | Volume |

Novas decisões vão para `docs/adr/NNN-titulo.md` com uma linha aqui.

## 2. Questões em aberto

| ID | Questão | Recomendação | Até |
|---|---|---|---|
| Q1 | Estender o OMB ou construir instrumento próprio? | Decidir pelo spike da F0 (doc 11 §6) | F0 |
| Q2 | Nuvem ou bare metal para o ambiente dedicado? | Bare metal reduz ruído; nuvem facilita reset de VM para F2 e a segunda mídia | F0 |
| Q3 | Fonte da carga representativa (traço público, sistema real ou premissa declarada) | Buscar traço público; na falta, premissa declarada + sensibilidade | F1 |
| Q4 | TLA+ ou Alloy para `spec/` | TLA+ se o foco for composição temporal; Alloy se for contraexemplo estrutural | F1 |
| Q5 | Redis/NATS/Pulsar entram no guia P1 ou P2? | P1 conforme cronograma pós-núcleo | F11 |
| Q6 | Publicação: artigo científico, série de posts ou site navegável? | Artigo do núcleo + guia navegável | F7 |
| Q7 | Revisão externa: quem revisa contratos e configurações? | Convidar pessoas de cada tecnologia antes do congelamento | F8 |

## 3. Riscos

| ID | Risco | Prob. | Impacto | Mitigação |
|---|---|---|---|---|
| R1 | A revisão mostra que a lacuna não existe | Média | Alto | Fase F0 é barata comparada à campanha; a pergunta é revista antes de qualquer código |
| R2 | Gerador ou janela de voo limitam o MST | Média | Alto | Três evidências obrigatórias (doc 03 §4.2) |
| R3 | F2 não reproduzível no ambiente escolhido | Média | Alto | Reset de VM como critério de escolha do ambiente; `dm-log-writes` como alternativa; o que não for testado é "não verificado" |
| R4 | Orçamento de máquina (~337 h) estoura | Alta | Médio | Exploratória primeiro; repetições por precisão; guia só depois do núcleo; ambientes paralelos |
| R5 | Precisão-alvo não atingida em resultados primários | Média | Médio | Escalonar até `R_max`; publicar `inconclusivo` |
| R6 | Modelo preditivo falha na validação | Média | Médio | É resultado publicável (refutação), não retrabalho silencioso |
| R7 | Contratos não equivalentes geram comparação injusta | Média | Alto | Asserção de contrato efetivo; grupos por contrato; classe de serviço declarada |
| R8 | Bugs no verificador produzem integridade falsa | Média | Alto | `chaos`, histórias mínimas na CI, modelo formal |
| R9 | Carga representativa mal fundamentada | Média | Alto | Origem de cada parâmetro declarada; sensibilidade; N15 compara com controle |
| R10 | Escopo volta a crescer | Alta | Médio | Núcleo fechado por ADR 013; guia só na F11 |
| R11 | Perda de dados brutos | Baixa | Alto | Checksums e cópia redundante |

## 4. Congelamento do protocolo (`protocol-v1`)

Inclui: cenários, cargas, configurações e contratos alvo, `versions.env`, código de análise dos
resultados primários, critérios de MST e validação, previsões P1–P9, lista de comparações primárias
para a correção de multiplicidade, **arquivo de previsões do modelo** e **conjunto selado de
configurações de validação**. Mudança posterior = desvio documentado no artigo.
