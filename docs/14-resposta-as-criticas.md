# 14 — Resposta às críticas

Registro do que mudou no plano em resposta à avaliação crítica recebida. Cada item aponta **onde**
a mudança está, para poder ser conferida.

| # | Crítica | Mudança | Onde |
|---|---|---|---|
| 1 | **Contribuição não formulada**; antecedentes (OMB, comparações publicadas) não endereçados | Pergunta central reformulada para **fronteiras de decisão entre polling, NOTIFY e CDC**, com três pilares (fronteiras, verificação executável, modelo preditivo). Revisão sistemática leve com protocolo, matriz e declaração de lacuna virou **pré-requisito bloqueante**, com avaliação explícita de reúso/extensão do OMB. Guia amplo rebaixado a caracterização declarada, sem alegação de novidade | doc 00 §1–§3, doc 11 inteiro, doc 08 F0 |
| 2 | **Paridade de garantias inexistente**; Redis com asterisco em D2; replicação do PostgreSQL em P2 | Perfis D0–D3 **eliminados**. Contratos K/E/O/S com modelo de falhas F1–F9, obrigações de verificação e asserção de configuração efetiva. Redis passa a K2 e **não participa** de grupos K3/K4. Standby síncrono do PostgreSQL entra no núcleo. Introduzida a **janela de dependência** da cadeia e a regra de que limpeza antes do ack rebaixa o contrato (controle `cleanup=early`) | doc 12, doc 01 §4–§5, doc 02 §3.2 |
| 3 | **Verificador deixa passar duplicação e diagnostica perda incorretamente** | Multiplicidade **por grupo** (bitmaps + mapa de estouro consolidados por grupo, não por instância); comparação por **conjuntos**, não por contagens; **prova de quiescência** obrigatória para dizer "perdido"; categoria **`indeterminado`**; observador durável com checkpoint e `fsync` (história H-I) | doc 03 §8, doc 06 §8, doc 12 §11 |
| 4 | **Garantias de ordem excedem os algoritmos**: lease sem proteção contra dono obsoleto; "qual ordem" indefinida; verificação por instância | Ordem canônica definida como **versão do agregado atribuída sob lock** (ordem de versão = ordem de commit); fencing em duas camadas (epoch condicional no banco + cercamento do produtor no broker, onde existe); onde não existe cercamento, **O1 não é garantido** e isso é medido (H-A); verificação de ordem **por grupo**, detectando inversões entre instâncias e após rebalance, com banda de erro de relógio | doc 12 §5 e §12, doc 02 §3.1 e §6.2, doc 03 §8.3 |
| 5 | **Testes de falha não sustentam as afirmações**: kill não remove page cache; crashes aleatórios não atingem janelas críticas | **F2 com perda real de page cache** (reset de VM, `sysrq-b`, `dm-log-writes`); **fault points determinísticos** nas janelas críticas (após ack e antes de marcar, etc.); falhas do consumidor e do observador incluídas; **relatório de cobertura** obrigatório; linguagem de conclusão restrita ("nenhuma perda observada sob … com cobertura …") | doc 07 §4, doc 06 §9.1, doc 04 §3.2, doc 12 §10 |
| 6 | **Medição de latência esconde os eventos mais lentos**; decomposição do outbox pode ser negativa | População definida pelo **instante de intenção**, histogramas selecionados pela fase do evento, estados `concluído`/`falhou`/**`censurado`** e regra de identificabilidade de quantis; decomposição reescrita como **grafo com ramo paralelo** (confirmação ao relay fora da soma) | doc 03 §2 |
| 7 | **MST pode refletir limites do cliente e transientes** | Sustentabilidade passa a exigir **`W(t)`** (trabalho em sistema, incluindo o retido no gerador), **três evidências de não-gargalo do cliente** (escalonamento, sensibilidade à janela de voo, saúde), duração ≥ **3 ciclos de manutenção**, e aprovação em **todas** as repetições (fim da regra 4 de 5) | doc 03 §4 |
| 8 | **Estatística promete mais do que o desenho garante**; regras inconsistentes entre docs | Regra **única** de classificação (margem relativa + piso absoluto, IC pareado por bloco); **referência pré-especificada** (proibido usar a melhor mediana observada); **repetições por precisão-alvo** até `R_max`, com `inconclusivo` publicado como tal; CV monitorado em p99 e MST; multiplicidade com Holm sobre a família pré-registrada; doc 09 passa a **referenciar** a regra em vez de redefini-la; veredito editorial não pode resolver incerteza | doc 05 §7, doc 09 §3 e §5 |
| 9 | **Hipóteses mal especificadas ou não testáveis** | Reescritas como previsões P1–P9 com **população, condições, mecanismo, efeito mínimo e critério de refutação**. H3 virou comparação entre contratos diferentes; a espera do polling passou a ter forma funcional dependente de regime; o custo do outbox ganhou transação e ponto de carga definidos; a hipótese de dual-write virou **controle de correção** | doc 00 §4, doc 13 §5.2 |
| 10 | **Desenho descreve, mas não identifica causas** | Catálogo de mecanismos M1–M12, **ablações A1–A8** (incluindo K3×K4 no mesmo sistema e page cache), **fatorial 2×2 completo de runtime × destino do CDC** promovido a P0, **fatorial fracionado** para os parâmetros do outbox, e tabela de **linguagem permitida por tipo de evidência** | doc 13 §1–§4, doc 04 §3 |
| 11 | **Validade externa subordinada ao catálogo** | **Carga representativa** (skew, rajada, mix de tamanhos, concorrência por agregado) vira o padrão, com origem de cada parâmetro declarada; a carga uniforme fica como controle; a restrição "uma chave por produtor" foi eliminada; **retenção/replay** passa a ser requisito funcional (classes S-fila × S-log) com cenário próprio; validação de **transferência** para outra mídia e outro ambiente | doc 04 §1, doc 12 §6, doc 13 §6, doc 02 §3.1 |
| 12 | **Infraestrutura e custo com lacunas** | Multi-host especificado (Compose por host, contextos Docker remotos, endereçamento anunciado via `hosts.env`, duas interfaces lógicas, coleta remota, rejeição explícita de overlay); custo passa a exigir **teste de linearidade** antes de extrapolar, **working set medido** somado à memória anônima, retenção e gargalos compartilhados na conta, e sensibilidade focada nas premissas capazes de **inverter a recomendação** | doc 07 §3, doc 03 §7 |

## Mudanças estruturais decorrentes

| Mudança | Efeito |
|---|---|
| Duas trilhas com estatutos diferentes | Núcleo científico (P0) × guia operacional (P1/P2) |
| Escopo do núcleo reduzido | PostgreSQL + Kafka + RabbitMQ quorum + 3 estratégias + controles |
| Ordem do roadmap invertida | Lacuna → contratos → verificador → campanha (antes: harness → campanha) |
| Novos documentos | 11 (lacuna), 12 (contratos), 13 (explicação e validação), 14 (este) |
| Novo artefato versionado | `spec/` com modelo formal e histórias mínimas na CI |
| Novo artefato de análise | `analysis/predictions/` com previsões seladas antes da validação |

## O que permanece em aberto

- A **novidade ainda não está demonstrada**: depende da revisão da F0, que pode redirecionar a
  pergunta central. Isso está registrado como risco R1 e como critério de aceite da fase.
- A origem da carga representativa (traço público × premissa declarada) segue como questão Q3.
- A escolha entre estender o OMB e construir instrumento próprio segue como questão Q1.
