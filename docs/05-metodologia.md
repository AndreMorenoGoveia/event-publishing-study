# 05 — Metodologia experimental

## 1. Ciclo de vida de uma execução

| # | Etapa | Detalhe |
|---|---|---|
| 1 | Limpeza | `down -v`; page cache do host limpo; host ocioso confirmado |
| 2 | Subida | Compose do SUT com o perfil; healthchecks + sonda de aplicação |
| 3 | Provisionamento | Destinos, DDL, `customers` populado, slots/publicações do CDC |
| 4 | **Asserção de contrato** | Lê a configuração **efetiva** do sistema em execução e compara com o contrato alvo (doc 12 §9). Divergência **reprova** a execução |
| 5 | Impressão digital | Ambiente, versões, seeds, commit, hash das previsões seladas |
| 6 | Consumidores | Iniciados e estabilizados; checkpoint inicial do observador |
| 7 | Aquecimento | `phase=warmup`, descartado, com verificação de suficiência |
| 8 | Medição | Janela definida por `t_int`; snapshots de cgroup no início e fim |
| 9 | Falhas | Timeline com fault points determinísticos; instantes reais registrados |
| 10 | Drenagem | Até **prova de quiescência** (doc 03 §8.1) ou limite |
| 11 | Verificação | Conjuntos esperados, multiplicidade por grupo, ordem, categorias |
| 12 | Validação | Regras de §5 → `valid`, `invalid(motivo)`, `anomaly` |
| 13 | Derrubada | Tudo removido |

## 2. Ambiente

- Hosts dedicados; mesmo tipo de instância; modelo de CPU e **mídia de disco** registrados
  (NVMe local × disco de rede é fator explícito em N10 e N18).
- Governor `performance`; SMT igual; `cpuset` disjuntos quando houver co-residência; swap off.
- Volume dedicado para dados; sistema de arquivos e montagem iguais.
- Rede: mesma zona; RTT e vazão medidos antes da campanha.
- Limites de container idênticos por nó; heaps explícitos.
- `chrony` com offset medido e registrado por execução.
- **F2 exige perda real de page cache**: reset de VM (API do hipervisor/nuvem) ou `dm-log-writes`.
  Matar container **não** testa K2 — essa é a diferença que o plano anterior não cobria.

## 3. Justiça entre opções

1. **Contratos, não perfis** (doc 12): comparação só dentro do mesmo contrato; quem não alcança
   não entra no grupo.
2. **Classe de serviço declarada** (S-fila × S-log): comparação entre classes exige nota funcional
   explícita na tabela e na figura.
3. Mesmos recursos por nó; mesmo cliente; mesmo caminho de código no harness.
4. Duas configurações por sistema (`default` e `tuned`), com justificativa e link por parâmetro, e
   **tempo de tuning igual** (máx. 2 dias por sistema).
5. Dimensionamento de memória por regra documentada, e **working set medido** (doc 03 §7.2).
6. Paridade de confirmação no consumidor (lote equivalente), contrato `C-proc`.
7. Revisão externa de adapters e configs antes da campanha confirmatória.

## 4. Planejamento e ordem

- Plano expandido das matrizes, com IDs determinísticos e **blocos**: cada rodada contém todas as
  configurações de uma repetição, embaralhadas com semente.
- Comparações são **pareadas dentro do bloco** (§7), o que absorve deriva temporal e diferenças
  entre ambientes paralelos.
- Dependências respeitadas (N04 depende do `X_sat` de N03; N17 depende do modelo ajustado).
- **Configurações seladas** (N17) não são executadas nem inspecionadas antes do registro das
  previsões; o orquestrador recusa executá-las sem o arquivo de previsões versionado.
- Sementes registradas: payload, chaves, chegada, ordem, instantes e pontos de falha.
- Retomada por `state.json`; execuções inválidas voltam à fila até 2 vezes, com motivo.

## 5. Validação automática

| Condição | Resultado |
|---|---|
| Contrato efetivo ≠ contrato alvo | `invalid` |
| CPU do gerador > 50%, throttling no cliente, ou schedule lag do gerador > 1 ms (p99) | `invalid` |
| *Steal time* > 2% | `invalid` |
| Lacunas de dados (hlog, Prometheus > 5 s) | `invalid` |
| Offset de relógio acima do limite | `L_e2e` inválida; resto válido |
| `W(t)` com inclinação positiva fora do critério | Válido, marcado `insustentável` (é resultado) |
| Censurados > 0,1% | Válido, marcado; quantis afetados reportados como limite |
| `indeterminado > 0` | Válido, marcado; **não** sustenta alegação de perda nem de ausência |
| `lost_confirmado > 0` sem falha injetada | `anomaly` — investigação obrigatória |
| Throttling de CPU no SUT | Válido, marcado `cpu-bound` |
| Restart não planejado do SUT | Válido, `outcome=sut-failure` |

## 6. Calibração (N02)

| Teste | Critério |
|---|---|
| Teto do gerador (`noop`) | ≥ 3× o maior MST observado, **por processo**, e reavaliado a cada campanha |
| Piso de latência (`inmemory`) | p99/p99.9 do caminho completo; resultados dentro de 2× do piso são "indistinguíveis do piso" |
| Precisão do ritmo | Schedule lag do gerador nas taxas do estudo |
| *Coordinated omission* | Trava artificial de 100 ms a cada 10 s deve aparecer na cauda |
| Verificador | Adapter `chaos` com perda/duplicação/inversão conhecidas → contagens exatas, por grupo |
| **Fault points** | Cada ponto dispara na janela pretendida em ≥ 99% das tentativas, comprovado por log |
| Erro de relógio | Offset medido antes/depois; banda usada na verificação de ordem |

## 7. Estatística

### 7.1 Unidade, blocos e pareamento
Unidade de análise é a execução. Percentis são calculados **por execução**; entre repetições
reporta-se mediana e dispersão. **Nunca** se calcula média de percentis. Comparações usam
**bootstrap pareado por bloco** (10 mil reamostragens), preservando a estrutura de rodadas.

### 7.2 Uma única regra de classificação
Para cada comparação, com margem `δ` declarada:

```
margem efetiva = max( δ_relativa · valor_da_referência , δ_absoluta )
equivalente  : IC95 da diferença (ou razão) inteiramente DENTRO da margem
diferente    : IC95 inteiramente FORA da margem  (reporta magnitude)
inconclusivo : IC95 cruza a borda
```

| Família de métrica | `δ_relativa` | `δ_absoluta` |
|---|---|---|
| MST e throughput | 5% | — |
| Latência p50 | 10% | 0,5 ms |
| Latência p99/p99.9 | 10% | 1 ms |
| CPU e memória por evento | 10% | — |
| Bytes por evento | 5% | — |
| Custo mensal (modelo) | 15% | — |

Esta regra é a **única** em todo o estudo: os documentos 09 e 13 referenciam esta seção em vez de
redefinir margens.

### 7.3 Referência pré-especificada
A referência de cada comparação é definida **antes** dos dados: `tx-only` para custo da
consistência; `outbox-poll` com `p = 100 ms` para estratégias de relay; `default` para tuning;
K3 para custo de contrato. **Escolher a melhor mediana observada como referência é proibido** —
isso introduz seleção e infla diferenças.

### 7.4 Precisão-alvo em vez de "5 repetições"

| Resultado primário | Meia-largura do IC alvo |
|---|---|
| MST | ≤ 5% |
| p99 de `L_e2e` e `L_req` | ≤ 10% |
| Efeitos das ablações | ≤ metade do efeito mínimo declarado (doc 00 §4) |

Procedimento: 3 repetições iniciais → estimar variância → calcular repetições necessárias →
escalonar até `R_max = 15`. Se a precisão não for atingida, o resultado permanece
**inconclusivo** e é publicado assim. Veredito editorial (doc 09) **não** resolve incerteza
estatística: ele só escolhe entre opções estatisticamente equivalentes ou inconclusivas, e precisa
repetir a incerteza ao lado.

### 7.5 Variabilidade monitorada na cauda
CV calculado sobre **p99 e MST** (não só p50): ≤ 5% ok; 5–10% registrar; > 10% investigar e
escalonar repetições.

### 7.6 Multiplicidade
As comparações primárias são exatamente as das previsões P1–P9 (doc 00 §4), pré-registradas no
congelamento. Correção de Holm dentro dessa família. Tudo o mais é **exploratório** e rotulado como
tal — inclusive todo o guia, que não entra na correção.

### 7.7 Dados censurados
Comparação só em quantis identificáveis nos dois lados (doc 03 §2.2). Caso contrário, reporta-se
limite inferior e a comparação fica `inconclusiva`.

### 7.8 Fatoriais
Modelo linear sobre `log(métrica)` com bloco como fator; efeitos com IC; lista de efeitos de
interesse pré-registrada; interações confundidas são declaradas e desconfundidas por *fold-over*
quando relevantes (doc 13 §4).

## 8. Ameaças à validade

### Internas
| Ameaça | Mitigação |
|---|---|
| Cliente como gargalo | Três evidências obrigatórias (doc 03 §4.2), não só `noop` |
| Janela de voo limitando o throughput | Sensibilidade 0,5×/2× no MST |
| Viés de configuração | `default` + `tuned`, tempo igual, revisão externa, asserção de contrato |
| Container não reproduz F2 | Reset de VM ou `dm-log-writes`; o que não for testado é declarado "não verificado" |
| Falhas aleatórias não atingem janelas críticas | Fault points determinísticos + relatório de cobertura |
| Observador falho confundido com perda | Categoria `indeterminado` + checkpoints com `fsync` |
| Kafka em modo combinado | Declarado; afeta comparações de operação, não o contrato |
| Page cache residual | Volumes recriados; cache limpo |

### Externas
| Ameaça | Mitigação |
|---|---|
| Dependência de hardware (mídia de disco muda tudo em K4) | Mídia como fator (N10) e transferência (N18) |
| Carga sintética | Carga representativa como padrão; controle só para ablação; sensibilidade dos parâmetros declarados |
| Um só cliente (Java) | Declarado; sem conclusão sobre outras linguagens |
| Domínio do modelo | Faixa de validade declarada; extrapolação proibida (doc 13 §5.5) |
| Versões | Fixadas por digest e datadas |

### De construto
`L_e2e` exclui processamento do consumidor; `L_req` é a latência da operação até a fronteira de
sucesso declarada; contratos, e não "durabilidade" genérica.

### De conclusão
Linguagem limitada pelo desenho (doc 13 §1); inconclusivo permanece inconclusivo; cobertura de
falhas sempre declarada junto de qualquer afirmação de integridade.

## 9. Reprodutibilidade

`manifest.json` por execução (commit, digests, configs renderizadas, cenário, sementes, flags,
impressão digital do host, **contrato asserido**, hash do arquivo de previsões seladas) ·
Gradle e `uv` com lock · resultados imutáveis com `SHA256SUMS` · histórias mínimas e modelo formal
versionados em `spec/` e executados na CI · artigo e guia regeneráveis por comando
(`studyctl article`, `studyctl guide`).

## 10. Registros

`docs/diario-de-implementacao.md` (decisões, problemas, tempo por adapter) ·
`results/<campanha>/CAMPAIGN.md` (anomalias, inválidas, desvios) ·
`analysis/predictions/<campanha>.json` (previsões seladas, com hash no commit).
