# 13 — Explicação, desenho causal e validação preditiva

> Descrever diferenças não identifica causas. Este documento define o que precisa existir para o
> estudo passar de "esta configuração apresentou este resultado" para "este mecanismo explica o
> resultado, e o modelo prevê onde a recomendação muda".

## 1. Escopo permitido das conclusões

A frase que o estudo pode escrever é determinada pelo desenho que a sustenta.

| Evidência disponível | Frase permitida | Frase proibida |
|---|---|---|
| Comparação sem ablação | "Nesta configuração, X apresentou Y (IC ...)" | "X é mais rápido", "X escala melhor" |
| Ablação do mecanismo M | "M explica a diferença: ao neutralizar M, a diferença cai de A para B" | Atribuir a causa a arquitetura/linguagem sem ablação |
| Modelo validado fora da amostra | "A troca ocorre em λ* = ... ± ..., previsto com erro E" | Extrapolar fora do domínio de validade (§5.5) |
| Mesma API, implementações diferentes (Kafka × Redpanda) | "As duas implementações da mesma API diferiram em Z nestas condições" | "Isto isola o efeito de implementação" |

## 2. Mecanismos candidatos

| ID | Mecanismo | Como se manifesta | Como isolar |
|---|---|---|---|
| M1 | Espera do polling (relay dorme quando a outbox está vazia) | Piso de latência ≈ `p/2` no regime subcrítico | N05: `p` × regime de carga |
| M2 | Formação de lote | Latência sobe e custo por evento cai com `b` | N06: `b` com λ e capacidade fixas |
| M3 | Capacidade do relay (consulta + publicação + marcação) | Saturação; fila cresce | N06, N12 |
| M4 | Contenção por agregado no banco | Serialização de escritores do mesmo agregado | N08: escritores por agregado |
| M5 | Contenção do `NOTIFY` no commit | Queda do throughput da operação com concorrência | N05/N13 com `NOTIFY` on/off × concorrência |
| M6 | Manutenção do banco (autovacuum, checkpoint, bloat da outbox) | Degradação lenta; picos periódicos | N09, N16 |
| M7 | Decodificação lógica do WAL (tarefa única) e flush de offsets no CDC | Teto de capacidade do CDC; duplicatas após restart | N07, N04 |
| M8 | Durabilidade do broker (`fsync` × page cache) | Custo de K3 → K4 | N10 (mesmo sistema, dois contratos) |
| M9 | Replicação (quórum, amplificação de bytes) | Custo de rede e latência de cauda | N10, G15 |
| M10 | Dependência de page cache | Queda de desempenho quando a memória é limitada | N11 |
| M11 | Runtime do CDC × destino | Confusão entre "Connect é lento" e "o destino é lento" | N07 (fatorial 2×2) |
| M12 | Lease e fencing | Inversões e duplicatas sob troca de dono | N14 (história H-A) |

## 3. Ablações

| ID | Ablação | Fatores e níveis | O que passa a ser afirmável |
|---|---|---|---|
| A1 | Sono do polling | `p ∈ {10, 100, 1000} ms` × regime `{subcrítico, saturado}` | Que o efeito de `p` **depende do regime**, e não é aditivo |
| A2 | Lote | `b ∈ {1, 100, 1000}` com λ fixo e `k` fixo | Separar efeito de lote de efeito de capacidade |
| A3 | Contrato de durabilidade **dentro do mesmo sistema** | Kafka K3 × K4; JetStream K3 × K4; Redpanda com/sem write caching | Isola `fsync` sem trocar de implementação — é a ablação limpa de M8 |
| A4 | Runtime × destino do CDC | `{Kafka Connect, Debezium Server}` × `{Kafka, RabbitMQ}` (2×2 completo) | Separar runtime de destino; **P0**, não P2 |
| A5 | Contenção | Escritores por agregado `{1, 2, 4, 8}` | Efeito de M4 e se ele altera a ordenação das estratégias |
| A6 | Manutenção do banco | `autovacuum {on, off}` × `checkpoint {curto, longo}` × outbox `{particionada, simples}` | Atribuir picos e deriva a M6 |
| A7 | `NOTIFY` | `{sem, com}` × concorrência `{8, 32, 64}` | Custo de M5 e sua dependência da concorrência |
| A8 | Page cache | Memória do broker em `{100%, 50%, 25%}` do working set medido | Dependência de M10 e insumo do modelo de custo (doc 03 §7) |

## 4. Desenhos fatoriais

- **N12 — parâmetros do outbox:** fatorial fracionado `2^(4−1)` de resolução IV sobre
  `{pollInterval, batchSize, relays, cleanup}`, replicado em **dois níveis de carga**. Efeitos
  principais ficam livres de confusão com interações de dois fatores; as interações de dois
  fatores ficam confundidas entre si e são desconfundidas por um *fold-over* apenas onde o efeito
  estimado for relevante.
- **N07 — CDC:** fatorial completo `2×2` (runtime × destino).
- **N05, N08, N10, N11:** fatoriais completos pequenos (2–3 níveis × 2 fatores).
- **Análise:** modelo linear sobre `log(métrica)` com **bloco** (rodada/ambiente) como fator;
  efeitos relatados como tamanho de efeito com IC. A lista de efeitos de interesse é
  pré-registrada no congelamento do protocolo; o resto é exploratório e rotulado como tal.
- OFAT (um fator por vez) deixa de ser o desenho principal: ele não estima interações, e os
  fatores do outbox interagem por construção (lote × taxa × capacidade).

## 5. Modelo mecanístico

### 5.1 Variáveis

`λ` taxa de chegada · `p` intervalo de polling · `b` lote máximo · `k` relays · `c` escritores por
agregado · `T_q(b)` consulta · `T_pub(b)` publicação + confirmação · `T_mark(b)` marcação ·
`C_relay` capacidade · `ρ = λ / C_relay` · `d` latência do broker · `μ_wal` capacidade de
decodificação do CDC.

### 5.2 Forma funcional

**Polling** — capacidade e regimes:

```
C_relay(b, k) = k · b / ( T_q(b) + T_pub(b) + T_mark(b) )

Regime subcrítico (a outbox esvazia; o relay dorme):
  E[espera_pickup] ≈ p/2                       , enquanto λ·p ≤ b
Regime saturado (a outbox nunca esvazia; o relay não dorme):
  E[espera_pickup] ≈ (b−1)/(2λ) + ρ/(1−ρ) · E[S]/2      (fila com atendimento em lote)
```

**CDC** — latência aproximadamente independente de `p` e capacidade limitada por `μ_wal`:

```
L_cdc ≈ T_wal + T_decode + T_fila_interna(λ, μ_wal) + T_pub
```

**NOTIFY** — espera de pickup reduzida ao custo de contenção no commit:

```
E[espera_pickup] ≈ T_notify + T_q            ;  MST_op(c) ≈ MST_op^poll(c) · (1 − g(c))
```

As constantes (`T_q`, `T_pub`, `T_mark`, `μ_wal`, `g`) são **medidas**, não arbitradas: cada uma
tem um microexperimento próprio na fase de ajuste.

### 5.3 O que o modelo precisa prever (declarado antes da validação)

| Previsão | Critério de sucesso |
|---|---|
| p99 de `L_e2e` nas configurações seladas | MAPE ≤ 25% |
| Taxa de cruzamento `λ*` entre estratégias | Erro ≤ 20% do `λ*` medido |
| Sinal (direção) dos efeitos das ablações | 100% correto |
| Cobertura do intervalo de predição de 80% | ≥ 70% dos pontos |

Falhar é um resultado publicável: significa que os mecanismos listados não bastam para explicar as
fronteiras, e o estudo reporta isso.

### 5.4 Protocolo de ajuste e validação

1. O espaço de configurações do núcleo é particionado **por configuração** (não por execução) em
   `ajuste` (60%) e `validação` (40%), por amostragem estratificada com semente registrada.
2. As configurações de validação ficam **seladas** no congelamento do protocolo: não são
   executadas, olhadas nem usadas para ajustar nada.
3. O modelo é ajustado só com `ajuste`; as previsões para `validação` são **registradas antes** de
   as execuções acontecerem (arquivo `analysis/predictions/<campanha>.json`, com hash no commit).
4. Só então a campanha de validação roda, e o erro é reportado como em §5.3.

### 5.5 Domínio de validade

O modelo declara explicitamente: faixa de `λ`, tamanhos de payload, contratos, número de relays,
concorrência, hardware e versões em que foi ajustado. Extrapolação fora dessa faixa é proibida no
texto do artigo.

## 6. Validação de transferência

Subconjunto decisivo (as configurações que determinam as fronteiras) repetido em:

| Variação | Pergunta |
|---|---|
| Outro armazenamento (NVMe local × disco de rede) | As fronteiras se deslocam? O sinal dos efeitos muda? |
| Outro ambiente (bare metal × nuvem) | A ordenação das estratégias se mantém? |
| Carga representativa (skew, rajada, contenção, mix de tamanhos) × carga de controle | A recomendação derivada da carga de controle vale na representativa? |

Critério: a **ordenação** das estratégias em cada região da fronteira se mantém. Onde não se
mantiver, o estudo reporta a condição que inverte — esse é um dos resultados mais úteis do
trabalho, não um problema.

## 7. Relação com o guia operacional

O guia (doc 09) usa os mesmos dados, mas não herda automaticamente as explicações: só capítulos
sustentados por ablação ou por modelo validado podem afirmar mecanismo. Os demais ficam em
linguagem descritiva, como definido em §1.
