# Monte Carlo da base estatística — resultados

Executado em 2026-10-26 numa GPU L4 (Colab), com `mc_stats.py` e as sementes registradas no
código. Os dados brutos estão em `out/s1.csv` … `out/s4.csv`. O conteúdo vale para qualquer
variante do estudo: não depende de broker, estratégia nem pergunta central, só do fato de que se
mede quantil de latência por execução e se comparam execuções repetidas.

Convenção: `cv` é o desvio-padrão de `log(métrica)` **por braço**, entre execuções.
Aproximadamente, é o coeficiente de variação. A diferença pareada tem desvio `√2·cv` quando o
bloco não absorve nada.

## S1 — Quantos eventos por execução para estimar p99, p99.9 e p99.99

Erro relativo (RRMSE) do quantil empírico. Latência lognormal com σ = 1 (caudas moderadas) e
autocorrelação AR(1) de coeficiente ρ entre eventos consecutivos.

| quantil | ρ | 10³ | 10⁴ | 10⁵ | 10⁶ | 10⁷ |
|---|---|---|---|---|---|---|
| p99 | 0 | 11% | 3,7% | 1,2% | 0,4% | 0,1% |
| p99 | 0,99 | 58% | 31% | 9,3% | 2,9% | 0,9% |
| p99.9 | 0 | 26% | 9,1% | 2,9% | 0,9% | 0,3% |
| p99.9 | 0,99 | 63% | 48% | 19% | 5,8% | 1,8% |
| p99.99 | 0 | 44% | 23% | 7,9% | 2,5% | 0,8% |
| p99.99 | 0,999 | 90% | 76% | 51% | 33% | 14% |

O que isso mostra:

- **Regra prática com eventos independentes.** Com `n ≥ 10/(1−q)` o erro fica em torno de 10%;
  com `n ≥ 100/(1−q)`, em torno de 3%. Para p99, isso significa 10³ e 10⁴ eventos; para p99.9,
  10⁴ e 10⁵.
- **A autocorrelação é o que manda.** Latências de eventos consecutivos são correlacionadas em
  qualquer sistema com fila, porque um evento lento atrasa os vizinhos. O tamanho que importa é
  o efetivo, `n_ef ≈ n·(1−ρ)/(1+ρ)`. Com ρ = 0,99, `n_ef ≈ n/200`.
- **O erro não é só ruído: o quantil sai subestimado.** p99.99 com ρ = 0,999 e 10⁶ eventos sai,
  em média, **50% abaixo** do valor real. Uma janela curta faz a cauda parecer melhor do que é.
- **O polling é benigno.** Quando a espera uniforme em (0, p) domina a latência, a cauda é
  limitada e o erro é 3 a 10 vezes menor.

**Proposta para o doc 03 §2 e o doc 05 §5.** Medir a autocorrelação da série de latências em
cada execução (tempo de autocorrelação integrado, ou o método de *batch means*) e registrar
`n_ef`. Só publicar um quantil q se `n_ef ≥ 10/(1−q)`; abaixo disso, ele é marcado como
**não identificável**, como já acontece com os dados censurados.

## S2 — Qual intervalo de confiança usar entre repetições

Cobertura real do IC, no pior caso entre as distribuições normal, t₃ e contaminada (5% de
execuções outliers):

| n | bootstrap 95% | t 95% | bootstrap 90% | t 90% |
|---|---|---|---|---|
| 3 | **0,745** | 0,948 | 0,605 | 0,894 |
| 5 | 0,833 | 0,949 | 0,779 | 0,896 |
| 10 | 0,894 | 0,949 | 0,833 | 0,895 |
| 15 | 0,908 | 0,948 | 0,849 | 0,896 |
| 30 | 0,925 | 0,948 | 0,869 | 0,896 |

- **O bootstrap pareado do doc 05 §7.1 não serve para n ≤ 15.** Ele subcobre, e na regra de
  equivalência isso vira **falsa equivalência de 12 a 19%** com n = 3, quando o nominal é 2,5%.
- **O IC t sobre a razão em escala log é nominal em todos os cenários**, inclusive com caudas
  pesadas e outliers.
- **Equivalência.** "IC95 dentro da margem" dá α = 2,5% (verificado: 0,023 a 0,027). "IC90 dentro
  da margem" é o TOST convencional, com α = 5% (0,047 a 0,053). Os dois estão corretos; o
  protocolo precisa dizer qual usa.

Meia-largura relativa mediana do IC t 95%:

| n | cv 2% | cv 5% | cv 10% | cv 20% |
|---|---|---|---|---|
| 3 | 6,0% | 16% | 34% | 79% |
| 5 | 3,3% | 8,4% | 18% | 38% |
| 10 | 2,0% | 5,0% | 10% | 22% |
| 15 | 1,5% | 3,9% | 8,0% | 17% |
| 30 | 1,1% | 2,6% | 5,4% | 11% |

- **O que os alvos do doc 05 §7.4 exigem.** O alvo do MST (meia-largura ≤ 5%) exige cv ≤ 5% com
  n = 10, ou cv ≤ 2% com n = 3 a 4. O alvo do p99 (≤ 10%) exige cv ≤ 10% com n = 10.
- **Com cv = 20%, nenhum n ≤ 30 atinge os alvos.** É preciso reduzir a variância: bloqueio
  melhor, janelas mais longas (S1 e S4) ou ambiente mais estável. Repetir mais não resolve.
- **Declarar "equivalente" com margem δ = 10% exige cv ≤ 5%.** Com cv = 5%, a probabilidade de
  concluir equivalência quando ela é verdadeira chega a 95% com n = 10. Com cv = 10%, fica em 46%
  mesmo com n = 15. Na prática, **"inconclusivo" vai ser o resultado mais comum** quando o p99 tem
  cv ≥ 10%. Isso precisa ser esperado e dito no artigo.

**Proposta para o doc 05 §7.1.** Trocar o bootstrap pareado por bloco pelo **IC t pareado sobre
`log(razão)`**. O bootstrap fica só para estatísticas dentro da execução, onde n é grande.

## S3 — A regra "3 iniciais → escalonar até R_max = 15" infla o erro?

Sim. É o problema da parada opcional: decidir quantas repetições fazer olhando os mesmos dados
que vão para o IC.

| procedimento | cobertura (pior caso) | falsa equivalência na borda (pior caso, nominal 2,5%) |
|---|---|---|
| n fixo = 15 | 0,950 | 2,5% |
| dois estágios (doc 05 §7.4) | 0,915 | 4,2% |
| sequencial (para quando o IC fica estreito) | 0,905 | 4,7% |

**Proposta para o doc 05 §7.4.** Usar a campanha exploratória da F8 para estimar o `cv` de cada
família de métrica e, com a tabela do S2, **fixar n por família no `protocol-v1`**. A campanha
confirmatória então não adapta nada. Isso já combina com a lógica de congelamento do estudo. Se
for preciso adaptar, a alternativa exata é o procedimento de dois estágios de Stein: o IC final
usa o desvio do primeiro estágio com n₀ − 1 graus de liberdade.

## S4 — Quantos ciclos de manutenção a janela de medição precisa cobrir

Modelo: pausas periódicas (checkpoint, autovacuum, GC) que ocupam uma fração `f` do ciclo; quem
chega durante a pausa espera o fim dela. A tabela mostra o CV do p99 por execução, em função do
número de ciclos cobertos pela janela:

| ciclos | f = 0,5% | f = 1% | f = 2% | f = 5% |
|---|---|---|---|---|
| 1,5 | 2,6% | 11% | 37% | 9,3% |
| 3,5 | 1,4% | 4,9% | 13% | 3,5% |
| 5,5 | 1,0% | 3,2% | 8,2% | 2,2% |
| 10,5 | 0,7% | 1,8% | 4,3% | 1,2% |
| números inteiros (3, 5, 10) | ≤ 1,5% | ≤ 1,5% | ≤ 1% | ≤ 0,3% |

- **O pior caso é quando a fração de tempo em pausa é parecida com a massa da cauda.** Para p99,
  isso acontece com `f ≈ 1 a 2 × (1−q)`, ou seja, `f` entre 1% e 2%. Nessa faixa, o p99 da
  execução depende de quantas pausas caíram dentro da janela.
- **Janelas com número inteiro de ciclos parecem estáveis, mas é artefato.** Manutenção real não
  é periódica, então não dá para alinhar a janela ao ciclo.
- **"≥ 3 ciclos" (doc 03 §4.3) não basta para o p99.** No pior caso, o CV cai como `≈ 0,45/k`,
  em que k é o número de ciclos. CV ≤ 10% exige k ≥ 5; CV ≤ 5% exige k ≥ 9; CV ≤ 2% exige
  k ≥ 23.
- **O p99.9 é menos sensível nesta faixa de `f`:** CV ≤ 2,5% a partir de 3,5 ciclos.

**Proposta para o doc 03 §4.3.** Medir `f` na campanha exploratória. Quando `f` estiver entre
(1−q) e 3·(1−q), exigir pelo menos 10 ciclos ou reportar o quantil sobre a janela agregada das
repetições. Em ambos os casos, o `cv` resultante alimenta o S2.

## Resumo das mudanças sugeridas

| Onde | Hoje | Proposta |
|---|---|---|
| doc 05 §7.1 | bootstrap pareado por bloco | IC t pareado sobre `log(razão)` |
| doc 05 §7.2 | "IC95 dentro da margem" | manter, declarando α = 2,5%, ou adotar IC90 (TOST, α = 5%) |
| doc 05 §7.4 | 3 iniciais → escalonar até 15 | n fixo por família, decidido na F8 e congelado; ou Stein |
| doc 03 §2 | quantil por execução sem critério de tamanho | `n_ef` registrado; quantil q só com `n_ef ≥ 10/(1−q)` |
| doc 03 §4.3 | ≥ 3 ciclos de manutenção | ≥ 10 ciclos quando `f` estiver perto de (1−q) |
| doc 04 §5 | orçamento com 3 repetições iniciais | refazer com n por família; para p99 com cv ≥ 10%, n ≈ 10 a 15 |

## Limitações

- O modelo de autocorrelação é um AR(1) gaussiano em escala log. Rajadas reais podem ter memória
  longa, o que tornaria `n_ef` ainda menor.
- As pausas do S4 são estritamente periódicas e de duração fixa. Com jitter no período ou na
  duração, o CV tende a ficar entre o caso inteiro e o fracionário.
- A quantização do HdrHistogram (3 dígitos significativos, erro ≤ 0,1%) foi ignorada: é
  desprezível perto dos erros acima.
- Réplicas de Monte Carlo: S1 e S4 com 2.000 por célula; S2 com 20.000 experimentos e
  B = 4.000; S3 com 200.000. O erro de Monte Carlo na cobertura é ≈ ±0,3 p.p. no S2 e ≈ ±0,1 p.p.
  no S3.
