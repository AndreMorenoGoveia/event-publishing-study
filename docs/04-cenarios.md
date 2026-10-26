# 04 — Cenários

Duas famílias: **N** (núcleo científico, responde à pergunta central) e **G** (guia operacional,
caracterização). As histórias mínimas `H-A`…`H-J` (doc 12 §11) **não** são cenários de campanha:
rodam na CI e são pré-requisito de qualquer alegação de contrato.

## 1. Cargas

| Perfil | Definição | Papel |
|---|---|---|
| **Controle** | Payload fixo de 1 KiB incompressível, chegada constante, chaves uniformes, 1 escritor por agregado | Isola mecanismos; não sustenta recomendação sozinha |
| **Representativa** (P0) | Mix de 3 tipos de evento; tamanho lognormal (mediana 1 KiB, p99 ≈ 20 KiB); chaves Zipf `s ≈ 1,0`; chegada modulada com rajadas; **2–8 escritores concorrentes por agregado** | Base de toda recomendação |

A carga representativa é **declarada e versionada** (`scenarios/_workloads.yaml`) com a origem de
cada parâmetro: traço público, sistema de referência ou premissa. Onde for premissa, entra na
análise de sensibilidade (doc 03 §7.4). Todo resultado do núcleo é executado nas **duas** cargas
nos pontos decisivos (N15), e a recomendação só vale onde as duas concordam — divergência é
resultado, e é reportada.

## 2. Parâmetros padrão

| Parâmetro | Padrão |
|---|---|
| Cadeia/contrato | CH-B (PostgreSQL K3 + broker K3), doc 01 §5.1 |
| Carga | Representativa (controle nos cenários de ablação) |
| Partições/filas | 12 |
| Relay | `partition-lease`, `k = 1`, `b = 100`, `p = 100 ms`, `cleanup = delete` |
| Consumidores | 1 grupo, 4 instâncias, contrato `C-proc` |
| Aquecimento | 120 s |
| Medição | ≥ 3 ciclos de manutenção (doc 03 §4.3); 300 s nos cenários curtos, 15–30 min nas confirmações de MST |
| Drenagem | Até prova de quiescência (doc 03 §8.1) ou 60 s sem progresso |
| Repetições | 3 iniciais, escalonadas por **precisão-alvo** até 15 (doc 05 §7) |
| SLO | p99 de `L_e2e` em 10 ms, 100 ms e 1 s |

## 3. Cenários do núcleo

| ID | Nome | Responde | Desenho |
|---|---|---|---|
| N01 | Sanidade e conformidade | Gate | Todos os adapters e estratégias; verificador com zero perda/duplicata sem falha |
| N02 | Calibração | Método | `noop`, `inmemory`, `chaos`, precisão dos fault points, erro de relógio |
| N03 | Capacidade bruta (`X_sat`) | QP1 | Malha fechada por estratégia e contrato |
| N04 | **Fronteira λ × estratégia** | QP2 | Escada de taxas × {polling `p∈{10,100,1000}`, NOTIFY, CDC Connect, CDC Server} × {Kafka, RabbitMQ} |
| N05 | Ablação A1 — sono do polling | QP3 (M1) | `p` × regime {subcrítico, saturado} |
| N06 | Ablação A2 — lote e capacidade | QP3 (M2, M3) | `b` com λ e `k` fixos |
| N07 | Ablação A4 — CDC runtime × destino | QP3 (M11) | Fatorial 2×2 completo |
| N08 | Ablação A5 — contenção por agregado | QP3 (M4) | Escritores por agregado {1,2,4,8} |
| N09 | Ablação A6 — manutenção do banco | QP3 (M6) | autovacuum × checkpoint × outbox particionada; execuções de 30 min |
| N10 | Ablação A3 — contrato K3 × K4 × mídia | QP1, P4 | Mesmo sistema, dois contratos, NVMe e disco de rede |
| N11 | Ablação A8 — page cache | QP3 (M10), P9 | Memória do broker em {100%, 50%, 25%} do working set |
| N12 | Fatorial fracionado do outbox | QP2, QP3 | `2^(4−1)` res. IV + fold-over seletivo, em 2 níveis de carga |
| N13 | Custo da consistência | QP1, P7 | `tx-only`, `dw-after`, `dw-before`, `outbox-poll`, `outbox-notify`, `outbox-cdc`, `pg-queue` |
| N14 | **Falhas de contrato** | QP6 | F1–F9 com fault points determinísticos; cobertura tabelada |
| N15 | Carga representativa × controle | QP5 | Subconjunto decisivo nas duas cargas |
| N16 | Sustentação longa | QP1, QP3 | 4 h em 50% do MST; deriva, manutenção, bloat |
| N17 | **Validação preditiva** | QP4 | Configurações **seladas**; previsões registradas antes (doc 13 §5.4) |
| N18 | Transferência | QP5 | Outro armazenamento e outro ambiente |

### 3.1 N04 — fronteira (cenário principal)

- **Variáveis:** estratégia (6 variantes) × broker (2) × taxa (6 degraus, de 10% de `X_sat` até
  1,1×) × cadeia (CH-B; CH-A e CH-C em subconjunto).
- **Saída:** curvas de p99 de `L_e2e` e MST por estratégia, e o **cruzamento `λ*`** com IC.
- **Validade:** prova de não-gargalo do cliente (doc 03 §4.2) obrigatória em cada ponto de MST.

### 3.2 N14 — falhas de contrato

| Classe | Injeção | Alvo da verificação |
|---|---|---|
| F1 | `kill -9` do processo | Sobrevivência K1 |
| **F2** | Reset de VM / `dm-log-writes` descartando escritas não sincronizadas | **K2 × K1** (é a falha que separa page cache de `fsync`) |
| F3 | Remoção permanente de nó | K3 |
| F4 | Parada simultânea de subconjunto | K4 |
| F5 | Partição de rede | K3, split-brain |
| **F6** | **Fault points** na aplicação: após commit e antes de publicar; após publicar e antes de commitar | `dw-after` (H-C), `dw-before` (H-D) |
| **F7** | Fault points no relay: após ack do broker e antes de marcar; pausa longa com expiração de lease e retomada do dono antigo | H-B, **H-A** (fencing), H-J (`cleanup=early`) |
| F8 | `kill` do consumidor entre processar e confirmar; `C-auto` como controle | H-G, H-H |
| F9 | `kill` do consumidor antes do checkpoint do verificador | H-I: classificação `indeterminado` |

Crashes aleatórios **não** substituem fault points: a janela crítica entre o ack do broker e a
marcação da outbox pode nunca ser atingida por sorteio. Ela é atingida por construção.
Cada execução reporta **cobertura**: quais janelas foram atingidas e quantas vezes.

## 4. Cenários do guia

Caracterização, sem alegação de novidade. Mantêm o método (contratos, população de latência,
verificação), mas não exigem ablação nem modelo.

| ID | Nome | Capítulo do guia | Prioridade |
|---|---|---|---|
| G01 | Sanidade e footprint ocioso | C4, C13 | P1 |
| G02 | Saturação | C1 | P1 |
| G03 | Latência × carga | C1, C3 | P1 |
| G04 | Tamanho de payload | C12 | P1 |
| G05 | Concorrência de produtores | C1 | P1 |
| G06 | Batching do cliente | C3 | P1 |
| G07 | Particionamento e ordem | C10 | P1 |
| G08 | Fan-out | C11 | P1 |
| G09 | Muitos destinos | C11 | P2 |
| G10 | Contratos por sistema (K1→K4) | C1, C6 | P1 |
| G11 | Rajada | C9 | P1 |
| G12 | Backlog / consumidor parado | C9 | P1 |
| G13 | Soak | C1, C4 | P2 |
| G14 | Falha de nó | C8 | P1 |
| G15 | Degradação de rede | C5 | P1 |
| G16 | Crash de produtor/relay | C7 | P1 |
| G17 | Indisponibilidade de dependência | C7, C8 | P1 |
| G18 | Retenção e **replay** | C6, C11 | P1 |
| G19 | Idempotência no consumidor | C7 | P2 |
| G20 | Hot keys | C1, C10 | P2 |
| G21 | Cold start e reconexão | C13 | P2 |
| G22 | TLS e overhead de framework | C4 | P2 |

G18 trata replay como **requisito funcional** (doc 12 §6): tempo e custo para um novo grupo
reprocessar N eventos, e o que acontece em sistemas S-fila (não suportam).

## 5. Orçamento de tempo

Tempo por execução curta ≈ 10–12 min (subida, aquecimento, medição, drenagem com quiescência,
coleta, derrubada). Estimativas de ordem de grandeza, com 3 repetições iniciais:

| Bloco | Horas |
|---|---|
| N01–N03 (gate, calibração, capacidade) | ~15 |
| **N04 (fronteira)** | ~40 |
| N05–N08, N10–N12 (ablações e fatorial) | ~50 |
| N09, N16 (execuções longas) | ~40 |
| N13 (custo da consistência) | ~18 |
| N14 (falhas com fault points) | ~20 |
| N15 (carga representativa) | ~26 |
| N17 (validação preditiva) | ~24 |
| N18 (transferência) | ~14 |
| **Subtotal núcleo** | **~247** |
| Guia P1 (subconjunto dos G) | ~90 |
| **Total** | **~337 h (~14 dias de máquina)** |

Controles de orçamento: campanha exploratória (1 repetição, janelas curtas) antes da
confirmatória; **repetições guiadas por precisão-alvo** (só escalona onde o IC estiver largo);
ambientes paralelos; e o guia só roda depois do núcleo fechado.
