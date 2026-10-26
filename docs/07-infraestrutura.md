# 07 — Infraestrutura

## 1. Ambientes

| Ambiente | Uso | Entra no artigo/guia? |
|---|---|---|
| Local (WSL2) | Desenvolvimento, histórias mínimas, conformidade, cenários curtos em nó único | **Não** |
| Dedicado multi-host | Calibração, campanhas, validação | **Sim** |

## 2. Máquina local (verificado em 2026-09-15)

16 vCPUs · 11 GiB de RAM (≈ 2,8 GiB livres na checagem) · ~800 GB de disco · JDK 21 · Python 3.12 ·
**Docker indisponível na distro WSL** (ativar *WSL Integration* no Docker Desktop ou instalar o
Docker Engine na distro) · sem `uv`, sem Gradle (usar wrapper).

Limitações que valem como regra: T1 apenas, um sistema por vez, e **F2 não é testável aqui**
(disco virtualizado). Ajustar `.wslconfig` (`memory=10GB`, `processors=12`, `swap=0`).

## 3. Ambiente dedicado

### 3.1 Distribuição por host

| Host | Função | Perfil |
|---|---|---|
| `host-a/b/c` | Nós do broker (T3) | 4–8 vCPU, 16 GiB, **duas mídias**: NVMe local e volume de rede com IOPS provisionadas |
| `host-db` | PostgreSQL primário + standby síncrono | 4–8 vCPU, 16 GiB, NVMe |
| `host-load1` | Produtores / operação de negócio | 8 vCPU |
| `host-load2` | Consumidores, relays e CDC | 8 vCPU |
| `host-ctl` | `studyctl`, Prometheus, Grafana | 2–4 vCPU |

A **colocação do relay** (junto ao banco × junto ao broker) é declarada no manifesto; o padrão é
`host-load2`, e a variação entra como fator no cenário de transferência (N18).

### 3.2 Rede entre hosts

Redes bridge do Compose são **locais ao host** e não formam cluster entre máquinas. O desenho evita
camadas extras de rede, que distorceriam a medição:

- Um Compose **por host** (`docker --context host-a compose -f ...`), com o orquestrador usando
  **contextos Docker remotos** sobre SSH.
- Cada serviço publica portas na interface do host; `infra/hosts.env` mapeia `host → IP/DNS`.
- Endereçamento anunciado explícito: `advertised.listeners` (Kafka/Redpanda), `cluster_name` e
  nomes de nó (RabbitMQ), `routes` (NATS), `advertisedListeners` (Pulsar) — todos derivados de
  `hosts.env`, nunca de nomes internos do Compose.
- Duas interfaces lógicas por nó: tráfego de clientes e tráfego de replicação, para medir bytes
  separadamente (doc 03 §6.1) e aplicar `netem` por enlace.
- Overlay do Swarm/Kubernetes é **rejeitado**: acrescenta encapsulamento ao caminho medido.

### 3.3 Coleta remota

Prometheus em `host-ctl` raspa exporters e cAdvisor de todos os hosts por portas publicadas.
Snapshots de cgroup e tamanhos de volume são lidos via contexto Docker remoto. Após cada execução,
as séries da janela são exportadas para Parquet na pasta da execução — o Prometheus não é fonte da
verdade.

### 3.4 Linha de base do host

Antes de cada campanha: `fio` (nas duas mídias), `iperf3` e `ping` entre todos os pares, `sysbench`
de CPU, e verificação de *steal time*. Os resultados vão para o manifesto da campanha.

## 4. Falha F2 — perda real de page cache

Matar container **não** testa K2. Três mecanismos, em ordem de preferência:

| Mecanismo | Como | Quando usar |
|---|---|---|
| **Reset de VM** | API do hipervisor/nuvem (`stop --force` / `reset`) | Padrão no ambiente dedicado; corta energia do ponto de vista do SO |
| `sysrq-b` | `echo b > /proc/sysrq-trigger` dentro da VM | Quando não há API de reset; reinicia sem `sync` |
| **`dm-log-writes` / `dm-flakey`** | Volume de dados do broker sobre um alvo device-mapper; após a falha, reproduz **apenas** as escritas sincronizadas | Quando é preciso controlar exatamente o que sobrevive, e para reproduzir o mesmo corte várias vezes |

Requisitos: o volume de dados de cada nó do SUT fica sobre o alvo device-mapper durante os cenários
F2; o procedimento de reprodução é versionado em `infra/faults/f2/`. O que não for testado assim é
declarado **"K2 não verificado"** (doc 12 §10).

## 5. Demais injeções

| Ação | Mecanismo |
|---|---|
| F1 | `docker kill -s KILL` |
| F3/F4 | Parada definitiva de nó(s), sem retorno |
| F5 | `iptables` em sidecar que compartilha o namespace de rede do alvo |
| Latência/perda | `tc netem` no mesmo sidecar, por enlace (cliente ou replicação) |
| F6/F7/F8/F9 | **Fault points** no runner (doc 06 §9.1), acionados pela API de controle |
| Dependência fora | Parada de PostgreSQL, broker ou conector CDC |

Instantes planejados e reais vão para `faults.json`; pontos atingidos, para `coverage.json`.

## 6. Contratos na infraestrutura

- `infra/configs/<sistema>/<contrato>/` contém a configuração que **pretende** atingir K1…K4.
- A asserção lê a configuração **efetiva** do sistema em execução (doc 06 §5) — configuração de
  tópico, tipo de fila, réplicas, `min.insync.replicas`, `sync_interval`, `synchronous_commit`,
  `synchronous_standby_names` — e reprova a execução em caso de divergência.
- PostgreSQL com standby síncrono faz parte do núcleo sempre que a cadeia alvo for K3/K4.

## 7. Working set e memória

Procedimento da ablação A8 (doc 13 §3), usado também pelo modelo de custo (doc 03 §7.2):
fixar a carga e a retenção, reduzir a memória do container em degraus (100%, 75%, 50%, 25% da
memória disponível), medir o MST em cada degrau e tomar como **working set** o menor valor que
ainda sustenta o MST dentro da margem de equivalência.

## 8. Observabilidade

cAdvisor (1 s) · node-exporter (1 s, inclui *steal*) · exporters por sistema (5 s) ·
`postgres_exporter` + consultas próprias (pendentes na outbox, WAL retido, dead tuples, esperas por
lock) · `/metrics` do runner (1 s) · Grafana com dashboards versionados. A observabilidade roda em
host separado; uma vez por campanha mede-se um ponto com e sem exporters para quantificar o overhead.

## 9. Checklist local (F0)

1. Ativar Docker na distro WSL; validar `docker version` e `docker compose version`.
2. Ajustar `.wslconfig` e reiniciar o WSL.
3. Instalar `uv`; gerar o Gradle wrapper.
4. sysctls comuns: `vm.max_map_count=262144`, limites de descritores, `net.core.somaxconn=4096`.
5. Subir a observabilidade e confirmar coleta.
6. Registrar que F2, multi-host e mídia de rede **não** existem localmente.
