# Monitoramento dos Preços dos Combustíveis em Salvador — 2025

Pipeline de dados para coleta, transformação e análise dos preços de combustíveis automotivos em Salvador/BA, com base nos dados públicos da ANP (Agência Nacional do Petróleo, Gás Natural e Biocombustíveis).

---

## Visão Geral

O projeto implementa uma arquitetura de dados completa seguindo o padrão **Medallion Architecture** (Bronze → Silver → Gold), com orquestração via Apache Airflow, armazenamento em AWS S3 e Data Warehouse em PostgreSQL. A API FastAPI expõe os dados tratados para consulta, e um dashboard Streamlit está em desenvolvimento para visualização interativa.

---

## Arquitetura

```
CSV's (ANP)
    │
    ▼
[Bronze] → Dados brutos em Parquet + upload S3
    │
    ▼
[Silver] → Dados limpos, sem duplicatas, colunas padronizadas + upload S3
    │
    ▼
[Gold]   → Modelo estrela (dimensões + fato) em Parquet + upload S3
    │
    ▼
[PostgreSQL] → Data Warehouse com tabelas dimensionais e fato
    │
    ▼
[FastAPI] → Endpoints de consulta
    │
    ▼
[Streamlit] → Dashboard interativo (em desenvolvimento)
```

---

## Stack Tecnológica

- **Python** — linguagem principal (Pandas, SQLAlchemy, python-dotenv)
- **Apache Airflow** — orquestração do pipeline ETL
- **Docker / Docker Compose** — containerização de todos os serviços
- **AWS S3** — armazenamento do data lake (camadas Bronze, Silver e Gold)
- **PostgreSQL** — Data Warehouse relacional
- **FastAPI** — API REST para exposição dos dados
- **Streamlit** — dashboard interativo (em desenvolvimento)

---

## Camadas do Data Lake

### Bronze
Armazena os dados brutos extraídos diretamente dos arquivos CSV da ANP, convertidos para o formato Parquet. Nenhuma transformação é aplicada nesta camada — apenas a ingestão e persistência dos dados originais, tanto localmente quanto no S3.

Arquivos gerados:
- `data_lake/bronze/analise_semestre_1.parquet`
- `data_lake/bronze/analise_semestre_2.parquet`

### Silver
Camada de tratamento e qualidade dos dados. As transformações incluem:
- Concatenação dos dois semestres em um único dataset anual
- Renomeação e padronização de colunas
- Remoção de duplicatas
- Validação e conversão dos campos de valor de venda
- Validação e padronização de datas

Arquivo gerado:
- `data_lake/silver/analise_geral_2025.parquet`

### Gold
Camada analítica com modelo dimensional no padrão **Star Schema**, composta por tabelas de dimensão e uma tabela fato:

- `dim_tempo` — datas, ano, mês, dia e dia da semana
- `dim_revenda` — postos, CNPJ e bandeira
- `dim_produto` — tipo de combustível e unidade de medida
- `dim_localizacao` — região, estado, município e bairro
- `fato_preco` — tabela fato com o valor de venda referenciando todas as dimensões

---

## Orquestração com Apache Airflow

A DAG `Monitoramento_preco_combustivel` orquestra o pipeline completo com as seguintes tasks em sequência:

```
extract_load_bronze → transform_load_silver → build_metrics_load_gold → load_dw
```

| Task | Descrição |
|---|---|
| `extract_load_bronze` | Lê os CSVs, converte para Parquet e envia para o S3 (camada Bronze) |
| `transform_load_silver` | Aplica transformações e limpeza, gera o Parquet Silver e envia para o S3 |
| `build_metrics_load_gold` | Constrói o modelo estrela, gera os Parquets Gold e envia para o S3 |
| `load_dw` | Carrega as tabelas dimensionais e a tabela fato no PostgreSQL |

---

## API — Endpoints Disponíveis

A API é construída com **FastAPI** e realiza consultas diretamente no Data Warehouse PostgreSQL, usando o modelo estrela para cruzamento de informações.

### Bairros
| Método | Rota | Descrição |
|---|---|---|
| GET | `/bairro/media` | Preço médio por combustível em cada bairro de Salvador |
| GET | `/bairro/mais-caro` | Bairros com maior preço médio (ordem decrescente) |
| GET | `/bairro/mais-barato` | Bairros com menor preço médio (ordem crescente) |

### Postos
| Método | Rota | Descrição |
|---|---|---|
| GET | `/postos` | Lista todos os postos de abastecimento em Salvador |
| GET | `/postos/mais-caros` | Postos com maior preço médio de venda |
| GET | `/postos/mais-barato` | Postos com menor preço médio de venda |

### Preços
| Método | Rota | Descrição |
|---|---|---|
| GET | `/precos/media-geral` | Preço médio geral por tipo de combustível em Salvador |

### Períodos Festivos
| Método | Rota | Descrição |
|---|---|---|
| GET | `/periodo-festivo/revellion` | Preços durante o Réveillon (01 a 03/01/2025) |
| GET | `/periodo-festivo/carnaval` | Preços durante o Carnaval (27/02 a 04/03/2025) |
| GET | `/periodo-festivo/festas-juninas` | Preços durante as Festas Juninas (20 a 26/06/2025) |
| GET | `/periodo-festivo/natal` | Preços durante o Natal (22 a 28/12/2025) |

---

## Estrutura do Projeto

```
monitoramento_combustivel_salvador/
├── app/
│   ├── database/          # Configuração da conexão com PostgreSQL
│   ├── routers/           # Endpoints da API (bairro, postos, preço, período festivo)
│   ├── dependencies.py    # Injeção de dependência do banco
│   └── main.py            # Inicialização da aplicação FastAPI
├── dags/
│   └── run_pipeline.py    # DAG do Airflow
├── data/                  # Arquivos CSV brutos da ANP
├── data_lake/
│   ├── bronze/            # Dados brutos em Parquet
│   ├── silver/            # Dados tratados em Parquet
│   └── gold/              # Modelo estrela em Parquet
├── src/
│   ├── extract.py         # Extração e carga na camada Bronze
│   ├── transform.py       # Transformações e carga na camada Silver
│   ├── build_metrics.py   # Modelo dimensional e carga na camada Gold
│   ├── load.py            # Carga no Data Warehouse PostgreSQL
│   ├── s3_load.py         # Upload para AWS S3
│   └── utils/             # Logger
├── sql/
│   └── script_tabelas.sql # DDL das tabelas do Data Warehouse
├── streamlit_app/
│   └── dashboard.py       # Dashboard interativo (em desenvolvimento)
├── docker-compose.yml
├── Dockerfile
├── Dockerfile.airflow
└── requirements.txt
```

---

## Fonte dos Dados

Os dados utilizados são públicos e disponibilizados pela **ANP — Agência Nacional do Petróleo, Gás Natural e Biocombustíveis**, referentes ao levantamento semanal de preços de combustíveis automotivos no ano de 2025.

---

## Status do Projeto

O projeto está em desenvolvimento ativo. O pipeline ETL e o modelo dimensional estão operacionais. As etapas pendentes são:

- [ ] Deploy da API FastAPI
- [ ] Deploy do banco de dados PostgreSQL
- [ ] Desenvolvimento e deploy do dashboard Streamlit

---

## Como Executar (Localmente)

**Pré-requisitos:** Docker e Docker Compose instalados.

```bash
# Clone o repositório
git clone <url-do-repositorio>
cd monitoramento_combustivel_salvador

# Configure as variáveis de ambiente
cp .env.docker .env
# Edite o .env com suas credenciais (AWS, PostgreSQL, caminhos dos CSVs)

# Suba os serviços
docker-compose up -d

# Acesse o Airflow em http://localhost:8080
# Ative e execute a DAG: Monitoramento_preco_combustivel

# Acesse a documentação da API em http://localhost:8000/docs
```