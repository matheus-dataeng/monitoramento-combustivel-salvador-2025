<div align="center">

# ⛽ Monitoramento dos Preços dos Combustíveis em Salvador
### Pipeline de Dados · ANP 2025

![Python](https://img.shields.io/badge/Python-3.11-3776AB?style=for-the-badge&logo=python&logoColor=white)
![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-2.x-017CEE?style=for-the-badge&logo=apacheairflow&logoColor=white)
![FastAPI](https://img.shields.io/badge/FastAPI-1.0.0-009688?style=for-the-badge&logo=fastapi&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-Data%20Warehouse-4169E1?style=for-the-badge&logo=postgresql&logoColor=white)
![AWS S3](https://img.shields.io/badge/AWS%20S3-Data%20Lake-FF9900?style=for-the-badge&logo=amazons3&logoColor=white)
![AWS RDS](https://img.shields.io/badge/AWS%20RDS-PostgreSQL-527FFF?style=for-the-badge&logo=amazonrds&logoColor=white)
![AWS EC2](https://img.shields.io/badge/AWS%20EC2-Deploy-FF9900?style=for-the-badge&logo=amazonec2&logoColor=white)
![Streamlit](https://img.shields.io/badge/Streamlit-Dashboard-FF4B4B?style=for-the-badge&logo=streamlit&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?style=for-the-badge&logo=docker&logoColor=white)
![Status](https://img.shields.io/badge/Status-Concluído-brightgreen?style=for-the-badge)

<br/>

> Pipeline completo de engenharia de dados para coleta, transformação e análise dos preços de combustíveis automotivos em **Salvador/BA**, com base nos dados públicos da **ANP — Agência Nacional do Petróleo, Gás Natural e Biocombustíveis**.

</div>

## Sobre o Projeto

Os preços dos combustíveis impactam diretamente o dia a dia de milhões de brasileiros. Este projeto nasce da curiosidade de entender como esses preços se comportam na cidade de Salvador ao longo de 2025 e de construir, do zero, uma arquitetura de dados robusta e escalável para responder a essas perguntas.

O pipeline implementa a **Medallion Architecture** (Bronze → Silver → Gold), com orquestração via Apache Airflow, armazenamento em AWS S3, Data Warehouse em AWS RDS PostgreSQL e uma API REST construída com FastAPI hospedada em AWS EC2 para expor os dados tratados. Um dashboard interativo desenvolvido com Streamlit permite visualizar indicadores, tendências de preços e comparativos entre combustíveis, bairros e períodos festivos.

---

## Arquitetura do Pipeline

<div align="center">
  <img src="assets/Arquitetura%20-%20Projeto.png" alt="Arquitetura do Pipeline de Dados" width="100%"/>
</div>

---

## Stack Tecnológica

| Tecnologia | Papel no Projeto |
|---|---|
| **Python 3.11** | Linguagem principal — Pandas, SQLAlchemy, python-dotenv |
| **Apache Airflow** | Orquestração e agendamento do pipeline ETL (on-premise) |
| **Docker / Docker Compose** | Containerização dos serviços |
| **AWS S3** | Armazenamento do Data Lake (camadas Bronze, Silver e Gold) |
| **AWS RDS PostgreSQL** | Data Warehouse relacional em nuvem com modelo estrela |
| **AWS EC2** | Hospedagem da API FastAPI e dashboard Streamlit |
| **AWS IAM** | Gerenciamento de acessos e roles para os serviços AWS |
| **FastAPI** | API REST para exposição e consulta dos dados |
| **Streamlit** | Dashboard analítico para visualização dos dados |

---

## Infraestrutura em Nuvem (AWS)

| Serviço | Configuração | Uso no Projeto |
|---|---|---|
| **AWS S3** | Região us-east-1 | Data Lake com camadas Bronze, Silver e Gold em Parquet |
| **AWS RDS PostgreSQL** | db.t4g.micro · sa-east-1 | Data Warehouse com 802.930 registros na fato_preco |
| **AWS EC2** | t3.micro · Ubuntu 26.04 · sa-east-1 | API FastAPI + Dashboard Streamlit em produção |
| **AWS IAM** | Usuários + Role para EC2 | Controle de acesso seguro sem credenciais hardcoded |

> **Decisão arquitetural:** A orquestração com Apache Airflow roda on-premise. A instância t3.micro do Free Tier da AWS possui apenas 1GB de RAM, insuficiente para sustentar os containers do Airflow (webserver + scheduler) em operação simultânea. A evolução natural do projeto é realizar o upgrade da instância e, em paralelo, migrar o processamento para **Apache Spark**, permitindo execução distribuída e eficiente em cloud.

---

## Camadas do Data Lake

### 🥉 Bronze — Ingestão
Camada de aterrisagem dos dados. Os arquivos CSV semestrais da ANP são lidos e convertidos diretamente para Parquet, sem qualquer transformação. O objetivo é preservar os dados originais com fidelidade.

```
data_lake/bronze/
├── analise_semestre_1.parquet
└── analise_semestre_2.parquet
```

### 🥈 Silver — Qualidade
Camada de tratamento e confiabilidade dos dados. As seguintes transformações são aplicadas:

- Concatenação dos dois semestres em um único dataset anual
- Renomeação e padronização de todas as colunas
- Remoção de registros duplicados
- Validação e conversão do campo `valor_venda` (vírgula → ponto, coerção numérica)
- Validação e padronização das datas de coleta

```
data_lake/silver/
└── analise_geral_2025.parquet
```

### 🥇 Gold — Modelo Analítico
Camada de valor de negócio. Os dados são modelados seguindo o padrão **Star Schema**, com dimensões e tabela fato prontas para consultas analíticas de alto desempenho.

| Tabela | Linhas | Conteúdo |
|---|---|---|
| `dim_tempo` | 274 | Data de coleta, ano, mês, dia e dia da semana |
| `dim_revenda` | 15.226 | Nome do posto, CNPJ e bandeira |
| `dim_produto` | 7 | Tipo de combustível e unidade de medida |
| `dim_localizacao` | 5.607 | Região, estado, município e bairro |
| `fato_preco` | 802.930 | Valor de venda referenciando todas as dimensões |

```
data_lake/gold/
├── dim_tempo.parquet
├── dim_revenda.parquet
├── dim_produto.parquet
├── dim_localizacao.parquet
└── fato_preco.parquet
```

---

## Orquestração com Apache Airflow

A DAG `Monitoramento_preco_combustivel` gerencia o fluxo completo do pipeline, garantindo que cada etapa seja executada na ordem correta e com tratamento de erros.

```
extract_load_bronze ──► transform_load_silver ──► build_metrics_load_gold ──► load_dw
```

| Task | O que faz |
|---|---|
| `extract_load_bronze` | Lê os CSVs da ANP, converte para Parquet e faz upload para o S3 |
| `transform_load_silver` | Aplica todas as transformações e envia o Parquet tratado para o S3 |
| `build_metrics_load_gold` | Constrói o Star Schema e envia os arquivos Gold para o S3 |
| `load_dw` | Carrega as dimensões e a tabela fato no AWS RDS PostgreSQL |

---

## API REST — Endpoints Disponíveis

A API é construída com **FastAPI**, hospedada em **AWS EC2**, e consulta diretamente o Data Warehouse no **AWS RDS PostgreSQL**, cruzando as tabelas do Star Schema para entregar respostas ricas e precisas.

### 🏘️ Bairros

| Método | Rota | Descrição |
|---|---|---|
| `GET` | `/bairro/media` | Preço médio por combustível em cada bairro de Salvador |
| `GET` | `/bairro/mais-caro` | Bairros ordenados pelo maior preço médio |
| `GET` | `/bairro/mais-barato` | Bairros ordenados pelo menor preço médio |

### ⛽ Postos

| Método | Rota | Descrição |
|---|---|---|
| `GET` | `/postos` | Lista todos os postos de abastecimento em Salvador |
| `GET` | `/postos/mais-caros` | Postos com maior preço médio de venda |
| `GET` | `/postos/mais-barato` | Postos com menor preço médio de venda |

### 💰 Preços

| Método | Rota | Descrição |
|---|---|---|
| `GET` | `/precos/media-geral` | Preço médio geral por tipo de combustível em Salvador |

### 🎉 Períodos Festivos

Análise do comportamento dos preços durante as principais datas do calendário baiano:

| Método | Rota | Período |
|---|---|---|
| `GET` | `/periodo-festivo/revellion` | 01 a 03/01/2025 |
| `GET` | `/periodo-festivo/carnaval` | 27/02 a 04/03/2025 |
| `GET` | `/periodo-festivo/festas-juninas` | 20 a 26/06/2025 |
| `GET` | `/periodo-festivo/natal` | 22 a 28/12/2025 |

---

## Dashboard Streamlit

O dashboard é construído com **Streamlit** e **Plotly**, consumindo dados diretamente da API FastAPI. Organizado em múltiplas páginas:

- **Visão Geral** — preço médio por combustível em Salvador
- **Réveillon** — comportamento dos preços no período de 01 a 03/01/2025
- **Carnaval** — evolução dos preços de 27/02 a 04/03/2025
- **Festas Juninas** — análise do período de 20 a 26/06/2025
- **Natal** — preços no período de 22 a 28/12/2025

---

## Estrutura do Repositório

```
monitoramento_combustivel_salvador/
│
├── app/                          # Aplicação FastAPI
│   ├── database/                 # Configuração e conexão com PostgreSQL
│   ├── routers/                  # Endpoints organizados por domínio
│   │   ├── bairro.py
│   │   ├── postos.py
│   │   ├── preco.py
│   │   └── periodo_festivo.py
│   ├── dependencies.py           # Injeção de dependência do banco
│   └── main.py                   # Inicialização da API
│
├── dags/
│   └── run_pipeline.py           # DAG do Airflow
│
├── data/                         # Arquivos CSV brutos (ANP)
│
├── data_lake/
│   ├── bronze/                   # Dados brutos em Parquet
│   ├── silver/                   # Dados tratados em Parquet
│   └── gold/                     # Star Schema em Parquet
│
├── src/                          # Lógica do pipeline
│   ├── extract.py                # Extração → Bronze
│   ├── transform.py              # Transformação → Silver
│   ├── build_metrics.py          # Modelagem → Gold
│   ├── load.py                   # Carga → AWS RDS PostgreSQL
│   ├── s3_load.py                # Upload → AWS S3
│   └── utils/logger_config.py   # Configuração de logs
│
├── sql/
│   └── script_tabelas.sql        # DDL do Data Warehouse
│
├── streamlit_app/                # Dashboard interativo
│   ├── app.py                    # Página principal
│   ├── pages/                    # Páginas por período festivo
│   └── services/
│       └── api.py                # Camada de integração com a API
│
├── docker-compose.yml
├── Dockerfile.api
├── Dockerfile.streamlit
├── Dockerfile.airflow
└── requirements.txt
```

---

## Como Executar

**Pré-requisitos:** Docker e Docker Compose instalados.

```bash
# 1. Clone o repositório
git clone https://github.com/matheus-dataeng/monitoramento-combustivel-salvador-2025.git
cd monitoramento-combustivel-salvador-2025

# 2. Configure as variáveis de ambiente
cp .env.docker .env
# Edite o .env com suas credenciais (AWS, PostgreSQL e caminhos dos CSVs)

# 3. Suba os serviços
docker compose up -d
```

> **Nota:** O arquivo `.env.docker` deve conter as credenciais do AWS S3, AWS RDS e os caminhos dos CSVs da ANP. Nunca commite credenciais no repositório.

---

## Status do Projeto

- [x] Pipeline ETL com Airflow (Bronze → Silver → Gold)
- [x] Integração com AWS S3 (Data Lake em nuvem)
- [x] Star Schema e carga no PostgreSQL
- [x] API REST com FastAPI
- [x] Deploy do Data Lake (AWS S3)
- [x] Deploy do Data Warehouse (AWS RDS PostgreSQL)
- [x] Deploy da API (AWS EC2)
- [x] Dashboard Streamlit com Plotly
- [x] Deploy do Dashboard (AWS EC2)

---

## Fonte dos Dados

Os dados utilizados são públicos, disponibilizados semanalmente pela **ANP — Agência Nacional do Petróleo, Gás Natural e Biocombustíveis**, como parte do Programa de Monitoramento dos Preços dos Combustíveis (PMPC).

---

<div align="center">
  <sub>Desenvolvido por <strong>Matheus</strong> · Salvador, BA · 2026</sub>
</div>
