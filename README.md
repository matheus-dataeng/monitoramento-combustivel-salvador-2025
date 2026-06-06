<div align="center">

# ⛽ Monitoramento dos Preços dos Combustíveis em Salvador
### Pipeline de Dados · ANP 2025

![Python](https://img.shields.io/badge/Python-3.11-3776AB?style=for-the-badge&logo=python&logoColor=white)
![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-2.x-017CEE?style=for-the-badge&logo=apacheairflow&logoColor=white)
![FastAPI](https://img.shields.io/badge/FastAPI-1.0.0-009688?style=for-the-badge&logo=fastapi&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-Data%20Warehouse-4169E1?style=for-the-badge&logo=postgresql&logoColor=white)
![AWS S3](https://img.shields.io/badge/AWS%20S3-Data%20Lake-FF9900?style=for-the-badge&logo=amazons3&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?style=for-the-badge&logo=docker&logoColor=white)
![Status](https://img.shields.io/badge/Status-Em%20Desenvolvimento-yellow?style=for-the-badge)

<br/>

> Pipeline completo de engenharia de dados para coleta, transformação e análise dos preços de combustíveis automotivos em **Salvador/BA**, com base nos dados públicos da **ANP — Agência Nacional do Petróleo, Gás Natural e Biocombustíveis**.

</div>

---

## Sobre o Projeto

Os preços dos combustíveis impactam diretamente o dia a dia de milhões de brasileiros. Este projeto nasce da curiosidade de entender como esses preços se comportam na cidade de Salvador ao longo de 2025 — e de construir, do zero, uma arquitetura de dados robusta e escalável para responder a essas perguntas.

O pipeline implementa a **Medallion Architecture** (Bronze → Silver → Gold), com orquestração via Apache Airflow, armazenamento em AWS S3, Data Warehouse em PostgreSQL e uma API REST construída com FastAPI para expor os dados tratados. Um dashboard interativo com Streamlit está em desenvolvimento para tornar as análises acessíveis visualmente.

---

## Arquitetura do Pipeline

<div align="center">
  <img src="assets/Arquitetura%20-%20Projeto.png" alt="Arquitetura do Pipeline de Dados" width="100%"/>
</div>

---

## Stack Tecnológica

| Tecnologia | Papel no Projeto |
|---|---|
| **Python 3.12** | Linguagem principal — Pandas, SQLAlchemy, python-dotenv |
| **Apache Airflow** | Orquestração e agendamento do pipeline ETL |
| **Docker / Docker Compose** | Containerização de todos os serviços |
| **AWS S3** | Armazenamento do Data Lake (camadas Bronze, Silver e Gold) |
| **PostgreSQL** | Data Warehouse relacional com modelo estrela |
| **FastAPI** | API REST para exposição e consulta dos dados |
| **Streamlit** | Dashboard interativo *(em desenvolvimento)* |

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

| Tabela | Conteúdo |
|---|---|
| `dim_tempo` | Data de coleta, ano, mês, dia e dia da semana |
| `dim_revenda` | Nome do posto, CNPJ e bandeira |
| `dim_produto` | Tipo de combustível e unidade de medida |
| `dim_localizacao` | Região, estado, município e bairro |
| `fato_preco` | Valor de venda referenciando todas as dimensões |

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
| `load_dw` | Carrega as dimensões e a tabela fato no PostgreSQL |

---

## API REST — Endpoints Disponíveis

A API é construída com **FastAPI** e consulta diretamente o Data Warehouse PostgreSQL, cruzando as tabelas do Star Schema para entregar respostas ricas e precisas.

> Documentação interativa disponível em `http://localhost:8000/docs`

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
│   ├── load.py                   # Carga → PostgreSQL
│   ├── s3_load.py                # Upload → AWS S3
│   └── utils/logger_config.py   # Configuração de logs
│
├── sql/
│   └── script_tabelas.sql        # DDL do Data Warehouse
│
├── streamlit_app/
│   └── dashboard.py              # Dashboard interativo (em desenvolvimento)
│
├── docker-compose.yml
├── Dockerfile
├── Dockerfile.airflow
└── requirements.txt
```

---

## Como Executar

**Pré-requisitos:** Docker e Docker Compose instalados.

```bash
# 1. Clone o repositório
git clone <url-do-repositorio>
cd monitoramento_combustivel_salvador

# 2. Configure as variáveis de ambiente
cp .env.docker .env
# Edite o .env com suas credenciais (AWS, PostgreSQL e caminhos dos CSVs)

# 3. Suba todos os serviços
docker-compose up -d

# 4. Acesse o Airflow e execute o pipeline
# http://localhost:8080
# DAG: Monitoramento_preco_combustivel

# 5. Consulte a API
# http://localhost:8000/docs
```

---

## Fonte dos Dados

Os dados utilizados são públicos, disponibilizados semanalmente pela **ANP — Agência Nacional do Petróleo, Gás Natural e Biocombustíveis**, como parte do Programa de Monitoramento dos Preços dos Combustíveis (PMPC).

---

## Status do Projeto

O pipeline ETL completo e o modelo dimensional estão operacionais e testados. As próximas etapas são:

- [x] Pipeline ETL com Airflow (Bronze → Silver → Gold)
- [x] Integração com AWS S3
- [x] Star Schema e carga no PostgreSQL
- [x] API REST com FastAPI
- [ ] Deploy da API
- [ ] Deploy do banco de dados PostgreSQL
- [ ] Conclusão e deploy do dashboard Streamlit

---

<div align="center">
  <sub>Desenvolvido por <strong>Matheus</strong> · Salvador, BA · 2025</sub>
</div>
