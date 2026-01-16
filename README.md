# Visão Geral

Este projeto tem como objetivo o monitoramento, consolidação e análise de preços de combustíveis na cidade de Salvador (BA) ao longo de 2025, com foco especial em períodos festivos, onde historicamente há maior variação de preços.

O projeto foi desenvolvido com mentalidade de engenharia de dados, priorizando:

- organização de pipelines
- separação entre código, consultas e dados
- reprodutibilidade
- boas práticas de versionamento

# Objetivos do Projeto

- Coletar e consolidar dados de preços de combustíveis
- Monitorar variações ao longo do tempo
- Analisar periodos festivos
- Disponibilizar dados tratados para analise e visualização

# Arquitetura do Projeto

Fluxo do pipeline:

# 1 - Extração

- Dados obtidos a partir de fontes públicas (APIs / arquivos CSV) GOV

# 2 - Transformação

- Padronização de colunas
- Limpeza de dados
- Consolidação de múltiplos arquivos (ambos os semestres de 2025)

# 3 - Carga

- Geração de datasets finais
- Exportação para arquivos estruturados
- Integração com Google Sheets

# Versionamento de Dados

# 1 - Por boas práticas de engenharia de dados:

- Arquivos de dados (.csv) não são versionados no repositório (Apenas o relatorio de 2025, contendo 100 linhas)
- Apenas código, queries e pipelines são mantidos no Git

# 2 - Garantindo:

- Repositório leve
- Histórico limpo
- Facilidade de reprodução dos dados via pipeline

# Possíveis Análises

- Evolução dos preços ao longo do ano
- Comparação entre bairros e postos
- Impacto de datas festivas nos preços
- Identificação de padrões 

# Tecnologias Utilizadas

- Python
- Pandas
- SQL
- Jupyter Notebook
- Git & GitHub
- Google Sheets API

# Observação Final

Este projeto tem fins educacionais e demonstrativos, simulando um cenário real do universo da engenharia de dados aplicada a um problema de negócio.