import streamlit as st

st.set_page_config(
    page_title="Monitoramento de Combustíveis",
    page_icon="⛽",
    layout="wide"
)

st.markdown("""
# ⛽ Monitoramento dos Preços dos Combustíveis em Salvador

### Pipeline de Engenharia de Dados • ANP 2025

Este dashboard apresenta análises sobre os preços dos combustíveis automotivos em Salvador/BA,
utilizando dados públicos da Agência Nacional do Petróleo (ANP).

Os dados são processados por um pipeline de engenharia de dados com arquitetura medallion
(Bronze → Silver → Gold), orquestrado pelo Apache Airflow, salvos em um Data Warehouse PostgreSQL e disponibilizado através de uma API FastAPI.

---
""")

col1, col2, col3 = st.columns(3)

with col1:
    st.metric(
        label="Cidade Monitorada",
        value="Salvador/BA"
    )

with col2:
    st.metric(
        label="Ano de Referência",
        value="2025"
    )

with col3:
    st.metric(
        label="Fonte dos Dados",
        value="ANP"
    )

st.markdown("## 📊 Análises Disponíveis")

st.markdown("""
- Média geral por combustível
- Ranking dos bairros mais caros
- Ranking dos bairros mais baratos
- Ranking dos postos mais caros
- Ranking dos postos mais baratos
- Comparativo em períodos festivos
""")

st.info(
    "Utilize o menu lateral para navegar entre as análises disponíveis."
)