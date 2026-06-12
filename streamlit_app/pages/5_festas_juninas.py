import streamlit as st
import plotly.express as px
import pandas as pd
from services.api import get_festas_juninas
from utils.layout_graph import default_layout

st.title("Preços dos combustiveis no periodo de festas juninas")

dados_juninos = get_festas_juninas()

df_junino = pd.DataFrame(dados_juninos)
df_junino["data_coleta"] = pd.to_datetime(df_junino["data_coleta"]).dt.strftime("%d/%m/%Y")

with st.expander("Visualizar dados"):
    st.dataframe(df_junino)

produto_selecionado = st.selectbox(
    "Selecione o combustível",
    df_junino["produto"].unique()
)

df_filtrado = df_junino[df_junino["produto"] == produto_selecionado]

graph_junino = px.bar(
    df_filtrado,
    x= "data_coleta",
    y= "media_preco",
    color="bairro",
    barmode="group",
    text= "media_preco",
    title= f"Média do(a) {produto_selecionado}<br>durante festas juninas em Salvador - 2025"
)

graph_junino = default_layout(graph_junino)

st.plotly_chart(graph_junino, use_container_width=True)