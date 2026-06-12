import streamlit as st
import plotly.express as px
import pandas as pd
from services.api import get_natal
from utils.layout_graph import default_layout

st.title("Preços dos combustiveis no periodo de festas juninas")

dados_natal = get_natal()

df_natal = pd.DataFrame(dados_natal)
df_natal["data_coleta"] = pd.to_datetime(df_natal["data_coleta"]).dt.strftime("%d/%m/%Y")

with st.expander("Visualizar dados"):
    st.dataframe(df_natal)

produto_selecionado = st.selectbox(
    "Selecione o combustível",
    df_natal["produto"].unique()
)

df_filtrado = df_natal[df_natal["produto"] == produto_selecionado]

graph_natal = px.bar(
    df_filtrado,
    x= "data_coleta",
    y= "media_preco",
    color="bairro",
    barmode="group",
    text= "media_preco",
    title= f"Média do(a) {produto_selecionado}<br>durante o natal em Salvador - 2025"
)

graph_natal = default_layout(graph_natal)
st.plotly_chart(graph_natal, use_container_width=True)