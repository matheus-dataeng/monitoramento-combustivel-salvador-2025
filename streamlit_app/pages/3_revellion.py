import streamlit as st
import plotly.express as px
import pandas as pd 
from services.api import get_revellion
from utils.layout_graph import default_layout

st.title("Preços dos combustiveis no periodo de revellion")

dados_revellion = get_revellion()

df_revellion = pd.DataFrame(dados_revellion)

with st.expander("Visualizar dados"):
    st.dataframe(df_revellion)

produto_selecionado = st.selectbox(
    "Selecione o combustível",
    df_revellion["produto"].unique()
)

df_filtrado = df_revellion[df_revellion["produto"] == produto_selecionado]

graph_revellion = px.bar(
    df_filtrado,
    x= "bairro",
    y= "media_preco",
    color="bairro",
    barmode="group",
    title= "Média dos combustiveis no revellion em Salvador - 2025"
) 

graph_revellion = default_layout(graph_revellion)

st.plotly_chart(graph_revellion, use_container_width=True)