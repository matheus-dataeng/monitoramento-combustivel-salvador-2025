import streamlit as st 
import pandas as pd 
import plotly.express as px 
from services.api import get_media_geral
from utils.layout_graph import default_layout, color_map

st.title("Visão Geral")

dados = get_media_geral()

df_geral = pd.DataFrame(dados)

with st.expander("Visualizar dados"):
    st.dataframe(df_geral)

graph_geral = px.bar(
    df_geral,
    x= "produto",
    y= "media_preco",
    color ="produto",
    text= "media_preco",
    color_discrete_map=color_map(),
    title= "Preço médio por combustivel em Salvador - 2025"
)

graph_geral = default_layout(graph_geral)
st.plotly_chart(graph_geral, use_container_width=True)