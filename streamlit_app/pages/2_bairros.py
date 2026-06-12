import streamlit as st 
import plotly.express as px
import pandas as pd 
from services.api import get_bairro
from utils.layout_graph import default_layout


st.title("Preços por bairros")

dados = get_bairro()

df_bairros = pd.DataFrame(dados)

with st.expander("Visualizar dados"):
    st.dataframe(df_bairros)

graph_bairros = px.bar(
    df_bairros,
    orientation="h",
    x= "media_preco",
    y= "bairro",
    text= "media_preco",
    title= "Preço médio dos combustiveis por bairro em Salvador - 2025"
)

graph_bairros.update_layout(
    height=600,
    title_font_size=25,
    xaxis_title_font=dict(size=17),
    yaxis_title_font=dict(size=17),
    xaxis=dict(tickfont=dict(size=17)),
    yaxis=dict(tickfont=dict(size=17))
)

st.plotly_chart(graph_bairros, use_container_width=True)