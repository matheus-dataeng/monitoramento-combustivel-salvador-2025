import streamlit as st
import plotly.express as px
import pandas as pd
from services.api import get_carnaval
from utils.layout_graph import default_layout

st.title("Preços dos combustiveis no periodo de carnaval")

dados_carnaval = get_carnaval()

df_carnaval = pd.DataFrame(dados_carnaval)

with st.expander("Visualizar dados"):
    st.dataframe(df_carnaval)

produto_selecionado = st.selectbox(
    "Selecione o combustível",
    df_carnaval["produto"].unique()
)

df_filtrado = df_carnaval[df_carnaval["produto"] == produto_selecionado]

graph_carnaval = px.line(
    df_filtrado,
    x= "data_coleta",
    y= "media_preco",
    color= "bairro",
    markers=True,
    title= f"Média do(a) {produto_selecionado} durante o carnaval em Salvador - 2025"
)

graph_carnaval = default_layout(graph_carnaval)
st.plotly_chart(graph_carnaval, use_container_width=True)