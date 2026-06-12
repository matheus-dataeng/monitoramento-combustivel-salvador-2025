import streamlit as st

st.set_page_config(
    page_title="Monitoramento de Combustíveis",
    page_icon="⛽",
    layout="wide"
)

st.title("Monitoramento de Combustíveis em Salvador")

st.write("""
Dashboard desenvolvido a partir dos dados da ANP,
processados por um pipeline de engenharia de dados.
""")
