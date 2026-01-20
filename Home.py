import streamlit as st

st.set_page_config(
    page_title="Análise CNO",
    layout="wide",
    page_icon="📊"
)

st.title("Dados Cadastro Nacional de Obras (CNO)")
st.markdown("""
Esse é um projeto desenvolvido com objetivo de mapear as obras cadastradas no CNO (Cadastro Nacional de Obras) que podem ser potenciais clientes Polar.

Os dados são provenientes do [Portal CNO](https://cno.dataprev.gov.br/) e armazenados em um banco de dados MotherDuck. A atualização dos dados ocorre semanalmente para que possam ser usados nas diferentes aplicações.
            
Aqui, alguns filtros já estão sendo feitos para focar em obras que são mais relevantes para a Polar, como:
- Categoria: Obra Nova
- Destinação: Residencial multifamiliar
- Situação: Ativa (código 02)
- Metragem: Acima de 500 m²
            
Você pode navegar entre as diferentes páginas para obter uma visão geral dos dados, bem como detalhes por estado e município.
            
Autor: Leonardo Koiti Watanabe
""")
