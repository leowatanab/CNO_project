import streamlit as st
from utils.data import load_data
from utils.filters import sidebar_filters

st.title("🏙️ Detalhamento por Município")

df = load_data()
df_filt = sidebar_filters(df)

tabela_municipios = (
    df_filt
    .groupby(["Estado", "Nome do município"])
    .size()
    .reset_index(name="Quantidade de obras")
    .sort_values("Quantidade de obras", ascending=False)
)

tabela_municipios = tabela_municipios.rename(
    columns={
        "Estado": "UF",
        "Nome do município": "Município"
    }
)

st.dataframe(tabela_municipios, width= 800, height=700)
