import streamlit as st
from utils.data import load_data
from utils.filters import sidebar_filters

st.title("📋 Tabela Completa")

df = load_data()
df_filt = sidebar_filters(df)

st.dataframe(df_filt, width= 'content', height=700)
