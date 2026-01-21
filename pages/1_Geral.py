import streamlit as st
from utils.data import load_data
from utils.filters import sidebar_filters
from utils.charts import chart_por_estado, mapa_coropletico_obras

st.title("Visão Geral")

df = load_data()
df_filt = sidebar_filters(df)

st.metric("Total de Obras", len(df_filt))

mapa_coropletico_obras(df_filt)