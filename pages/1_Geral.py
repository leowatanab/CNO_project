import streamlit as st
from utils.data import load_data
from utils.filters import sidebar_filters
from utils.charts import chart_por_estado, heatmap_por_estado

st.title("Visão Geral")

df = load_data()
df_filt = sidebar_filters(df)

st.metric("Total de Obras", len(df_filt))
chart_por_estado(df_filt)

heatmap_por_estado(df_filt)