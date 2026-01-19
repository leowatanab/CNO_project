import streamlit as st
import pandas as pd
import duckdb
import os
import folium
from folium.plugins import MarkerCluster
from streamlit_folium import st_folium
from openlocationcode import openlocationcode as olc
from streamlit_dynamic_filters import DynamicFilters
import altair as alt
import seaborn as sns
import matplotlib.pyplot as plt


@st.cache_data
# =====================================================
# Query a table from MotherDuck and return a pandas DataFrame
# =====================================================
def query_table(query: str) -> pd.DataFrame:
    md_token = os.getenv("MOTHERDUCK_TOKEN")
    if not md_token:
        raise ValueError("❌ Token do MotherDuck não encontrado")

    con = duckdb.connect(f"md:?motherduck_token={md_token}")
    try:
        df = con.sql(query).df()
    finally:
        con.close()

    return df

# =====================================================
# Query sentence
# =====================================================
query = """
SELECT
    CNO,
    Nome,
    "Data de início",
    "Data de registro",
    Estado,
    "Nome do município",
    CEP,
    "Tipo de logradouro" || ' ' || Logradouro AS "Endereço completo",
    "Número do logradouro",
    Categoria,
    Destinação,
    Situação,
    "Código de localização",
    "Nome empresarial", 
    Metragem
FROM cno.cno_base
WHERE "Data de registro" > '2020-01-01'
  AND Categoria = 'Obra Nova'
  AND Destinação = 'Residencial multifamiliar'
  AND Situação = '02'
"""

# =====================================================
# Load data
df = query_table(query)

# Transform "Data de registro" to datetime format
df["Data de registro"] = pd.to_datetime(df["Data de registro"], errors="coerce")
df["Data de início"] = pd.to_datetime(df["Data de início"], errors="coerce")

# =====================================================
# Filter 'Data de registro' in last 7 days
df = df[df["Data de registro"] >= (pd.Timestamp.now() - pd.Timedelta(days=7))]

# Filter 'Data de início' in last 100 days
df = df[df["Data de início"] >= (pd.Timestamp.now() - pd.Timedelta(days=100))]

# =====================================================
# Display dataframe
st.title("🏗️ Obras Novas Residenciais Multifamiliares - Últimos 7 dias")

st.dataframe(df)

st.write(f"Total de obras encontradas: {len(df)}")

