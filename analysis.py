import streamlit as st
import duckdb
from pathlib import Path

DB_PATH = Path(__file__).parent / "cno.duckdb"

@st.cache_resource
def get_connection():
    return duckdb.connect(database=str(DB_PATH), read_only=True)

@st.cache_data
def load_data(table, limit = 100000):
    con = get_connection()
    return con.execute(f"SELECT * FROM {table} LIMIT {limit}").df()

tables = ['cno', 'cno_areas', 'cno_cnaes', 'cno_totais', 'cno_vinculos']

st.title("📊 Análise do Cadastro Nacional de Obras (CNO)")

# Seleção da tabela pelo usuário
table = st.selectbox("Selecione a tabela", tables)

# Carrega apenas a tabela escolhida
df = load_data(table)

st.dataframe(df)



