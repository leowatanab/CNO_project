import streamlit as st
import duckdb
import os
import pandas as pd

@st.cache_data(ttl=3600)
def load_data() -> pd.DataFrame:
    con = duckdb.connect(f"md:?motherduck_token={os.getenv('MOTHERDUCK_TOKEN')}")
    query = """
    SELECT
        CNO,
        Nome,
        "Data de registro",
        Estado,
        "Nome do município",
        "Nome empresarial",
        Metragem
    FROM cno.cno_base
    WHERE Categoria = 'Obra Nova'
      AND Destinação = 'Residencial multifamiliar'
      AND Situação = '02'
      AND TRY_CAST(Metragem AS DOUBLE) > 500
    """
    df = con.sql(query).df()
    con.close()

    df["Data de registro"] = (
        pd.to_datetime(df["Data de registro"], errors="coerce")
        .dt.date
    )
    df["Metragem"] = (
        df["Metragem"]
        .astype(str)
        .str.replace(",", ".", regex=False)
    )
    df["Metragem"] = pd.to_numeric(df["Metragem"], errors="coerce")

    return df
