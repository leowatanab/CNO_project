import streamlit as st
import pandas as pd

def sidebar_filters(df: pd.DataFrame) -> pd.DataFrame:
    with st.sidebar:
        st.header("🔍 Filtros")

        df_filt = df.copy()

        # Estado
        estados = sorted(df_filt["Estado"].dropna().unique())
        estado = st.selectbox("Estado", ["Todos"] + estados)

        if estado != "Todos":
            df_filt = df_filt[df_filt["Estado"] == estado]

        # Município
        municipios = sorted(df_filt["Nome do município"].dropna().unique())
        municipio = st.selectbox("Município", ["Todos"] + municipios)

        if municipio != "Todos":
            df_filt = df_filt[df_filt["Nome do município"] == municipio]

        # Empresa
        empresas = sorted(df_filt["Nome empresarial"].dropna().unique())
        empresa = st.selectbox("Nome empresarial", ["Todos"] + empresas)

        if empresa != "Todos":
            df_filt = df_filt[df_filt["Nome empresarial"] == empresa]

        # Período
        st.markdown("### 📅 Período de registro")

        data_min = df_filt["Data de registro"].min()
        data_max = df_filt["Data de registro"].max()

        ini, fim = st.date_input(
            "Selecione o período",
            value=(data_min, data_max),
            min_value=data_min,
            max_value=data_max
        )

        df_filt = df_filt[
            (df_filt["Data de registro"] >= ini) &
            (df_filt["Data de registro"] <= fim)
        ]

    return df_filt
