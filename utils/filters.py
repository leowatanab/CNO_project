import streamlit as st
import pandas as pd

def sidebar_filters(df: pd.DataFrame) -> pd.DataFrame:
    with st.sidebar:
        st.header("🔍 Filtros")

        df_filt = df.copy()

        # =============================
        # ESTADO
        # =============================
        estados = sorted(df_filt["Estado"].dropna().unique())
        estado = st.selectbox(
            "Estado",
            ["Todos"] + estados,
            key="filtro_estado"
        )

        if estado != "Todos":
            df_filt = df_filt[df_filt["Estado"] == estado]

        # =============================
        # MUNICÍPIO
        # =============================
        municipios = sorted(df_filt["Nome do município"].dropna().unique())
        municipio = st.selectbox(
            "Município",
            ["Todos"] + municipios,
            key="filtro_municipio"
        )

        if municipio != "Todos":
            df_filt = df_filt[df_filt["Nome do município"] == municipio]

        # =============================
        # EMPRESA
        # =============================
        empresas = sorted(df_filt["Nome empresarial"].dropna().unique())
        empresa = st.selectbox(
            "Nome empresarial",
            ["Todos"] + empresas,
            key="filtro_empresa"
        )

        if empresa != "Todos":
            df_filt = df_filt[df_filt["Nome empresarial"] == empresa]

        # =============================
        # PERÍODO
        # =============================
        st.markdown("### 📅 Período de registro")

        df_filt["Data de registro"] = pd.to_datetime(df_filt["Data de registro"])

        data_min = df_filt["Data de registro"].min().date()
        data_max = df_filt["Data de registro"].max().date()

        ini, fim = st.date_input(
            "Selecione o período",
            value=st.session_state.get(
                "filtro_periodo",
                (data_min, data_max)
            ),
            min_value=data_min,
            max_value=data_max,
            key="filtro_periodo"
        )

        df_filt = df_filt[
            (df_filt["Data de registro"] >= pd.to_datetime(ini)) &
            (df_filt["Data de registro"] <= pd.to_datetime(fim))
        ]

    return df_filt