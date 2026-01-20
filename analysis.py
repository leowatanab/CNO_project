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


# =====================================================
# CONFIG STREAMLIT
# =====================================================
st.set_page_config(
    page_title="Análise CNO",
    layout="wide",
    page_icon="📊"
)

# =====================================================
# QUERY MOTHERDUCK (CONEXÃO CURTA)
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
# PLUS CODE → LAT/LON (SEGURO + BRASIL ONLY)
# =====================================================
@st.cache_data
def pluscode_to_latlon_safe(code):
    try:
        if not code or not olc.isFull(code):
            return None, None

        area = olc.decode(code)
        lat = (area.latitudeLo + area.latitudeHi) / 2
        lon = (area.longitudeLo + area.longitudeHi) / 2

        # Bounding box do Brasil
        if not (-33.75 <= lat <= 5.27 and -73.99 <= lon <= -34.79):
            return None, None

        return lat, lon
    except Exception:
        return None, None

def reset_municipio():
    st.session_state.municipio = "Todos"
    st.session_state.empresa = "Todos"

def reset_empresa():
    st.session_state.empresa = "Todos"


# =====================================================
# APP
# =====================================================
st.title("Análise de Dados CNO")
st.subheader("Consulta de Obras")


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
WHERE "Data de registro" > '2025-01-01'
  AND Categoria = 'Obra Nova'
  AND Destinação = 'Residencial multifamiliar'
  AND Situação = '02'
"""

df = query_table(query)

df["Data de registro"] = pd.to_datetime(
    df["Data de registro"],
    errors="coerce"
)


# =====================================================
# FILTRO NA BARRA LATERAL
# =====================================================

with st.sidebar:
    st.header("🔍 Filtros")

    # ======================
    # ESTADO
    # ======================
    estado = st.selectbox(
        "Estado",
        ["Todos"] + sorted(df["Estado"].dropna().unique()),
        key="estado",
        on_change=reset_municipio
    )

    df_filt = df if estado == "Todos" else df[df["Estado"] == estado]

    # ======================
    # MUNICÍPIO
    # ======================
    municipio = st.selectbox(
        "Município",
        ["Todos"] + sorted(df_filt["Nome do município"].dropna().unique()),
        key="municipio",
        on_change=reset_empresa
    )

    if municipio != "Todos":
        df_filt = df_filt[df_filt["Nome do município"] == municipio]

    # ======================
    # EMPRESA
    # ======================
    empresa = st.selectbox(
        "Nome empresarial",
        ["Todos"] + sorted(df_filt["Nome empresarial"].dropna().unique()),
        key="empresa"
    )

    if empresa != "Todos":
        df_filt = df_filt[df_filt["Nome empresarial"] == empresa]

    
    st.markdown("### 📅 Período de registro")

    data_min = df["Data de registro"].min()
    data_max = df["Data de registro"].max()

    data_inicio, data_fim = st.date_input(
        "Selecione o período",
        value=(data_min, data_max),
        min_value=data_min,
        max_value=data_max
    )

    # Aplicando o filtro
    if data_inicio and data_fim:
        df_filt = df_filt[
            (df_filt["Data de registro"] >= pd.to_datetime(data_inicio)) &
            (df_filt["Data de registro"] <= pd.to_datetime(data_fim))
        ]

# =====================================================
# ESTATÍSTICAS
# =====================================================
st.divider()
st.header("📈 Estatísticas")

st.metric("Total de Obras", len(df_filt))
# Garante string e remove nulos
df_aux = df_filt.copy()
df_aux["Estado"] = df_aux["Estado"].astype(str)
df_aux = df_aux[df_aux["Estado"].notna()]

# value_counts -> DataFrame bem definido
state_counts = (
    df_aux["Estado"]
    .value_counts()
    .rename("Quantidade")
    .reset_index()
    .rename(columns={"index": "Estado"})
)

# Gráfico de barras
bars = (
    alt.Chart(state_counts)
    .mark_bar()
    .encode(
        y=alt.Y(
            "Estado:N",
            sort="-x",
            title="Estado"
        ),
        x=alt.X(
            "Quantidade:Q",
            title="Quantidade de obras"
        ),
        tooltip=[
            alt.Tooltip("Estado:N"),
            alt.Tooltip("Quantidade:Q")
        ]
    )
)

# Data labels
labels = (
    alt.Chart(state_counts)
    .mark_text(
        align="left",
        baseline="middle",
        dx=3
    )
    .encode(
        y=alt.Y("Estado:N", sort="-x"),
        x=alt.X("Quantidade:Q"),
        text=alt.Text("Quantidade:Q")
    )
)

st.altair_chart(bars + labels,  use_container_width=True)

df_hist = df_filt.copy()
df_hist["Metragem"] = pd.to_numeric(df_hist["Metragem"], errors="coerce")
df_hist = df_hist.dropna(subset=["Metragem"])

hist_metragem = (
    alt.Chart(df_hist)
    .mark_bar()
    .encode(
        x=alt.X(
            "Metragem:Q",
            bin=alt.Bin(maxbins=30),  # ajuste aqui se quiser
            title="Metragem (m²)"
        ),
        y=alt.Y(
            "count():Q",
            title="Quantidade de obras"
        ),
        tooltip=[
            alt.Tooltip("count():Q", title="Quantidade")
        ]
    )
    .properties(
        title="Distribuição de Obras por Metragem"
    )
)

# Show dataframe
st.altair_chart(hist_metragem, use_container_width=True)

# =====================================================
# DETALHAMENTO MUNICÍPIO
# =====================================================
st.divider()
st.subheader("🏙️ Detalhamento por Município")

for _, row in state_counts.iterrows():
    estado = row["Estado"]
    qtd = row["Quantidade"]

    with st.expander(f"📍 {estado} — {qtd} obras"):
        muni_counts = (
            df[df["Estado"] == estado]
            .groupby("Nome do município")
            .size()
            .reset_index(name="Qtd_Obras")
            .sort_values("Qtd_Obras", ascending=False)
        )

        st.dataframe(muni_counts, use_container_width=True)

# =====================================================
# LAT / LON
# =====================================================
df[["lat", "lon"]] = df["Código de localização"].apply(
    lambda x: pd.Series(pluscode_to_latlon_safe(x))
)

total_antes = len(df)
df = df.dropna(subset=["lat", "lon"])
total_depois = len(df)

st.caption(
    f"🗺️ Registros com localização válida: {total_depois:,} | "
    f"Descartados: {total_antes - total_depois:,}"
)


# =====================================================
# TABLE
# =====================================================
st.dataframe(df_filt.head(100000), width='content')


# =====================================================
# MAP
# =====================================================
st.divider()
st.header("🗺️ Mapa das Obras")

BR_CENTER = [-14.2350, -51.9253]

if df.empty:
    st.warning("⚠️ Nenhuma obra com localização válida para exibir no mapa.")
    m = folium.Map(
        location=BR_CENTER,
        zoom_start=4,
        tiles="CartoDB positron"
    )
    st_folium(m, width=1100, height=600)
else:
    m = folium.Map(
        location=[df["lat"].mean(), df["lon"].mean()],
        zoom_start=5,
        tiles="CartoDB positron"
    )

    cluster = MarkerCluster().add_to(m)

    for _, row in df.iterrows():
        folium.Marker(
            location=[row["lat"], row["lon"]],
            tooltip=f"{row['Nome do município']} - {row['Estado']}",
            popup=f"""
            <b>CNO:</b> {row['CNO']}<br>
            <b>Nome:</b> {row['Nome']}<br>
            <b>Estado:</b> {row['Estado']}<br>
            <b>Município:</b> {row['Nome do município']}<br>
            <b>Categoria:</b> {row['Categoria']}<br>
            <b>Destinação:</b> {row['Destinação']}<br>
            <b>Metragem:</b> {row['Metragem']} m²
            """
        ).add_to(cluster)

    st_folium(m, width=1100, height=600)
