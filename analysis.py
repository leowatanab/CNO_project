import streamlit as st
import pandas as pd
import duckdb
import os
import folium
from folium.plugins import MarkerCluster
from streamlit_folium import st_folium
from openlocationcode import openlocationcode as olc


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


# =====================================================
# APP
# =====================================================
st.title("📊 Análise de Dados CNO")
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
    Metragem
FROM cno.cno_base
WHERE "Data de registro" > '2025-07-01'
  AND Categoria = 'Obra Nova'
  AND Destinação = 'Residencial multifamiliar'
  AND Situação = '02'
"""

df = query_table(query)

st.caption(f"🔎 Registros retornados: {len(df):,}")


# =====================================================
# FILTRO POR ESTADO
# =====================================================
estado_sel = st.selectbox(
    "Filtrar por Estado",
    options=["Todos"] + sorted(df["Estado"].dropna().unique().tolist())
)

if estado_sel != "Todos":
    df = df[df["Estado"] == estado_sel]


# =====================================================
# TABELA
# =====================================================
with st.expander("📋 Visualizar dados"):
    st.dataframe(df.head(1000), use_container_width=True)
    st.caption("Exibindo até 1.000 registros")


# =====================================================
# ESTATÍSTICAS
# =====================================================
st.divider()
st.header("📈 Estatísticas")

st.metric("Total de Obras", len(df))

st.subheader("Quantidade de obras por Estado")
state_counts = df["Estado"].value_counts()
st.bar_chart(state_counts)


# =====================================================
# DETALHAMENTO MUNICÍPIO
# =====================================================
st.divider()
st.subheader("🏙️ Detalhamento por Município")

for estado in state_counts.index:
    with st.expander(f"📍 {estado} — {state_counts[estado]} obras"):
        muni_counts = (
            df[df["Estado"] == estado]
            .groupby("Nome do município")
            .size()
            .reset_index(name="Qtd_Obras")
            .sort_values("Qtd_Obras", ascending=False)
        )

        st.dataframe(muni_counts, width='content')


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
# MAPA
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
